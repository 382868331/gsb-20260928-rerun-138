package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// DeliveryReviewItem explains, for one source, where the records of the
// current joint candidate actually come from: which observation scope
// supplied them, whether that scope was read fresh, reused from an earlier
// confirmation, or re-selected because the previous scope expired.
type DeliveryReviewItem struct {
	SourceID   string `json:"source_id"`
	Candidate  string `json:"candidate"`
	SnapshotID string `json:"snapshot_id"`
	Origin     string `json:"origin"`
	Reason     string `json:"reason"`
	Batches    int    `json:"batches"`
	Records    int    `json:"records"`
}

// DeliveryReview is the checkable view an operator inspects before freezing
// a delivery: per source it names the observation scope behind the records
// and how that scope entered this joint candidate.
type DeliveryReview struct {
	PlanID    string               `json:"plan_id"`
	Candidate string               `json:"candidate"`
	Complete  bool                 `json:"complete"`
	Items     []DeliveryReviewItem `json:"items"`
}

// BuildDeliveryReview summarizes the plan's current joint material per
// source. It works on incomplete plans too, so an operator can review the
// selected sources and record ranges before deciding to freeze.
func BuildDeliveryReview(plan *JointPlan, batches []Batch) (DeliveryReview, error) {
	if plan == nil {
		return DeliveryReview{}, fmt.Errorf("joint plan is missing")
	}
	type count struct {
		batches int
		records int
	}
	counts := map[string]*count{}
	for _, batch := range JointMaterial(plan, batches) {
		if batch.SourceID == JointManifestSource {
			continue
		}
		entry := counts[batch.SourceID]
		if entry == nil {
			entry = &count{}
			counts[batch.SourceID] = entry
		}
		entry.batches++
		entry.records += len(batch.Records)
	}
	review := DeliveryReview{
		PlanID:    plan.PlanID,
		Candidate: plan.CandidateID(),
		Complete:  plan.Complete,
	}
	for _, scope := range plan.Scopes {
		item := DeliveryReviewItem{
			SourceID:   scope.SourceID,
			Candidate:  scope.Candidate,
			SnapshotID: scope.SnapshotID,
			Origin:     scope.Origin,
			Reason:     scope.OriginReason,
		}
		if entry := counts[scope.SourceID]; entry != nil {
			item.Batches = entry.batches
			item.Records = entry.records
		}
		review.Items = append(review.Items, item)
	}
	return review, nil
}

// FrozenRecord is one record of a frozen delivery: the identity and payload
// hash exactly as delivered, plus the batch that carried it.
type FrozenRecord struct {
	DataID      string `json:"data_id"`
	SourceID    string `json:"source_id"`
	Kind        string `json:"kind"`
	ExternalID  string `json:"external_id"`
	Version     string `json:"version"`
	PayloadHash string `json:"payload_hash"`
	BatchID     string `json:"batch_id"`
}

// Delivery is the frozen record set one joint candidate was delivered with.
// Once saved, later source growth or re-imports cannot silently change it:
// verification replays the pinned plan's material and compares it record by
// record against this frozen set.
type Delivery struct {
	DeliveryID string         `json:"delivery_id"`
	PlanID     string         `json:"plan_id"`
	Candidate  string         `json:"candidate"`
	Review     DeliveryReview `json:"review"`
	Records    []FrozenRecord `json:"records"`
}

// FreezeDelivery pins the record set of a complete joint plan. The review
// the operator checked is stored inside the delivery, so a saved delivery
// still explains the origin of every source after a reopen.
func FreezeDelivery(plan *JointPlan, batches []Batch) (*Delivery, error) {
	if plan == nil {
		return nil, fmt.Errorf("joint plan is missing")
	}
	if !plan.Complete {
		return nil, fmt.Errorf("joint plan %q is not complete; only a complete candidate can be frozen into a delivery", plan.PlanID)
	}
	material := JointMaterial(plan, batches)
	if err := VerifyJointRecords(plan, material); err != nil {
		return nil, err
	}
	review, err := BuildDeliveryReview(plan, batches)
	if err != nil {
		return nil, err
	}
	delivery := &Delivery{
		DeliveryID: plan.PlanID + "@" + plan.CandidateID(),
		PlanID:     plan.PlanID,
		Candidate:  plan.CandidateID(),
		Review:     review,
	}
	for _, batch := range material {
		if batch.SourceID == JointManifestSource {
			continue
		}
		for _, record := range batch.Records {
			delivery.Records = append(delivery.Records, FrozenRecord{
				DataID:      record.DataID,
				SourceID:    record.Identity.Source,
				Kind:        record.Identity.Kind,
				ExternalID:  record.Identity.ExternalID,
				Version:     record.Identity.Version,
				PayloadHash: record.PayloadHash,
				BatchID:     batch.BatchID,
			})
		}
	}
	sortFrozenRecords(delivery.Records)
	return delivery, nil
}

func sortFrozenRecords(records []FrozenRecord) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].SourceID != records[j].SourceID {
			return records[i].SourceID < records[j].SourceID
		}
		return records[i].DataID < records[j].DataID
	})
}

// VerifyDelivery re-checks a frozen delivery against the current on-disk
// state: the pinned plan must still be complete under the same candidate,
// its joint material must still verify, and the material's records must
// equal the frozen set exactly. Sources growing or new imports running after
// the freeze do not change the outcome, because the plan pins every scope.
func VerifyDelivery(delivery *Delivery, plan *JointPlan, batches []Batch) error {
	if delivery == nil {
		return fmt.Errorf("delivery is missing")
	}
	if plan == nil {
		return fmt.Errorf("joint plan is missing")
	}
	if plan.PlanID != delivery.PlanID {
		return fmt.Errorf("delivery %q belongs to plan %q, not %q", delivery.DeliveryID, delivery.PlanID, plan.PlanID)
	}
	if !plan.Complete {
		return fmt.Errorf("joint plan %q is not complete; cannot re-verify delivery %q against it", plan.PlanID, delivery.DeliveryID)
	}
	if plan.CandidateID() != delivery.Candidate {
		return fmt.Errorf("delivery %q froze candidate %q, but plan %q now names candidate %q", delivery.DeliveryID, delivery.Candidate, plan.PlanID, plan.CandidateID())
	}
	material := JointMaterial(plan, batches)
	if err := VerifyJointRecords(plan, material); err != nil {
		return err
	}
	var current []FrozenRecord
	for _, batch := range material {
		if batch.SourceID == JointManifestSource {
			continue
		}
		for _, record := range batch.Records {
			current = append(current, FrozenRecord{
				DataID:      record.DataID,
				SourceID:    record.Identity.Source,
				Kind:        record.Identity.Kind,
				ExternalID:  record.Identity.ExternalID,
				Version:     record.Identity.Version,
				PayloadHash: record.PayloadHash,
				BatchID:     batch.BatchID,
			})
		}
	}
	sortFrozenRecords(current)
	if len(current) != len(delivery.Records) {
		return fmt.Errorf("delivery %q froze %d records, but the pinned plan material now yields %d; delivered content must not drift", delivery.DeliveryID, len(delivery.Records), len(current))
	}
	for i := range current {
		if current[i] != delivery.Records[i] {
			return fmt.Errorf("delivery %q record %d differs: froze %+v, current material yields %+v; delivered content must not drift", delivery.DeliveryID, i, delivery.Records[i], current[i])
		}
	}
	return nil
}

// RecordChange describes one added, removed, or content-changed record
// between two frozen deliveries, with the reason kept next to it.
type RecordChange struct {
	SourceID   string `json:"source_id"`
	Kind       string `json:"kind"`
	ExternalID string `json:"external_id"`
	OldVersion string `json:"old_version,omitempty"`
	NewVersion string `json:"new_version,omitempty"`
	Reason     string `json:"reason"`
}

// SourceComparison is the per-source diff between two frozen deliveries.
type SourceComparison struct {
	SourceID string         `json:"source_id"`
	Added    []RecordChange `json:"added,omitempty"`
	Removed  []RecordChange `json:"removed,omitempty"`
	Changed  []RecordChange `json:"changed,omitempty"`
}

// CompareDeliveries diffs two frozen deliveries record by record, grouped
// per source. Records are matched by source, kind, and external ID: a match
// with a different version or payload hash is a content change, a key only
// in the newer delivery is an addition, and a key only in the older one is
// a removal. Every entry carries its reason.
func CompareDeliveries(older, newer *Delivery) ([]SourceComparison, error) {
	if older == nil || newer == nil {
		return nil, fmt.Errorf("both deliveries are required")
	}
	key := func(record FrozenRecord) string {
		return record.SourceID + "\x00" + record.Kind + "\x00" + record.ExternalID
	}
	oldRecords := map[string]FrozenRecord{}
	for _, record := range older.Records {
		oldRecords[key(record)] = record
	}
	newRecords := map[string]FrozenRecord{}
	for _, record := range newer.Records {
		newRecords[key(record)] = record
	}
	bySource := map[string]*SourceComparison{}
	comparison := func(sourceID string) *SourceComparison {
		entry := bySource[sourceID]
		if entry == nil {
			entry = &SourceComparison{SourceID: sourceID}
			bySource[sourceID] = entry
		}
		return entry
	}
	for recordKey, newRecord := range newRecords {
		oldRecord, ok := oldRecords[recordKey]
		entry := comparison(newRecord.SourceID)
		if !ok {
			entry.Added = append(entry.Added, RecordChange{
				SourceID:   newRecord.SourceID,
				Kind:       newRecord.Kind,
				ExternalID: newRecord.ExternalID,
				NewVersion: newRecord.Version,
				Reason: fmt.Sprintf(
					"record is present only in newer delivery %q; it did not exist in the observation scopes frozen by %q",
					newer.DeliveryID, older.DeliveryID),
			})
			continue
		}
		if oldRecord.DataID != newRecord.DataID || oldRecord.PayloadHash != newRecord.PayloadHash {
			entry.Changed = append(entry.Changed, RecordChange{
				SourceID:   newRecord.SourceID,
				Kind:       newRecord.Kind,
				ExternalID: newRecord.ExternalID,
				OldVersion: oldRecord.Version,
				NewVersion: newRecord.Version,
				Reason: fmt.Sprintf(
					"content changed between delivery %q (version %q, payload hash %s) and delivery %q (version %q, payload hash %s)",
					older.DeliveryID, oldRecord.Version, shortID(oldRecord.PayloadHash, 12),
					newer.DeliveryID, newRecord.Version, shortID(newRecord.PayloadHash, 12)),
			})
		}
	}
	for recordKey, oldRecord := range oldRecords {
		if _, ok := newRecords[recordKey]; ok {
			continue
		}
		entry := comparison(oldRecord.SourceID)
		entry.Removed = append(entry.Removed, RecordChange{
			SourceID:   oldRecord.SourceID,
			Kind:       oldRecord.Kind,
			ExternalID: oldRecord.ExternalID,
			OldVersion: oldRecord.Version,
			Reason: fmt.Sprintf(
				"record was delivered in %q (version %q) but is absent from the observation scopes frozen by newer delivery %q",
				older.DeliveryID, oldRecord.Version, newer.DeliveryID),
		})
	}
	var out []SourceComparison
	for _, entry := range bySource {
		less := func(changes []RecordChange) {
			sort.Slice(changes, func(i, j int) bool { return changes[i].ExternalID < changes[j].ExternalID })
		}
		less(entry.Added)
		less(entry.Removed)
		less(entry.Changed)
		out = append(out, *entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SourceID < out[j].SourceID })
	return out, nil
}

// DeliveryStore persists frozen deliveries so a saved delivery can be
// re-checked after the process exits and every object is rebuilt.
type DeliveryStore interface {
	// LoadDelivery returns nil, nil when the delivery does not exist yet.
	LoadDelivery(ctx context.Context, deliveryID string) (*Delivery, error)
	SaveDelivery(ctx context.Context, delivery *Delivery) error
}

type FileDeliveryStore struct {
	Dir string
}

func (s FileDeliveryStore) path(deliveryID string) string {
	return filepath.Join(s.Dir, "delivery-"+deliveryID+".json")
}

func (s FileDeliveryStore) LoadDelivery(_ context.Context, deliveryID string) (*Delivery, error) {
	data, err := os.ReadFile(s.path(deliveryID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var delivery Delivery
	if err := json.Unmarshal(data, &delivery); err != nil {
		return nil, err
	}
	return &delivery, nil
}

func (s FileDeliveryStore) SaveDelivery(_ context.Context, delivery *Delivery) error {
	if delivery == nil {
		return fmt.Errorf("delivery is missing")
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(delivery, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, 10)
	tmp, err := os.CreateTemp(s.Dir, ".delivery-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path(delivery.DeliveryID))
}

type MemoryDeliveryStore struct {
	Deliveries map[string]Delivery
}

func (s *MemoryDeliveryStore) LoadDelivery(_ context.Context, deliveryID string) (*Delivery, error) {
	if s.Deliveries == nil {
		return nil, nil
	}
	delivery, ok := s.Deliveries[deliveryID]
	if !ok {
		return nil, nil
	}
	copied := delivery
	copied.Records = append([]FrozenRecord(nil), delivery.Records...)
	copied.Review.Items = append([]DeliveryReviewItem(nil), delivery.Review.Items...)
	return &copied, nil
}

func (s *MemoryDeliveryStore) SaveDelivery(_ context.Context, delivery *Delivery) error {
	if delivery == nil {
		return fmt.Errorf("delivery is missing")
	}
	if s.Deliveries == nil {
		s.Deliveries = map[string]Delivery{}
	}
	copied := *delivery
	copied.Records = append([]FrozenRecord(nil), delivery.Records...)
	copied.Review.Items = append([]DeliveryReviewItem(nil), delivery.Review.Items...)
	s.Deliveries[delivery.DeliveryID] = copied
	return nil
}
