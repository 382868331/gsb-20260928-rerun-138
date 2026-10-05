package datalink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Origin classifications reported by a delivery review: they tell the
// operator, per source, where the delivered records actually came from.
const (
	// OriginFresh marks a scope whose observation range was pinned and
	// fully read during the final generation of the plan.
	OriginFresh = "fresh"
	// OriginReused marks a scope confirmed under an earlier joint candidate
	// whose previously confirmed results were reused without recomputation.
	OriginReused = "reused"
	// OriginRefetched marks a scope whose previous observation range
	// expired, so the source alone was re-selected and read again.
	OriginRefetched = "refetched"
)

// DeliverySelection is the operator's choice of sources and record range for
// one delivery. Sources empty means every source of the plan; Limit caps the
// number of records taken per source (0 means no limit). The selection is
// stored inside the frozen delivery so a later re-verification applies
// exactly the same range.
type DeliverySelection struct {
	Sources []string `json:"sources,omitempty"`
	Limit   int      `json:"limit,omitempty"`
}

func (s DeliverySelection) includes(sourceID string) bool {
	if len(s.Sources) == 0 {
		return true
	}
	for _, id := range s.Sources {
		if id == sourceID {
			return true
		}
	}
	return false
}

// DeliveryScopeView is one source's row in the pre-freeze review: which
// observation scope the delivery comes from and how it was obtained.
type DeliveryScopeView struct {
	SourceID   string `json:"source_id"`
	Candidate  string `json:"candidate"`
	SnapshotID string `json:"snapshot_id"`
	Origin     string `json:"origin"`
	Reason     string `json:"reason"`
	Records    int    `json:"records"`
}

// DeliveryReview is the pre-freeze checklist view: the operator selected
// sources and a record range, and sees per source which observation scope
// the delivery would use and whether the results are fresh, reused or
// re-fetched. Freezing turns the review into a durable Delivery.
type DeliveryReview struct {
	PlanID    string              `json:"plan_id"`
	Candidate string              `json:"candidate"`
	Selection DeliverySelection   `json:"selection"`
	Scopes    []DeliveryScopeView `json:"scopes"`
	records   []Record
}

// scopeOrigin classifies how a completed scope's results were obtained.
func scopeOrigin(plan *JointPlan, scope JointScope) (origin, reason string) {
	switch {
	case scope.ReSelected:
		return OriginRefetched, fmt.Sprintf(
			"previous observation scope expired; source was re-selected alone under candidate %q and read again from snapshot %q",
			scope.Candidate, scope.SnapshotID)
	case scope.Candidate != plan.CandidateID():
		return OriginReused, fmt.Sprintf(
			"scope was confirmed earlier under candidate %q and its previously confirmed results were reused without recomputation",
			scope.Candidate)
	default:
		return OriginFresh, fmt.Sprintf(
			"observation scope %q was pinned and read during this delivery's candidate %q",
			scope.SnapshotID, scope.Candidate)
	}
}

// selectDeliveryRecords picks the records of one source out of the plan's
// joint material, honoring the selection's record range. Records are sorted
// by DataID before the limit is applied so the range is deterministic
// regardless of on-disk batch order.
func selectDeliveryRecords(material []Batch, sourceID string, limit int) []Record {
	var records []Record
	for _, batch := range material {
		if batch.SourceID == sourceID {
			records = append(records, batch.Records...)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].DataID < records[j].DataID })
	if limit > 0 && len(records) > limit {
		records = records[:limit]
	}
	return records
}

// ReviewDelivery builds the savable pre-freeze view for a completed joint
// plan. It refuses incomplete plans: only a complete plan has verifiable
// joint material to freeze.
func ReviewDelivery(plan *JointPlan, batches []Batch, selection DeliverySelection) (*DeliveryReview, error) {
	if plan == nil {
		return nil, fmt.Errorf("joint plan is missing")
	}
	if !plan.Complete {
		return nil, fmt.Errorf("joint plan %q is not complete; a delivery can only be reviewed once every scope is confirmed", plan.PlanID)
	}
	if selection.Limit < 0 {
		return nil, fmt.Errorf("selection limit must not be negative")
	}
	material := JointMaterial(plan, batches)
	if err := VerifyJointRecords(plan, material); err != nil {
		return nil, err
	}
	review := &DeliveryReview{
		PlanID:    plan.PlanID,
		Candidate: plan.CandidateID(),
		Selection: selection,
	}
	for _, scope := range plan.Scopes {
		if !selection.includes(scope.SourceID) {
			continue
		}
		origin, reason := scopeOrigin(plan, scope)
		records := selectDeliveryRecords(material, scope.SourceID, selection.Limit)
		review.Scopes = append(review.Scopes, DeliveryScopeView{
			SourceID:   scope.SourceID,
			Candidate:  scope.Candidate,
			SnapshotID: scope.SnapshotID,
			Origin:     origin,
			Reason:     reason,
			Records:    len(records),
		})
		review.records = append(review.records, records...)
	}
	return review, nil
}

// FrozenRecord is the identity and content fingerprint of one delivered
// record. The frozen set is what later verifications and comparisons check
// against; it never changes once the delivery is saved.
type FrozenRecord struct {
	Source      string `json:"source"`
	Kind        string `json:"kind"`
	ExternalID  string `json:"external_id"`
	Version     string `json:"version"`
	DataID      string `json:"data_id"`
	PayloadHash string `json:"payload_hash"`
}

func freezeRecord(record Record) FrozenRecord {
	return FrozenRecord{
		Source:      record.Identity.Source,
		Kind:        record.Identity.Kind,
		ExternalID:  record.Identity.ExternalID,
		Version:     record.Identity.Version,
		DataID:      record.DataID,
		PayloadHash: record.PayloadHash,
	}
}

// Delivery is the frozen record set one delivery is based on. After it is
// saved, sources gaining records or being re-imported must not silently
// change it: VerifyDelivery re-derives the set from the current batches and
// fails on any difference.
type Delivery struct {
	DeliveryID string              `json:"delivery_id"`
	PlanID     string              `json:"plan_id"`
	Candidate  string              `json:"candidate"`
	Selection  DeliverySelection   `json:"selection"`
	Scopes     []DeliveryScopeView `json:"scopes"`
	Records    []FrozenRecord      `json:"records"`
	Digest     string              `json:"digest"`
}

func deliveryDigest(records []FrozenRecord) string {
	sorted := append([]FrozenRecord(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].DataID < sorted[j].DataID })
	h := sha256.New()
	for _, record := range sorted {
		_, _ = fmt.Fprintf(h, "%s\x00%s\n", record.DataID, record.PayloadHash)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// Freeze turns the reviewed selection into a delivery whose record set is
// fixed. The delivery ID is derived from plan, candidate and content digest,
// so two different frozen sets never share an ID.
func (r *DeliveryReview) Freeze() (*Delivery, error) {
	if r == nil {
		return nil, fmt.Errorf("delivery review is missing")
	}
	records := make([]FrozenRecord, 0, len(r.records))
	for _, record := range r.records {
		records = append(records, freezeRecord(record))
	}
	digest := deliveryDigest(records)
	delivery := &Delivery{
		PlanID:    r.PlanID,
		Candidate: r.Candidate,
		Selection: r.Selection,
		Scopes:    append([]DeliveryScopeView(nil), r.Scopes...),
		Records:   records,
		Digest:    digest,
	}
	delivery.DeliveryID = fmt.Sprintf("%s-%s-%s", r.PlanID, r.Candidate, shortID(digest, 12))
	return delivery, nil
}

// VerifyDelivery re-derives the frozen record set from the batches currently
// on disk and compares it to the saved set. It passes only if the delivered
// content is exactly what was frozen: records added to a source afterwards
// or written by a later re-import belong to other batches and never leak in,
// while any tampering with the delivered batches is reported.
func VerifyDelivery(delivery *Delivery, batches []Batch) error {
	if delivery == nil {
		return fmt.Errorf("delivery is missing")
	}
	scopes := map[string]DeliveryScopeView{}
	for _, scope := range delivery.Scopes {
		scopes[scope.SourceID] = scope
	}
	var selected []FrozenRecord
	perSource := map[string][]Record{}
	for _, batch := range batches {
		if batch.PlanID != delivery.PlanID {
			continue
		}
		scope, ok := scopes[batch.SourceID]
		if !ok {
			continue
		}
		if batch.Candidate != scope.Candidate || batch.Snapshot != scope.SnapshotID {
			continue
		}
		perSource[batch.SourceID] = append(perSource[batch.SourceID], batch.Records...)
	}
	for sourceID, scope := range scopes {
		records := perSource[sourceID]
		sort.Slice(records, func(i, j int) bool { return records[i].DataID < records[j].DataID })
		if delivery.Selection.Limit > 0 && len(records) > delivery.Selection.Limit {
			records = records[:delivery.Selection.Limit]
		}
		if len(records) != scope.Records {
			return fmt.Errorf(
				"delivery %q: source %q now yields %d records for the pinned scope %q/%q, want the %d frozen at delivery time; delivered content must not change silently",
				delivery.DeliveryID, sourceID, len(records), scope.Candidate, scope.SnapshotID, scope.Records)
		}
		for _, record := range records {
			selected = append(selected, freezeRecord(record))
		}
	}
	if got := deliveryDigest(selected); got != delivery.Digest {
		return fmt.Errorf(
			"delivery %q: re-derived digest %s does not match frozen digest %s; the delivered record set changed after it was frozen",
			delivery.DeliveryID, shortID(got, 12), shortID(delivery.Digest, 12))
	}
	frozen := make(map[string]FrozenRecord, len(delivery.Records))
	for _, record := range delivery.Records {
		frozen[record.DataID] = record
	}
	for _, record := range selected {
		want, ok := frozen[record.DataID]
		if !ok {
			return fmt.Errorf("delivery %q: record %q of source %q was not part of the frozen set", delivery.DeliveryID, record.DataID, record.Source)
		}
		if want.PayloadHash != record.PayloadHash {
			return fmt.Errorf("delivery %q: record %q payload changed after freezing", delivery.DeliveryID, record.DataID)
		}
	}
	if len(selected) != len(delivery.Records) {
		return fmt.Errorf("delivery %q: frozen set has %d records, re-derived set has %d", delivery.DeliveryID, len(delivery.Records), len(selected))
	}
	return nil
}

// RecordChange describes one added, removed or content-changed record
// between two frozen deliveries, with the reason for the change.
type RecordChange struct {
	Source     string `json:"source"`
	Kind       string `json:"kind"`
	ExternalID string `json:"external_id"`
	Change     string `json:"change"` // "added", "removed" or "changed"
	OldVersion string `json:"old_version,omitempty"`
	NewVersion string `json:"new_version,omitempty"`
	Reason     string `json:"reason"`
}

// SourceComparison groups the record changes of one source.
type SourceComparison struct {
	SourceID string         `json:"source_id"`
	Changes  []RecordChange `json:"changes,omitempty"`
}

// DeliveryComparison is a new review view over two frozen deliveries: per
// source it lists what was added, what disappeared and what changed content.
type DeliveryComparison struct {
	FromDelivery string             `json:"from_delivery"`
	ToDelivery   string             `json:"to_delivery"`
	Sources      []SourceComparison `json:"sources"`
}

// CompareDeliveries diffs two frozen deliveries record by record. Matching
// is by source, kind and external ID: a matching external ID with a
// different identity or payload is a content change, otherwise records only
// present on one side are additions or removals. Each change keeps a reason.
func CompareDeliveries(from, to *Delivery) (*DeliveryComparison, error) {
	if from == nil || to == nil {
		return nil, fmt.Errorf("both deliveries are required")
	}
	comparison := &DeliveryComparison{FromDelivery: from.DeliveryID, ToDelivery: to.DeliveryID}
	type key struct{ source, kind, externalID string }
	oldRecords := map[key]FrozenRecord{}
	for _, record := range from.Records {
		oldRecords[key{record.Source, record.Kind, record.ExternalID}] = record
	}
	newRecords := map[key]FrozenRecord{}
	for _, record := range to.Records {
		newRecords[key{record.Source, record.Kind, record.ExternalID}] = record
	}
	changes := map[string][]RecordChange{}
	add := func(change RecordChange) {
		changes[change.Source] = append(changes[change.Source], change)
	}
	for id, old := range oldRecords {
		current, ok := newRecords[id]
		if !ok {
			add(RecordChange{
				Source: old.Source, Kind: old.Kind, ExternalID: old.ExternalID,
				Change: "removed", OldVersion: old.Version,
				Reason: fmt.Sprintf("record was delivered in %q but is absent from %q", from.DeliveryID, to.DeliveryID),
			})
			continue
		}
		if old.DataID == current.DataID && old.PayloadHash == current.PayloadHash {
			continue
		}
		reason := fmt.Sprintf("same external ID %q was re-imported with new content", old.ExternalID)
		if old.Version != current.Version {
			reason = fmt.Sprintf("source entity %q moved from version %q to version %q", old.ExternalID, old.Version, current.Version)
		}
		add(RecordChange{
			Source: old.Source, Kind: old.Kind, ExternalID: old.ExternalID,
			Change: "changed", OldVersion: old.Version, NewVersion: current.Version,
			Reason: reason,
		})
	}
	for id, current := range newRecords {
		if _, ok := oldRecords[id]; ok {
			continue
		}
		add(RecordChange{
			Source: current.Source, Kind: current.Kind, ExternalID: current.ExternalID,
			Change: "added", NewVersion: current.Version,
			Reason: fmt.Sprintf("record appeared in %q and was not part of the frozen set of %q; the source gained it after the first delivery was frozen", to.DeliveryID, from.DeliveryID),
		})
	}
	sourceIDs := make([]string, 0, len(changes))
	for sourceID := range changes {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	for _, sourceID := range sourceIDs {
		list := changes[sourceID]
		sort.Slice(list, func(i, j int) bool { return list[i].ExternalID < list[j].ExternalID })
		comparison.Sources = append(comparison.Sources, SourceComparison{SourceID: sourceID, Changes: list})
	}
	return comparison, nil
}

// DeliveryStore persists frozen deliveries so a saved session can be
// reopened and both deliveries re-verified later.
type DeliveryStore interface {
	SaveDelivery(ctx context.Context, delivery *Delivery) error
	// LoadDelivery returns nil, nil when the delivery does not exist.
	LoadDelivery(ctx context.Context, deliveryID string) (*Delivery, error)
}

type FileDeliveryStore struct {
	Dir string
}

func (s FileDeliveryStore) path(deliveryID string) string {
	return filepath.Join(s.Dir, "delivery-"+deliveryID+".json")
}

func (s FileDeliveryStore) SaveDelivery(_ context.Context, delivery *Delivery) error {
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
