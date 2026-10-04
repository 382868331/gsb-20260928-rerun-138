package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// JointManifestSource is the synthetic SourceID used for the manifest batch a
// JointRunner writes once every required source scope is complete.
const JointManifestSource = "joint"

// JointScope pins one source's observation range inside a joint export plan.
// Each source owns its scope independently: scopes are fixed at different
// times and are explicitly not a shared transaction moment. Candidate records
// which joint candidate the scope was confirmed under, so a re-scoped source
// never mixes into material confirmed for an older candidate.
type JointScope struct {
	SourceID   string `json:"source_id"`
	Candidate  string `json:"candidate"`
	SnapshotID string `json:"snapshot_id"`
	Complete   bool   `json:"complete"`
}

// JointPlan is the durable state of a joint export across independent
// sources. It is complete only when every required source scope is complete.
type JointPlan struct {
	PlanID     string       `json:"plan_id"`
	Generation int          `json:"generation"`
	Scopes     []JointScope `json:"scopes"`
	Notes      []string     `json:"notes,omitempty"`
	Complete   bool         `json:"complete"`
}

// CandidateID identifies the current joint candidate. Re-selecting one
// source's scope bumps the generation and therefore produces a new candidate
// instead of altering already confirmed material.
func (p *JointPlan) CandidateID() string {
	return fmt.Sprintf("%s-g%d", p.PlanID, p.Generation)
}

func (p *JointPlan) Scope(sourceID string) *JointScope {
	for i := range p.Scopes {
		if p.Scopes[i].SourceID == sourceID {
			return &p.Scopes[i]
		}
	}
	return nil
}

// ReusablePart describes what survives a pause or cancellation of a joint
// run: completed scopes stay valid, and an interrupted source keeps a
// resumable cursor inside its pinned snapshot.
type ReusablePart struct {
	SourceID   string `json:"source_id"`
	SnapshotID string `json:"snapshot_id"`
	State      string `json:"state"` // "complete", "resumable" or "pending"
	Token      string `json:"token,omitempty"`
}

// ReusableParts reports, per source, which parts of an unfinished plan can be
// reused after a pause or cancellation.
func (p *JointPlan) ReusableParts(cursors map[string]Cursor) []ReusablePart {
	parts := make([]ReusablePart, 0, len(p.Scopes))
	for _, scope := range p.Scopes {
		part := ReusablePart{
			SourceID:   scope.SourceID,
			SnapshotID: scope.SnapshotID,
			State:      "pending",
		}
		switch {
		case scope.Complete:
			part.State = "complete"
		default:
			cursor := cursors[jointCheckpointKey(p.PlanID, scope.SourceID)]
			if cursor.SnapshotID != "" {
				part.State = "resumable"
				part.Token = cursor.Token
				if part.SnapshotID == "" {
					part.SnapshotID = cursor.SnapshotID
				}
			}
		}
		parts = append(parts, part)
	}
	return parts
}

type JointPlanStore interface {
	// LoadPlan returns nil, nil when the plan does not exist yet.
	LoadPlan(ctx context.Context, planID string) (*JointPlan, error)
	SavePlan(ctx context.Context, plan *JointPlan) error
}

type FileJointPlanStore struct {
	Dir string
}

func (s FileJointPlanStore) path(planID string) string {
	return filepath.Join(s.Dir, "joint-plan-"+planID+".json")
}

func (s FileJointPlanStore) LoadPlan(_ context.Context, planID string) (*JointPlan, error) {
	data, err := os.ReadFile(s.path(planID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var plan JointPlan
	if err := json.Unmarshal(data, &plan); err != nil {
		return nil, err
	}
	return &plan, nil
}

func (s FileJointPlanStore) SavePlan(_ context.Context, plan *JointPlan) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, 10)
	tmp, err := os.CreateTemp(s.Dir, ".joint-plan-*")
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
	return os.Rename(tmpName, s.path(plan.PlanID))
}

type MemoryJointPlanStore struct {
	Plans map[string]JointPlan
}

func (s *MemoryJointPlanStore) LoadPlan(_ context.Context, planID string) (*JointPlan, error) {
	if s.Plans == nil {
		return nil, nil
	}
	plan, ok := s.Plans[planID]
	if !ok {
		return nil, nil
	}
	copied := plan
	copied.Scopes = append([]JointScope(nil), plan.Scopes...)
	copied.Notes = append([]string(nil), plan.Notes...)
	return &copied, nil
}

func (s *MemoryJointPlanStore) SavePlan(_ context.Context, plan *JointPlan) error {
	if s.Plans == nil {
		s.Plans = map[string]JointPlan{}
	}
	copied := *plan
	copied.Scopes = append([]JointScope(nil), plan.Scopes...)
	copied.Notes = append([]string(nil), plan.Notes...)
	s.Plans[plan.PlanID] = copied
	return nil
}

func jointCheckpointKey(planID, sourceID string) string {
	return planID + "/" + sourceID
}

// JointRunner runs one joint export plan. Every source keeps its own cursor
// and snapshot; a slow source can pause and resume without touching sources
// that already completed, and a source whose snapshot expired is re-scoped
// alone under a new joint candidate.
type JointRunner struct {
	PlanID      string
	Connectors  []Connector
	Plans       JointPlanStore
	Checkpoints CheckpointStore
	Sink        Sink
	PageSize    int
}

func (r JointRunner) Run(ctx context.Context) (*JointPlan, error) {
	if r.PlanID == "" {
		return nil, fmt.Errorf("PlanID is required")
	}
	if r.PageSize <= 0 {
		return nil, fmt.Errorf("PageSize must be positive")
	}
	if r.Plans == nil {
		return nil, fmt.Errorf("Plans is required")
	}
	if r.Checkpoints == nil {
		return nil, fmt.Errorf("Checkpoints is required")
	}
	if r.Sink == nil {
		return nil, fmt.Errorf("Sink is required")
	}
	plan, err := r.Plans.LoadPlan(ctx, r.PlanID)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		plan = &JointPlan{PlanID: r.PlanID, Generation: 1}
	}
	if plan.Complete {
		return plan, nil
	}
	for _, connector := range r.Connectors {
		if connector == nil {
			return nil, fmt.Errorf("connector cannot be nil")
		}
		if plan.Scope(connector.SourceID()) == nil {
			plan.Scopes = append(plan.Scopes, JointScope{SourceID: connector.SourceID()})
		}
	}
	cursors, err := r.Checkpoints.Load(ctx)
	if err != nil {
		return nil, err
	}
	savePlan := func() error { return r.Plans.SavePlan(ctx, plan) }
	if err := savePlan(); err != nil {
		return nil, err
	}
	var sequence uint64
	rescope := func(scope *JointScope, sourceID string, unavailable *SnapshotUnavailableError) error {
		// Only this source's observation scope expired. Re-select it under a
		// new joint candidate and record the change; scopes already confirmed
		// for other sources stay untouched.
		plan.Generation++
		candidate := plan.CandidateID()
		plan.Notes = append(plan.Notes, fmt.Sprintf(
			"source %q: observation scope %q expired at cursor token %q; re-selected a fresh scope under new joint candidate %q; confirmed scopes of other sources are unchanged",
			sourceID, unavailable.SnapshotID, unavailable.Token, candidate))
		key := jointCheckpointKey(plan.PlanID, sourceID)
		cursors[key] = Cursor{}
		if err := r.Checkpoints.Save(ctx, cursors); err != nil {
			return err
		}
		scope.Complete = false
		scope.Candidate = candidate
		scope.SnapshotID = ""
		return savePlan()
	}
	for _, connector := range r.Connectors {
		scope := plan.Scope(connector.SourceID())
		key := jointCheckpointKey(plan.PlanID, connector.SourceID())
		cursor := cursors[key]
		if scope.Complete {
			// A confirmed scope is reused only while its pinned snapshot is
			// still available; validation opens no pages and recomputes
			// nothing. If the scope expired, this source alone is re-selected.
			snapshot, err := connector.OpenSnapshot(ctx, cursor)
			if err == nil {
				if err := snapshot.Close(); err != nil {
					return plan, err
				}
				continue
			}
			var unavailable *SnapshotUnavailableError
			if !errors.As(err, &unavailable) {
				return plan, err
			}
			if err := rescope(scope, connector.SourceID(), unavailable); err != nil {
				return plan, err
			}
			cursor = cursors[key]
		}
		snapshot, err := connector.OpenSnapshot(ctx, cursor)
		if err != nil {
			var unavailable *SnapshotUnavailableError
			if !errors.As(err, &unavailable) {
				return plan, err
			}
			if err := rescope(scope, connector.SourceID(), unavailable); err != nil {
				return plan, err
			}
			snapshot, err = connector.OpenSnapshot(ctx, cursors[key])
			if err != nil {
				return plan, err
			}
		}
		if scope.Candidate == "" {
			scope.Candidate = plan.CandidateID()
		}
		scope.SnapshotID = snapshot.ID()
		if err := savePlan(); err != nil {
			return plan, err
		}
		for {
			if err := ctx.Err(); err != nil {
				_ = snapshot.Close()
				return plan, err
			}
			start := cursor
			page, err := snapshot.Read(ctx, cursor, r.PageSize)
			if err != nil {
				_ = snapshot.Close()
				return plan, err
			}
			sequence++
			batch := Batch{
				Sequence:   sequence,
				SourceID:   connector.SourceID(),
				Snapshot:   snapshot.ID(),
				StartToken: start.Token,
				Records:    page.Records,
				PlanID:     plan.PlanID,
				Candidate:  scope.Candidate,
			}
			batch.BatchID = deterministicBatchID(batch)
			if err := r.Sink.WriteBatch(ctx, batch); err != nil {
				_ = snapshot.Close()
				return plan, err
			}
			cursor = page.Next
			if !page.HasMore {
				cursor.SnapshotID = snapshot.ID()
				cursor.Complete = true
			}
			cursors[key] = cursor
			if err := r.Checkpoints.Save(ctx, cursors); err != nil {
				_ = snapshot.Close()
				return plan, err
			}
			if !page.HasMore {
				break
			}
		}
		if err := snapshot.Close(); err != nil {
			return plan, err
		}
		scope.Complete = true
		if err := savePlan(); err != nil {
			return plan, err
		}
	}
	plan.Complete = true
	if err := savePlan(); err != nil {
		return nil, err
	}
	if err := writeJointManifest(ctx, r.Sink, plan); err != nil {
		return plan, err
	}
	return plan, nil
}

func writeJointManifest(ctx context.Context, sink Sink, plan *JointPlan) error {
	record, err := NewRecord(JointManifestSource, "joint-manifest", plan.PlanID, plan.CandidateID(), plan)
	if err != nil {
		return err
	}
	batch := Batch{
		SourceID:  JointManifestSource,
		Snapshot:  plan.CandidateID(),
		Records:   []Record{record},
		PlanID:    plan.PlanID,
		Candidate: plan.CandidateID(),
	}
	batch.BatchID = deterministicBatchID(batch)
	return sink.WriteBatch(ctx, batch)
}

// JointMaterial selects exactly the batches that belong to the plan's pinned
// scopes. Batches written for superseded candidates (for example a source's
// expired old scope) are excluded, so stale material is never mixed into the
// completed joint export.
func JointMaterial(plan *JointPlan, batches []Batch) []Batch {
	scopes := map[string]JointScope{}
	for _, scope := range plan.Scopes {
		scopes[scope.SourceID] = scope
	}
	var out []Batch
	for _, batch := range batches {
		if batch.PlanID != plan.PlanID {
			continue
		}
		if batch.SourceID == JointManifestSource {
			if batch.Candidate == plan.CandidateID() {
				out = append(out, batch)
			}
			continue
		}
		scope, ok := scopes[batch.SourceID]
		if ok && scope.Complete && batch.Candidate == scope.Candidate && batch.Snapshot == scope.SnapshotID {
			out = append(out, batch)
		}
	}
	return out
}

// VerifyJointRecords checks every batch of the plan's current material: each
// batch must match the pinned candidate and snapshot of its source's scope,
// and every record must carry the source and scope it claims.
func VerifyJointRecords(plan *JointPlan, batches []Batch) error {
	if plan == nil {
		return fmt.Errorf("joint plan is missing")
	}
	if !plan.Complete {
		return fmt.Errorf("joint plan %q is not complete; only a complete plan has verifiable joint material", plan.PlanID)
	}
	scopes := map[string]JointScope{}
	for _, scope := range plan.Scopes {
		scopes[scope.SourceID] = scope
	}
	for _, batch := range batches {
		if batch.PlanID != plan.PlanID {
			continue
		}
		if batch.SourceID == JointManifestSource {
			if batch.Candidate != plan.CandidateID() {
				return fmt.Errorf("joint manifest batch %q names candidate %q, want %q", batch.BatchID, batch.Candidate, plan.CandidateID())
			}
			continue
		}
		scope, ok := scopes[batch.SourceID]
		if !ok {
			return fmt.Errorf("batch %q: source %q is not part of joint plan %q", batch.BatchID, batch.SourceID, plan.PlanID)
		}
		if !scope.Complete {
			return fmt.Errorf("batch %q: scope of source %q is not complete", batch.BatchID, batch.SourceID)
		}
		if batch.Candidate != scope.Candidate || batch.Snapshot != scope.SnapshotID {
			return fmt.Errorf(
				"batch %q of source %q belongs to candidate %q snapshot %q, but the plan pins candidate %q snapshot %q; stale material must not be mixed into the joint export",
				batch.BatchID, batch.SourceID, batch.Candidate, batch.Snapshot, scope.Candidate, scope.SnapshotID)
		}
		for _, record := range batch.Records {
			if record.Identity.Source != batch.SourceID {
				return fmt.Errorf("batch %q: record %q claims source %q, want %q", batch.BatchID, record.DataID, record.Identity.Source, batch.SourceID)
			}
		}
	}
	return nil
}
