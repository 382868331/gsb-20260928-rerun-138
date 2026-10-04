package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Joint export plans combine several independent sources into one export.
// Each source is pinned to its own observation range (snapshot); the sources
// are explicitly NOT observed at one shared transaction moment. The joint
// material is marked complete only after every required source has a complete
// result for its pinned range.

// JointSourceState tracks one source's pinned range and scan progress inside
// a joint plan.
type JointSourceState struct {
	SourceID   string `json:"source_id"`
	SnapshotID string `json:"snapshot_id"`
	Cursor     Cursor `json:"cursor"`
	Complete   bool   `json:"complete"`
}

// RangeChange records that one source's observation range was re-selected.
// Re-selecting a range always creates a new joint candidate; the new scan is
// never mixed into material already completed under an older candidate.
type RangeChange struct {
	SourceID       string `json:"source_id"`
	OldSnapshotID  string `json:"old_snapshot_id"`
	FromGeneration int    `json:"from_generation"`
	ToGeneration   int    `json:"to_generation"`
	Reason         string `json:"reason"`
}

// JointCheckpoint is the durable state of a joint export plan.
type JointCheckpoint struct {
	PlanID       string                       `json:"plan_id"`
	CandidateID  string                       `json:"candidate_id"`
	Generation   int                          `json:"generation"`
	Sources      map[string]*JointSourceState `json:"sources"`
	RangeChanges []RangeChange                `json:"range_changes,omitempty"`
	Complete     bool                         `json:"complete"`
}

// Reusable reports which parts of an interrupted or cancelled plan remain
// valid and reusable: sources whose pinned range was fully scanned.
func (c JointCheckpoint) Reusable() []JointSourceState {
	var out []JointSourceState
	for _, state := range c.Sources {
		if state.Complete {
			out = append(out, *state)
		}
	}
	return out
}

// Manifest maps each source to the observation range (snapshot) that the
// current candidate's material was scanned from. Use it to verify the source
// and range of every exported record.
func (c JointCheckpoint) Manifest() map[string]string {
	out := make(map[string]string, len(c.Sources))
	for id, state := range c.Sources {
		out[id] = state.SnapshotID
	}
	return out
}

type JointCheckpointStore interface {
	Load(ctx context.Context, planID string) (JointCheckpoint, error)
	Save(ctx context.Context, checkpoint JointCheckpoint) error
}

type FileJointCheckpointStore struct {
	Dir string
}

func (s FileJointCheckpointStore) path(planID string) string {
	return filepath.Join(s.Dir, "joint-"+planID+".json")
}

func (s FileJointCheckpointStore) Load(_ context.Context, planID string) (JointCheckpoint, error) {
	data, err := os.ReadFile(s.path(planID))
	if errors.Is(err, os.ErrNotExist) {
		return JointCheckpoint{}, nil
	}
	if err != nil {
		return JointCheckpoint{}, err
	}
	var checkpoint JointCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return JointCheckpoint{}, err
	}
	return checkpoint, nil
}

func (s FileJointCheckpointStore) Save(_ context.Context, checkpoint JointCheckpoint) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.Dir, ".joint-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(append(data, 10)); err != nil {
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
	return os.Rename(tmpName, s.path(checkpoint.PlanID))
}

type MemoryJointCheckpointStore struct {
	Data map[string]JointCheckpoint
}

func (s *MemoryJointCheckpointStore) Load(_ context.Context, planID string) (JointCheckpoint, error) {
	if s.Data == nil {
		return JointCheckpoint{}, nil
	}
	return s.Data[planID], nil
}

func (s *MemoryJointCheckpointStore) Save(_ context.Context, checkpoint JointCheckpoint) error {
	if s.Data == nil {
		s.Data = map[string]JointCheckpoint{}
	}
	s.Data[checkpoint.PlanID] = checkpoint
	return nil
}

// JointRunner runs a joint export plan. Batches for each candidate are written
// through a candidate-scoped sink so a re-selected range never contaminates
// material completed under an older candidate.
type JointRunner struct {
	PlanID     string
	Connectors []Connector
	Store      JointCheckpointStore
	// NewSink returns the sink for one candidate. Candidate IDs change whenever
	// a source's range is re-selected.
	NewSink  func(candidateID string) Sink
	PageSize int
}

func (r JointRunner) Run(ctx context.Context) error {
	if r.PlanID == "" {
		return fmt.Errorf("PlanID is required")
	}
	if r.Store == nil {
		return fmt.Errorf("Store is required")
	}
	if r.NewSink == nil {
		return fmt.Errorf("NewSink is required")
	}
	if r.PageSize <= 0 {
		return fmt.Errorf("PageSize must be positive")
	}
	checkpoint, err := r.Store.Load(ctx, r.PlanID)
	if err != nil {
		return err
	}
	if checkpoint.PlanID == "" {
		checkpoint = JointCheckpoint{
			PlanID:      r.PlanID,
			Generation:  1,
			CandidateID: fmt.Sprintf("%s-candidate-%d", r.PlanID, 1),
			Sources:     map[string]*JointSourceState{},
		}
		for _, connector := range r.Connectors {
			checkpoint.Sources[connector.SourceID()] = &JointSourceState{SourceID: connector.SourceID()}
		}
	}
	if checkpoint.Complete {
		return nil
	}
	sink := r.NewSink(checkpoint.CandidateID)
	for _, connector := range r.Connectors {
		if connector == nil {
			return fmt.Errorf("connector cannot be nil")
		}
		state := checkpoint.Sources[connector.SourceID()]
		if state == nil {
			state = &JointSourceState{SourceID: connector.SourceID()}
			checkpoint.Sources[connector.SourceID()] = state
		}
		if state.Complete {
			continue
		}
		snapshot, err := connector.OpenSnapshot(ctx, state.Cursor)
		if err != nil {
			var unavailable *SnapshotUnavailableError
			if !errors.As(err, &unavailable) || state.Cursor.Token == "" {
				return err
			}
			// The pinned range expired. Re-select this source's range alone:
			// start a new joint candidate, reset only this source, and record
			// the change. Other sources' confirmed results stay untouched.
			checkpoint.Generation++
			checkpoint.CandidateID = fmt.Sprintf("%s-candidate-%d", r.PlanID, checkpoint.Generation)
			checkpoint.RangeChanges = append(checkpoint.RangeChanges, RangeChange{
				SourceID:       state.SourceID,
				OldSnapshotID:  state.SnapshotID,
				FromGeneration: checkpoint.Generation - 1,
				ToGeneration:   checkpoint.Generation,
				Reason:         err.Error(),
			})
			*state = JointSourceState{SourceID: state.SourceID}
			if err := r.Store.Save(ctx, checkpoint); err != nil {
				return err
			}
			sink = r.NewSink(checkpoint.CandidateID)
			snapshot, err = connector.OpenSnapshot(ctx, Cursor{})
			if err != nil {
				return err
			}
		}
		if state.SnapshotID == "" {
			state.SnapshotID = snapshot.ID()
		}
		cursor := state.Cursor
		for {
			if err := ctx.Err(); err != nil {
				_ = snapshot.Close()
				return err
			}
			page, err := snapshot.Read(ctx, cursor, r.PageSize)
			if err != nil {
				_ = snapshot.Close()
				return err
			}
			batch := Batch{
				SourceID:   connector.SourceID(),
				Snapshot:   snapshot.ID(),
				StartToken: cursor.Token,
				Records:    page.Records,
			}
			batch.BatchID = deterministicBatchID(batch)
			if err := sink.WriteBatch(ctx, batch); err != nil {
				_ = snapshot.Close()
				return err
			}
			cursor = page.Next
			if !page.HasMore {
				cursor.SnapshotID = snapshot.ID()
				cursor.Complete = true
			}
			state.Cursor = cursor
			state.Complete = cursor.Complete
			// Persist after every batch so a paused slow source keeps its
			// finished pages and other sources' confirmed results intact.
			if err := r.Store.Save(ctx, checkpoint); err != nil {
				_ = snapshot.Close()
				return err
			}
			if !page.HasMore {
				break
			}
		}
		if err := snapshot.Close(); err != nil {
			return err
		}
	}
	for _, state := range checkpoint.Sources {
		if !state.Complete {
			return nil
		}
	}
	checkpoint.Complete = true
	return r.Store.Save(ctx, checkpoint)
}

// Status loads the current joint checkpoint so a caller can verify, after a
// pause or cancellation, which parts are complete and reusable.
func (r JointRunner) Status(ctx context.Context) (JointCheckpoint, error) {
	return r.Store.Load(ctx, r.PlanID)
}
