package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

var errSlowPaused = errors.New("slow source paused")

// pausingConnector simulates a slow source: its first scan session stops after
// failAfterPages pages; later sessions (after save/reopen) run uninterrupted.
type pausingConnector struct {
	inner         Connector
	failAfterPage int
	// pausesLeft is shared across rebuilds so the source pauses only a fixed
	// number of times no matter how often the process is restarted.
	pausesLeft *int
}

func (c *pausingConnector) SourceID() string { return c.inner.SourceID() }

func (c *pausingConnector) OpenSnapshot(ctx context.Context, resume Cursor) (Snapshot, error) {
	snapshot, err := c.inner.OpenSnapshot(ctx, resume)
	if err != nil {
		return nil, err
	}
	shouldPause := false
	if *c.pausesLeft > 0 {
		*c.pausesLeft--
		shouldPause = true
	}
	if !shouldPause {
		return snapshot, nil
	}
	return &pausingSnapshot{inner: snapshot, remaining: c.failAfterPage}, nil
}

type pausingSnapshot struct {
	inner     Snapshot
	remaining int
}

func (s *pausingSnapshot) ID() string { return s.inner.ID() }

func (s *pausingSnapshot) Read(ctx context.Context, cursor Cursor, limit int) (Page, error) {
	if s.remaining <= 0 {
		return Page{}, errSlowPaused
	}
	s.remaining--
	return s.inner.Read(ctx, cursor, limit)
}

func (s *pausingSnapshot) Close() error { return s.inner.Close() }

type countingSink struct {
	mu     sync.Mutex
	next   Sink
	writes map[string]int
}

func (s *countingSink) WriteBatch(ctx context.Context, batch Batch) error {
	s.mu.Lock()
	s.writes[batch.SourceID]++
	s.mu.Unlock()
	return s.next.WriteBatch(ctx, batch)
}

func (s *countingSink) count(sourceID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes[sourceID]
}

func readBatches(t *testing.T, dir string) []Batch {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var batches []Batch
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var batch Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatal(err)
		}
		batches = append(batches, batch)
	}
	sort.Slice(batches, func(i, j int) bool { return batches[i].StartToken < batches[j].StartToken })
	return batches
}

// TestJointPlanAcrossThreeSources demonstrates a joint export plan over three
// fictional sources with different speeds: a fast entity map, a medium append
// log, and a slow source that pauses mid-scan. It covers range expiry with a
// new joint candidate, save/reopen with all objects rebuilt, per-record
// source/range verification, and confirms that a partial redo never recomputes
// sources that were already confirmed.
func TestJointPlanAcrossThreeSources(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	snapshotDir := t.TempDir()
	jointDir := t.TempDir()

	pausesLeft := new(int)
	*pausesLeft = 1
	buildSources := func() (Connector, Connector, Connector) {
		fast := NewEntityMapSource("fast-entities", "user", []Entity{
			{ID: "u1", Version: "v1", Payload: map[string]any{"n": 1}},
			{ID: "u2", Version: "v1", Payload: map[string]any{"n": 2}},
		})
		fast.SnapshotDir = snapshotDir
		mid := NewAppendLogSource("mid-events", "event")
		mid.SnapshotDir = snapshotDir
		for _, id := range []string{"e1", "e2", "e3"} {
			if err := mid.Append(id, "v1", map[string]any{"id": id}); err != nil {
				t.Fatal(err)
			}
		}
		slowInner := NewEntityMapSource("slow-archive", "doc", []Entity{
			{ID: "d1", Version: "v1", Payload: map[string]any{"n": 1}},
			{ID: "d2", Version: "v1", Payload: map[string]any{"n": 2}},
			{ID: "d3", Version: "v1", Payload: map[string]any{"n": 3}},
			{ID: "d4", Version: "v1", Payload: map[string]any{"n": 4}},
		})
		slowInner.SnapshotDir = snapshotDir
		slow := &pausingConnector{inner: slowInner, failAfterPage: 1, pausesLeft: pausesLeft}
		return fast, mid, slow
	}

	sinks := map[string]*countingSink{}
	newSink := func(candidateID string) Sink {
		sink := &countingSink{next: FileSink{Dir: filepath.Join(outputDir, candidateID)}, writes: map[string]int{}}
		sinks[candidateID] = sink
		return sink
	}
	newRunner := func(fast, mid, slow Connector) JointRunner {
		return JointRunner{
			PlanID:     "joint-export",
			Connectors: []Connector{fast, mid, slow},
			Store:      FileJointCheckpointStore{Dir: jointDir},
			NewSink:    newSink,
			PageSize:   2,
		}
	}

	// Run 1: the slow source pauses after its first page.
	fast, mid, slow := buildSources()
	runner := newRunner(fast, mid, slow)
	if err := runner.Run(ctx); !errors.Is(err, errSlowPaused) {
		t.Fatalf("expected slow source pause, got %v", err)
	}

	// Cancellation/pause must clearly state which parts remain reusable.
	status, err := runner.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Complete {
		t.Fatal("plan must not be complete while the slow source is paused")
	}
	reusable := map[string]JointSourceState{}
	for _, state := range status.Reusable() {
		reusable[state.SourceID] = state
	}
	if _, ok := reusable["fast-entities"]; !ok {
		t.Fatal("fast source must be reusable after the pause")
	}
	if _, ok := reusable["mid-events"]; !ok {
		t.Fatal("mid source must be reusable after the pause")
	}
	if _, ok := reusable["slow-archive"]; ok {
		t.Fatal("slow source must not be reusable: its scan is incomplete")
	}
	slowState := status.Sources["slow-archive"]
	if slowState.Cursor.Token == "" || slowState.SnapshotID == "" {
		t.Fatal("slow source partial progress must be preserved for resume")
	}
	firstCandidate := status.CandidateID
	firstSlowSnapshot := slowState.SnapshotID

	// The slow source's observation range expires while it is paused.
	entries, err := os.ReadDir(snapshotDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), firstSlowSnapshot) {
			if err := os.Remove(filepath.Join(snapshotDir, entry.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Run 2 after a full save/reopen: every object is rebuilt; only the joint
	// checkpoint, output dirs, and remaining snapshot files survive.
	fast2, mid2, slow2 := buildSources()
	runner2 := newRunner(fast2, mid2, slow2)
	if err := runner2.Run(ctx); err != nil {
		t.Fatal(err)
	}

	final, err := runner2.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !final.Complete {
		t.Fatal("plan must be complete once all required sources finished")
	}
	if final.CandidateID == firstCandidate || final.Generation != 2 {
		t.Fatalf("range re-selection must create a new joint candidate, got %+v", final)
	}
	if len(final.RangeChanges) != 1 {
		t.Fatalf("expected one recorded range change, got %+v", final.RangeChanges)
	}
	change := final.RangeChanges[0]
	if change.SourceID != "slow-archive" || change.OldSnapshotID != firstSlowSnapshot {
		t.Fatalf("range change must name the expired source and range, got %+v", change)
	}
	if !strings.Contains(change.Reason, "no longer available") {
		t.Fatalf("range change must explain the expiry, got %q", change.Reason)
	}

	// The partial redo must not recompute already-confirmed sources: run 2
	// wrote batches only for the slow source.
	run2Sink := sinks[final.CandidateID]
	if run2Sink.count("fast-entities") != 0 || run2Sink.count("mid-events") != 0 {
		t.Fatalf("confirmed sources must not be recomputed, got fast=%d mid=%d",
			run2Sink.count("fast-entities"), run2Sink.count("mid-events"))
	}
	if run2Sink.count("slow-archive") != 2 {
		t.Fatalf("slow source must be rescanned in 2 pages, got %d", run2Sink.count("slow-archive"))
	}

	// Old completed material is not mixed with the new candidate: the first
	// candidate's partial slow batch stays in its own directory.
	oldSlow := readBatches(t, filepath.Join(outputDir, firstCandidate))
	var oldSlowRecords int
	for _, batch := range oldSlow {
		if batch.SourceID == "slow-archive" {
			if batch.Snapshot != firstSlowSnapshot {
				t.Fatalf("old material must keep its original range, got %q", batch.Snapshot)
			}
			oldSlowRecords += len(batch.Records)
		}
	}
	if oldSlowRecords != 2 {
		t.Fatalf("old candidate must retain its 2 partial slow records, got %d", oldSlowRecords)
	}

	// Verify every exported record's source and observation range against the
	// final joint manifest.
	manifest := final.Manifest()
	for _, sourceID := range []string{"fast-entities", "mid-events", "slow-archive"} {
		if manifest[sourceID] == "" {
			t.Fatalf("manifest must pin a range for %s", sourceID)
		}
	}
	if manifest["slow-archive"] == firstSlowSnapshot {
		t.Fatal("manifest must point at the re-selected slow range")
	}
	checkCandidate := func(candidateID string, sources ...string) {
		for _, batch := range readBatches(t, filepath.Join(outputDir, candidateID)) {
			wantRange, ok := manifest[batch.SourceID]
			if !ok {
				t.Fatalf("batch from unknown source %q", batch.SourceID)
			}
			if batch.Snapshot != wantRange && batch.SourceID == "slow-archive" && candidateID == firstCandidate {
				continue // old candidate keeps the expired slow range by design
			}
			if batch.Snapshot != wantRange {
				t.Fatalf("batch %s range %q does not match manifest %q", batch.BatchID, batch.Snapshot, wantRange)
			}
			for _, record := range batch.Records {
				if record.Identity.Source != batch.SourceID {
					t.Fatalf("record %s source %q does not match batch source %q",
						record.DataID, record.Identity.Source, batch.SourceID)
				}
			}
		}
		_ = sources
	}
	checkCandidate(firstCandidate, "fast-entities", "mid-events", "slow-archive")
	checkCandidate(final.CandidateID, "slow-archive")

	// A further run is a no-op: the completed plan is not recomputed.
	before := 0
	for _, sink := range sinks {
		sink.mu.Lock()
		for _, n := range sink.writes {
			before += n
		}
		sink.mu.Unlock()
	}
	fast3, mid3, slow3 := buildSources()
	if err := newRunner(fast3, mid3, slow3).Run(ctx); err != nil {
		t.Fatal(err)
	}
	after := 0
	for _, sink := range sinks {
		sink.mu.Lock()
		for _, n := range sink.writes {
			after += n
		}
		sink.mu.Unlock()
	}
	if after != before {
		t.Fatalf("completed plan must not recompute, writes %d -> %d", before, after)
	}
}
