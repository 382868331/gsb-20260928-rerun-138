package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// countingConnector records how often a snapshot is opened and how many pages
// are read, so a test can prove that a partial redo never recomputes sources
// that already confirmed their scope (a validation open reads zero pages).
type countingConnector struct {
	Connector
	opens int
	reads int
}

func (c *countingConnector) OpenSnapshot(ctx context.Context, resume Cursor) (Snapshot, error) {
	c.opens++
	snapshot, err := c.Connector.OpenSnapshot(ctx, resume)
	if err != nil {
		return nil, err
	}
	return &countingSnapshot{Snapshot: snapshot, parent: c}, nil
}

type countingSnapshot struct {
	Snapshot
	parent *countingConnector
}

func (s *countingSnapshot) Read(ctx context.Context, cursor Cursor, limit int) (Page, error) {
	s.parent.reads++
	return s.Snapshot.Read(ctx, cursor, limit)
}

// pausingConnector simulates a slow source being paused: after readsLeft
// pages it cancels the run context and fails the next read.
type pausingConnector struct {
	Connector
	cancel    context.CancelFunc
	readsLeft int
}

func (p *pausingConnector) OpenSnapshot(ctx context.Context, resume Cursor) (Snapshot, error) {
	snapshot, err := p.Connector.OpenSnapshot(ctx, resume)
	if err != nil {
		return nil, err
	}
	return &pausingSnapshot{Snapshot: snapshot, parent: p}, nil
}

type pausingSnapshot struct {
	Snapshot
	parent *pausingConnector
}

func (s *pausingSnapshot) Read(ctx context.Context, cursor Cursor, limit int) (Page, error) {
	if s.parent.readsLeft == 0 {
		s.parent.cancel()
		return Page{}, ctx.Err()
	}
	s.parent.readsLeft--
	return s.Snapshot.Read(ctx, cursor, limit)
}

func buildJointSources(t *testing.T, snapshotRoot string) (fast, volatile *EntityMapSource, slow *AppendLogSource) {
	t.Helper()
	fast = NewEntityMapSource("fast-entities", "user", []Entity{
		{ID: "u1", Version: "v1", Payload: map[string]any{"n": 1}},
		{ID: "u2", Version: "v1", Payload: map[string]any{"n": 2}},
		{ID: "u3", Version: "v1", Payload: map[string]any{"n": 3}},
	})
	fast.SnapshotDir = filepath.Join(snapshotRoot, "fast")
	volatile = NewEntityMapSource("volatile-entities", "doc", []Entity{
		{ID: "d1", Version: "v1", Payload: map[string]any{"n": 1}},
		{ID: "d2", Version: "v1", Payload: map[string]any{"n": 2}},
	})
	volatile.SnapshotDir = filepath.Join(snapshotRoot, "volatile")
	slow = NewAppendLogSource("slow-log", "event")
	slow.SnapshotDir = filepath.Join(snapshotRoot, "slow")
	for i := 0; i < 6; i++ {
		if err := slow.Append(string(rune('a'+i)), "v1", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	return fast, volatile, slow
}

func readOutputBatches(t *testing.T, dir string) []Batch {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var batches []Batch
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "batch-") {
			continue
		}
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
	return batches
}

func countBatchesBySource(batches []Batch) map[string]int {
	counts := map[string]int{}
	for _, batch := range batches {
		counts[batch.SourceID]++
	}
	return counts
}

// TestJointExportPauseInvalidateAndResume drives three fictional sources with
// different speeds through a joint export plan: the slow source is paused
// mid-scan, the volatile source's observation scope expires while paused, and
// the resumed run re-selects only that scope under a new joint candidate
// without recomputing the sources that already confirmed their results.
func TestJointExportPauseInvalidateAndResume(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	checkpointPath := filepath.Join(stateDir, "checkpoint.json")
	planStore := FileJointPlanStore{Dir: stateDir}

	// Phase 1: fast and volatile finish; the slow source is paused after two
	// of its three pages.
	runCtx, cancel := context.WithCancel(context.Background())
	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	slowPause := &pausingConnector{Connector: slow, cancel: cancel, readsLeft: 2}
	runner := JointRunner{
		PlanID:      "export-2026-10",
		Connectors:  []Connector{fast, volatile, slowPause},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err := runner.Run(runCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("paused run error = %v, want context.Canceled", err)
	}
	if plan.Complete {
		t.Fatal("plan is complete after pause, want incomplete")
	}
	if !plan.Scope("fast-entities").Complete || !plan.Scope("volatile-entities").Complete {
		t.Fatalf("fast and volatile scopes should be complete, got %+v", plan.Scopes)
	}
	if plan.Scope("slow-log").Complete {
		t.Fatal("slow scope should still be incomplete")
	}
	slowSnapshotID := plan.Scope("slow-log").SnapshotID
	volatileSnapshotID := plan.Scope("volatile-entities").SnapshotID

	// Cancellation must make explicit which parts stay reusable.
	cursors, err := FileCheckpointStore{Path: checkpointPath}.Load(runCtx)
	if err != nil {
		t.Fatal(err)
	}
	reusable := map[string]ReusablePart{}
	for _, part := range plan.ReusableParts(cursors) {
		reusable[part.SourceID] = part
	}
	if reusable["fast-entities"].State != "complete" || reusable["volatile-entities"].State != "complete" {
		t.Fatalf("finished sources must be reusable as complete, got %+v", reusable)
	}
	if reusable["slow-log"].State != "resumable" || reusable["slow-log"].Token != "4" {
		t.Fatalf("slow source should be resumable at token 4, got %+v", reusable["slow-log"])
	}
	if reusable["slow-log"].SnapshotID != slowSnapshotID {
		t.Fatalf("resumable part names snapshot %q, want %q", reusable["slow-log"].SnapshotID, slowSnapshotID)
	}

	// While paused, the volatile source's old observation scope expires.
	if err := os.RemoveAll(volatile.SnapshotDir); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen everything from disk (sources, stores, runner are all
	// rebuilt) and finish the plan.
	fast2, volatile2, slow2 := buildJointSources(t, snapshotRoot)
	fastCount := &countingConnector{Connector: fast2}
	volatileCount := &countingConnector{Connector: volatile2}
	slowCount := &countingConnector{Connector: slow2}
	resumed := JointRunner{
		PlanID:      "export-2026-10",
		Connectors:  []Connector{fastCount, volatileCount, slowCount},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = resumed.Run(context.Background())
	if err != nil {
		t.Fatalf("resumed run = %v", err)
	}
	if !plan.Complete {
		t.Fatalf("plan should be complete, got %+v", plan)
	}
	if plan.Generation != 2 {
		t.Fatalf("generation = %d, want 2 after re-scoping one source", plan.Generation)
	}

	// The partial redo must not recompute sources that already confirmed
	// their scope: fast is only validated (one open, zero page reads), and
	// slow resumes its pinned snapshot instead of restarting.
	if fastCount.opens != 1 || fastCount.reads != 0 {
		t.Fatalf("fast source opens=%d reads=%d, want 1 validation open and 0 reads", fastCount.opens, fastCount.reads)
	}
	if slowCount.opens != 1 {
		t.Fatalf("slow source opened %d times, want 1 resume of its pinned snapshot", slowCount.opens)
	}
	if volatileCount.opens != 2 {
		t.Fatalf("volatile source opened %d times, want 1 failed validation + 1 fresh scope", volatileCount.opens)
	}
	if slowCount.reads != 1 {
		t.Fatalf("slow source read %d pages on resume, want exactly the 1 remaining page", slowCount.reads)
	}
	if got := plan.Scope("slow-log").SnapshotID; got != slowSnapshotID {
		t.Fatalf("slow source scope = %q, want resumed snapshot %q", got, slowSnapshotID)
	}
	if got := plan.Scope("volatile-entities").SnapshotID; got == volatileSnapshotID {
		t.Fatalf("volatile source kept expired snapshot %q", got)
	}

	// The new joint candidate must point out the change.
	if len(plan.Notes) != 1 {
		t.Fatalf("notes = %v, want exactly one change note", plan.Notes)
	}
	note := plan.Notes[0]
	for _, want := range []string{"volatile-entities", volatileSnapshotID, "export-2026-10-g2"} {
		if !strings.Contains(note, want) {
			t.Fatalf("note %q does not mention %q", note, want)
		}
	}
	if plan.Scope("volatile-entities").Candidate != "export-2026-10-g2" {
		t.Fatalf("volatile candidate = %q, want export-2026-10-g2", plan.Scope("volatile-entities").Candidate)
	}
	if plan.Scope("fast-entities").Candidate != "export-2026-10-g1" {
		t.Fatalf("fast candidate = %q, want unchanged export-2026-10-g1", plan.Scope("fast-entities").Candidate)
	}

	// Verify every record's source and scope against the plan. Stale batches
	// of superseded candidates stay on disk for audit but are not part of the
	// joint material, so verification runs on the selected material.
	batches := readOutputBatches(t, outputDir)
	if err := VerifyJointRecords(plan, JointMaterial(plan, batches)); err != nil {
		t.Fatalf("VerifyJointRecords = %v", err)
	}

	// The joint material contains each source's confirmed batches only; the
	// volatile source's stale generation-1 batch stays on disk for audit but
	// is not mixed in.
	counts := countBatchesBySource(batches)
	if counts["fast-entities"] != 2 || counts["slow-log"] != 3 || counts["volatile-entities"] != 2 {
		t.Fatalf("batch counts = %v, want fast=2 slow=3 volatile=2 (incl. stale)", counts)
	}
	material := JointMaterial(plan, batches)
	materialCounts := countBatchesBySource(material)
	if materialCounts["fast-entities"] != 2 || materialCounts["slow-log"] != 3 || materialCounts["volatile-entities"] != 1 {
		t.Fatalf("joint material counts = %v, want fast=2 slow=3 volatile=1", materialCounts)
	}
	if materialCounts[JointManifestSource] != 1 {
		t.Fatalf("joint material should contain exactly one manifest, got %v", materialCounts)
	}
	for _, batch := range material {
		if batch.SourceID == JointManifestSource {
			continue
		}
		scope := plan.Scope(batch.SourceID)
		if batch.Snapshot != scope.SnapshotID {
			t.Fatalf("material batch of %q has snapshot %q, want pinned %q", batch.SourceID, batch.Snapshot, scope.SnapshotID)
		}
		for _, record := range batch.Records {
			if record.Identity.Source != batch.SourceID {
				t.Fatalf("record %q source %q does not match batch source %q", record.DataID, record.Identity.Source, batch.SourceID)
			}
		}
	}

	// No record is duplicated across the joint material.
	seen := map[string]int{}
	for _, batch := range material {
		for _, record := range batch.Records {
			seen[record.DataID]++
		}
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("record %s appears %d times in joint material, want 1", id, count)
		}
	}

	// A further run is a no-op: the plan is already complete.
	plan2, err := resumed.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !plan2.Complete || fastCount.opens != 1 || slowCount.opens != 1 || volatileCount.opens != 2 {
		t.Fatalf("completed plan should not reopen any source, got opens fast=%d slow=%d volatile=%d", fastCount.opens, slowCount.opens, volatileCount.opens)
	}
}

// TestJointMaterialRejectsStaleScope proves the verifier refuses to mix a
// re-scoped source's old completed material into the new joint candidate.
func TestJointMaterialRejectsStaleScope(t *testing.T) {
	plan := &JointPlan{
		PlanID:     "p",
		Generation: 2,
		Complete:   true,
		Scopes: []JointScope{
			{SourceID: "a", Candidate: "p-g2", SnapshotID: "snap-new", Complete: true},
		},
	}
	record, err := NewRecord("a", "k", "id1", "v1", map[string]any{"n": 1})
	if err != nil {
		t.Fatal(err)
	}
	stale := Batch{
		BatchID:   "stale",
		SourceID:  "a",
		Snapshot:  "snap-old",
		Records:   []Record{record},
		PlanID:    "p",
		Candidate: "p-g1",
	}
	if material := JointMaterial(plan, []Batch{stale}); len(material) != 0 {
		t.Fatalf("stale batch leaked into joint material: %+v", material)
	}
	if err := VerifyJointRecords(plan, []Batch{stale}); err == nil {
		t.Fatal("VerifyJointRecords accepted stale material from a superseded candidate")
	}
	if err := VerifyJointRecords(&JointPlan{PlanID: "p"}, nil); err == nil {
		t.Fatal("VerifyJointRecords accepted an incomplete plan")
	}
}

// TestJointRescopeAfterPauseReadsNewScopeFromScratch drives the regression
// where a slow source pauses after its first batch, its old observation
// scope expires while paused, and the resumed run re-selects a fresh scope.
// The new candidate must read the new scope from its own start: the old
// scope's offset and already-read prefix must not substitute for the new
// scope's reads. The run pauses a second time inside the new scope, then a
// full save/reopen finishes the plan, and the joint material must contain
// every record of the new scope exactly once.
func TestJointRescopeAfterPauseReadsNewScopeFromScratch(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	checkpointPath := filepath.Join(stateDir, "checkpoint.json")
	planStore := FileJointPlanStore{Dir: stateDir}

	// Phase 1: fast and volatile finish; the slow source is paused after its
	// first page (records a,b of six).
	runCtx, cancel := context.WithCancel(context.Background())
	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	slowPause := &pausingConnector{Connector: slow, cancel: cancel, readsLeft: 1}
	runner := JointRunner{
		PlanID:      "export-rescope",
		Connectors:  []Connector{fast, volatile, slowPause},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err := runner.Run(runCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("paused run error = %v, want context.Canceled", err)
	}
	if plan.Scope("slow-log").Complete {
		t.Fatal("slow scope should be incomplete after the pause")
	}
	oldSlowSnapshotID := plan.Scope("slow-log").SnapshotID

	// While paused, the slow source's old observation scope expires.
	if err := os.RemoveAll(slow.SnapshotDir); err != nil {
		t.Fatal(err)
	}

	// Phase 2: rebuild everything from disk and resume. The slow source's
	// expired scope is re-selected under a new candidate, then the source is
	// paused again after one page of the new scope.
	runCtx2, cancel2 := context.WithCancel(context.Background())
	fast2, volatile2, slow2 := buildJointSources(t, snapshotRoot)
	fastCount := &countingConnector{Connector: fast2}
	volatileCount := &countingConnector{Connector: volatile2}
	slowPause2 := &pausingConnector{Connector: slow2, cancel: cancel2, readsLeft: 1}
	resumed := JointRunner{
		PlanID:      "export-rescope",
		Connectors:  []Connector{fastCount, volatileCount, slowPause2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = resumed.Run(runCtx2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("second paused run error = %v, want context.Canceled", err)
	}
	if plan.Generation != 2 {
		t.Fatalf("generation = %d, want 2 after re-scoping the slow source", plan.Generation)
	}
	newSlowSnapshotID := plan.Scope("slow-log").SnapshotID
	if newSlowSnapshotID == "" || newSlowSnapshotID == oldSlowSnapshotID {
		t.Fatalf("slow scope snapshot = %q, want a fresh scope replacing %q", newSlowSnapshotID, oldSlowSnapshotID)
	}
	if plan.Scope("slow-log").Candidate != "export-rescope-g2" {
		t.Fatalf("slow candidate = %q, want export-rescope-g2", plan.Scope("slow-log").Candidate)
	}
	// The re-selected scope restarted from its own beginning: the first page
	// of the new scope was read before the second pause.
	if plan.Scope("slow-log").Complete {
		t.Fatal("slow scope should still be incomplete after the second pause")
	}

	// The confirmed scopes of the other sources were reused, not recomputed.
	if fastCount.opens != 1 || fastCount.reads != 0 {
		t.Fatalf("fast source opens=%d reads=%d, want 1 validation open and 0 reads", fastCount.opens, fastCount.reads)
	}
	if volatileCount.opens != 1 || volatileCount.reads != 0 {
		t.Fatalf("volatile source opens=%d reads=%d, want 1 validation open and 0 reads", volatileCount.opens, volatileCount.reads)
	}

	// The second cancellation must state exactly what stays reusable.
	cursors, err := FileCheckpointStore{Path: checkpointPath}.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reusable := map[string]ReusablePart{}
	for _, part := range plan.ReusableParts(cursors) {
		reusable[part.SourceID] = part
	}
	if reusable["fast-entities"].State != "complete" || reusable["volatile-entities"].State != "complete" {
		t.Fatalf("finished sources must be reusable as complete, got %+v", reusable)
	}
	if reusable["slow-log"].State != "resumable" ||
		reusable["slow-log"].Token != "2" ||
		reusable["slow-log"].SnapshotID != newSlowSnapshotID {
		t.Fatalf("slow part = %+v, want resumable at token 2 of the new scope %q", reusable["slow-log"], newSlowSnapshotID)
	}

	// Phase 3: save/reopen once more and finish the plan.
	fast3, volatile3, slow3 := buildJointSources(t, snapshotRoot)
	finisher := JointRunner{
		PlanID:      "export-rescope",
		Connectors:  []Connector{fast3, volatile3, slow3},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = finisher.Run(context.Background())
	if err != nil {
		t.Fatalf("finishing run = %v", err)
	}
	if !plan.Complete {
		t.Fatalf("plan should be complete, got %+v", plan)
	}
	if got := plan.Scope("slow-log").SnapshotID; got != newSlowSnapshotID {
		t.Fatalf("slow scope = %q, want the re-selected scope %q", got, newSlowSnapshotID)
	}
	if plan.Scope("fast-entities").Candidate != "export-rescope-g1" ||
		plan.Scope("volatile-entities").Candidate != "export-rescope-g1" {
		t.Fatalf("untouched scopes must keep candidate g1, got %+v", plan.Scopes)
	}

	// The joint material of the finished plan must contain every record of
	// the slow source's new scope exactly once, including the prefix the old
	// scope had already read before expiring.
	batches := readOutputBatches(t, outputDir)
	material := JointMaterial(plan, batches)
	if err := VerifyJointRecords(plan, material); err != nil {
		t.Fatalf("VerifyJointRecords = %v", err)
	}
	slowIDs := map[string]int{}
	for _, batch := range material {
		if batch.SourceID != "slow-log" {
			continue
		}
		if batch.Candidate != "export-rescope-g2" || batch.Snapshot != newSlowSnapshotID {
			t.Fatalf("slow batch %q names candidate %q snapshot %q, want g2 scope %q",
				batch.BatchID, batch.Candidate, batch.Snapshot, newSlowSnapshotID)
		}
		for _, record := range batch.Records {
			if record.Identity.Source != "slow-log" {
				t.Fatalf("record %q claims source %q", record.DataID, record.Identity.Source)
			}
			slowIDs[record.Identity.ExternalID]++
		}
	}
	for _, id := range []string{"a", "b", "c", "d", "e", "f"} {
		if slowIDs[id] != 1 {
			t.Fatalf("slow record %q appears %d times in joint material, want 1", id, slowIDs[id])
		}
	}

	// The expired scope's old batch stays on disk for audit but is not part
	// of the joint material.
	counts := countBatchesBySource(batches)
	if counts["slow-log"] != 4 {
		t.Fatalf("slow batch count on disk = %d, want 4 (1 stale + 3 current)", counts["slow-log"])
	}
	if got := countBatchesBySource(material)["slow-log"]; got != 3 {
		t.Fatalf("slow batches in joint material = %d, want 3", got)
	}
}
