package datalink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestJointSlowSourceReScopedAfterPause is the regression test for a slow
// source paused after its first page: when the pinned observation scope
// expires during the pause, the replacement scope must be read in full under
// the new joint candidate. The old offset must not be carried over (which
// would skip records already read under the dead scope while still marking
// the new candidate complete), and the old batches stay on disk for audit
// without entering the joint material.
func TestJointSlowSourceReScopedAfterPause(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	checkpointPath := filepath.Join(stateDir, "checkpoint.json")
	planStore := FileJointPlanStore{Dir: stateDir}

	// Phase 1: fast and volatile sources finish; the slow source is paused
	// after only its first page (2 of 6 records).
	runCtx, cancel := context.WithCancel(context.Background())
	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	slowPause := &pausingConnector{Connector: slow, cancel: cancel, readsLeft: 1}
	runner := JointRunner{
		PlanID:      "export-slow",
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
		t.Fatal("plan must be incomplete after the slow source paused")
	}
	fastScope := plan.Scope("fast-entities")
	volatileScope := plan.Scope("volatile-entities")
	slowScope := plan.Scope("slow-log")
	if !fastScope.Complete || !volatileScope.Complete || slowScope.Complete {
		t.Fatalf("scope states = fast:%t volatile:%t slow:%t, want true,true,false", fastScope.Complete, volatileScope.Complete, slowScope.Complete)
	}
	oldSlowSnapshot := slowScope.SnapshotID
	if slowScope.Candidate != "export-slow-g1" {
		t.Fatalf("slow candidate = %q, want export-slow-g1", slowScope.Candidate)
	}

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
	if reusable["slow-log"].State != "resumable" || reusable["slow-log"].Token != "2" {
		t.Fatalf("slow source should be resumable at token 2, got %+v", reusable["slow-log"])
	}

	// The slow source's observation scope expires while it is paused.
	if err := os.RemoveAll(slow.SnapshotDir); err != nil {
		t.Fatal(err)
	}

	// Phase 2: rebuild every object from disk. The resumed run must detect the
	// dead scope, re-select the slow source alone under candidate g2 and read
	// the fresh scope from its own start. It is paused again after one page of
	// the new scope. The already confirmed sources are only validated.
	fast2, volatile2, slow2 := buildJointSources(t, snapshotRoot)
	fastCount := &countingConnector{Connector: fast2}
	volatileCount := &countingConnector{Connector: volatile2}
	slowCount := &countingConnector{Connector: slow2}
	runCtx2, cancel2 := context.WithCancel(context.Background())
	slowPause2 := &pausingConnector{Connector: slowCount, cancel: cancel2, readsLeft: 1}
	resumed := JointRunner{
		PlanID:      "export-slow",
		Connectors:  []Connector{fastCount, volatileCount, slowPause2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = resumed.Run(runCtx2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("second pause error = %v, want context.Canceled", err)
	}
	if plan.Generation != 2 {
		t.Fatalf("generation = %d, want 2 after re-scoping the slow source", plan.Generation)
	}
	if fastCount.opens != 1 || fastCount.reads != 0 {
		t.Fatalf("fast opens=%d reads=%d, want one validation open and no reads", fastCount.opens, fastCount.reads)
	}
	if volatileCount.opens != 1 || volatileCount.reads != 0 {
		t.Fatalf("volatile opens=%d reads=%d, want one validation open and no reads", volatileCount.opens, volatileCount.reads)
	}
	if slowCount.opens != 2 {
		t.Fatalf("slow opens=%d, want one failed resume of the dead scope plus one fresh scope", slowCount.opens)
	}
	if slowCount.reads != 1 {
		t.Fatalf("slow reads=%d, want exactly one page read from the start of the fresh scope", slowCount.reads)
	}
	slowScope = plan.Scope("slow-log")
	newSlowSnapshot := slowScope.SnapshotID
	if newSlowSnapshot == "" || newSlowSnapshot == oldSlowSnapshot {
		t.Fatalf("slow scope snapshot = %q, want a fresh snapshot distinct from expired %q", newSlowSnapshot, oldSlowSnapshot)
	}
	if slowScope.Candidate != "export-slow-g2" || slowScope.Complete {
		t.Fatalf("slow scope = %+v, want incomplete export-slow-g2", slowScope)
	}
	if plan.Scope("fast-entities").Candidate != "export-slow-g1" || plan.Scope("volatile-entities").Candidate != "export-slow-g1" {
		t.Fatalf("confirmed candidates must stay on g1, got %+v", plan.Scopes)
	}
	if len(plan.Notes) != 1 || !strings.Contains(plan.Notes[0], oldSlowSnapshot) || !strings.Contains(plan.Notes[0], "export-slow-g2") {
		t.Fatalf("notes = %v, want one note naming the expired scope and the new candidate", plan.Notes)
	}
	cursors, err = FileCheckpointStore{Path: checkpointPath}.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	reusable = map[string]ReusablePart{}
	for _, part := range plan.ReusableParts(cursors) {
		reusable[part.SourceID] = part
	}
	if reusable["slow-log"].State != "resumable" || reusable["slow-log"].SnapshotID != newSlowSnapshot || reusable["slow-log"].Token != "2" {
		t.Fatalf("re-scoped slow part = %+v, want resumable inside the fresh snapshot at token 2", reusable["slow-log"])
	}

	// The single page read under g2 must start at token 0: the dead scope's
	// offset 2 must not have skipped the first records of the new scope.
	batches := readOutputBatches(t, outputDir)
	var newSlowStarts []string
	oldSlowBatches := 0
	for _, batch := range batches {
		if batch.SourceID != "slow-log" {
			continue
		}
		switch batch.Candidate {
		case "export-slow-g2":
			if batch.Snapshot != newSlowSnapshot {
				t.Fatalf("g2 slow batch names snapshot %q, want %q", batch.Snapshot, newSlowSnapshot)
			}
			newSlowStarts = append(newSlowStarts, batch.StartToken)
		case "export-slow-g1":
			if batch.Snapshot != oldSlowSnapshot {
				t.Fatalf("stale slow batch names snapshot %q, want %q", batch.Snapshot, oldSlowSnapshot)
			}
			oldSlowBatches++
		}
	}
	sort.Strings(newSlowStarts)
	if len(newSlowStarts) != 1 || newSlowStarts[0] != "" {
		t.Fatalf("fresh scope page starts = %v, want only a page starting at token 0", newSlowStarts)
	}
	if oldSlowBatches != 1 {
		t.Fatalf("stale slow batches on disk = %d, want 1 kept for audit", oldSlowBatches)
	}

	// Phase 3: another real save-and-reopen. The fresh scope resumes inside
	// its own snapshot; confirmed sources are still not recomputed.
	fast3, volatile3, slow3 := buildJointSources(t, snapshotRoot)
	fastCount3 := &countingConnector{Connector: fast3}
	volatileCount3 := &countingConnector{Connector: volatile3}
	slowCount3 := &countingConnector{Connector: slow3}
	final := JointRunner{
		PlanID:      "export-slow",
		Connectors:  []Connector{fastCount3, volatileCount3, slowCount3},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = final.Run(context.Background())
	if err != nil {
		t.Fatalf("final run = %v", err)
	}
	if !plan.Complete || plan.CandidateID() != "export-slow-g2" {
		t.Fatalf("plan = complete:%t candidate:%q, want complete export-slow-g2", plan.Complete, plan.CandidateID())
	}
	if fastCount3.reads != 0 || volatileCount3.reads != 0 {
		t.Fatalf("confirmed sources must not be re-read, got fast reads=%d volatile reads=%d", fastCount3.reads, volatileCount3.reads)
	}
	if slowCount3.opens != 1 || slowCount3.reads != 2 {
		t.Fatalf("slow opens=%d reads=%d, want one resume reading the two remaining pages", slowCount3.opens, slowCount3.reads)
	}

	batches = readOutputBatches(t, outputDir)
	material := JointMaterial(plan, batches)
	if err := VerifyJointRecords(plan, material); err != nil {
		t.Fatalf("VerifyJointRecords = %v", err)
	}
	materialCounts := countBatchesBySource(material)
	if materialCounts["fast-entities"] != 2 || materialCounts["volatile-entities"] != 1 || materialCounts["slow-log"] != 3 {
		t.Fatalf("joint material batch counts = %v, want fast=2 volatile=1 slow=3", materialCounts)
	}
	if materialCounts[JointManifestSource] != 1 {
		t.Fatalf("joint material manifests = %d, want 1", materialCounts[JointManifestSource])
	}

	// The new scope independently covers all six slow records exactly once;
	// the records read under the dead scope are re-read under g2 and the stale
	// g1 batch is excluded from the joint material.
	newSlowStarts = nil
	seen := map[string]int{}
	var slowIDs []string
	for _, batch := range material {
		if batch.SourceID == JointManifestSource {
			continue
		}
		scope := plan.Scope(batch.SourceID)
		if batch.Candidate != scope.Candidate || batch.Snapshot != scope.SnapshotID {
			t.Fatalf("material batch of %q belongs to %q/%q, scope pins %q/%q", batch.SourceID, batch.Candidate, batch.Snapshot, scope.Candidate, scope.SnapshotID)
		}
		if batch.SourceID == "slow-log" {
			newSlowStarts = append(newSlowStarts, batch.StartToken)
		}
		for _, record := range batch.Records {
			if record.Identity.Source != batch.SourceID {
				t.Fatalf("record %q source %q != batch source %q", record.DataID, record.Identity.Source, batch.SourceID)
			}
			seen[record.DataID]++
			if batch.SourceID == "slow-log" {
				slowIDs = append(slowIDs, record.Identity.ExternalID)
			}
		}
	}
	sort.Strings(newSlowStarts)
	if got := strings.Join(newSlowStarts, ","); got != ",2,4" {
		t.Fatalf("g2 slow page starts = %q, want 0,2,4 (full independent read)", got)
	}
	sort.Strings(slowIDs)
	if got := strings.Join(slowIDs, ","); got != "a,b,c,d,e,f" {
		t.Fatalf("slow records in joint material = %q, want a,b,c,d,e,f", got)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("record %s appears %d times in joint material, want 1", id, count)
		}
	}

	// The stale g1 material remains on disk solely for audit and must never be
	// selected as part of the completed joint export.
	diskCounts := countBatchesBySource(batches)
	if diskCounts["slow-log"] != 4 {
		t.Fatalf("slow batches on disk = %d, want 3 complete g2 batches plus 1 stale g1 batch", diskCounts["slow-log"])
	}
	if len(JointMaterial(plan, batches)) != len(material) {
		t.Fatal("joint material selection is not stable")
	}

	// After completion every reusable part is reported as complete.
	cursors, err = FileCheckpointStore{Path: checkpointPath}.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range plan.ReusableParts(cursors) {
		if part.State != "complete" {
			t.Fatalf("part %+v should be complete after the joint export", part)
		}
	}
}
