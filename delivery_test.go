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

// TestDeliveryFreezeCompareAndReopen drives the full delivery checklist
// flow across two joint exports with interleaved source changes:
//   - export-1: the volatile source's observation scope expires mid-plan and
//     is re-selected alone (refetched), the fast source keeps its previously
//     confirmed results (reused), and the slow source is resumed inside its
//     pinned scope. The operator reviews the origins and freezes delivery D1.
//   - afterwards the slow source gains new records and a volatile entity
//     changes version; a second joint export freezes delivery D2.
//   - a comparison view names, per source, the added and changed records
//     with reasons; after saving and reopening everything from disk, both
//     frozen deliveries still verify against the delivered batches, and
//     tampering with a delivered batch is detected.
func TestDeliveryFreezeCompareAndReopen(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	deliveriesDir := t.TempDir()
	planStore := FileJointPlanStore{Dir: stateDir}
	deliveryStore := FileDeliveryStore{Dir: deliveriesDir}
	ctx := context.Background()

	// Phase 1: fast and volatile finish; the slow source is paused after two
	// of its three pages.
	runCtx, cancel := context.WithCancel(ctx)
	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	slowPause := &pausingConnector{Connector: slow, cancel: cancel, readsLeft: 2}
	runner := JointRunner{
		PlanID:      "export-1",
		Connectors:  []Connector{fast, volatile, slowPause},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: filepath.Join(stateDir, "checkpoint-1.json")},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	if _, err := runner.Run(runCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("paused run error = %v, want context.Canceled", err)
	}

	// The volatile source's observation scope expires while paused.
	if err := os.RemoveAll(volatile.SnapshotDir); err != nil {
		t.Fatal(err)
	}

	// Rebuild everything from disk and finish export-1: the volatile source
	// is re-selected alone, the fast source keeps its confirmed results.
	fast2, volatile2, slow2 := buildJointSources(t, snapshotRoot)
	resumed := JointRunner{
		PlanID:      "export-1",
		Connectors:  []Connector{fast2, volatile2, slow2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: filepath.Join(stateDir, "checkpoint-1.json")},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan1, err := resumed.Run(ctx)
	if err != nil {
		t.Fatalf("resumed run = %v", err)
	}
	if !plan1.Complete || plan1.CandidateID() != "export-1-g2" {
		t.Fatalf("plan1 = complete:%t candidate:%q, want complete export-1-g2", plan1.Complete, plan1.CandidateID())
	}

	// The operator reviews the delivery before freezing it: per source the
	// view shows the actual observation scope and how the results were
	// obtained.
	batches := readOutputBatches(t, outputDir)
	review1, err := ReviewDelivery(plan1, batches, DeliverySelection{})
	if err != nil {
		t.Fatalf("ReviewDelivery = %v", err)
	}
	if review1.Candidate != "export-1-g2" || len(review1.Scopes) != 3 {
		t.Fatalf("review1 = candidate %q scopes %d, want export-1-g2 with 3 scopes", review1.Candidate, len(review1.Scopes))
	}
	origins := map[string]DeliveryScopeView{}
	for _, scope := range review1.Scopes {
		origins[scope.SourceID] = scope
	}
	if got := origins["volatile-entities"].Origin; got != OriginRefetched {
		t.Fatalf("volatile origin = %q, want %q (scope expired and was re-selected)", got, OriginRefetched)
	}
	if got := origins["fast-entities"].Origin; got != OriginReused {
		t.Fatalf("fast origin = %q, want %q (previously confirmed results kept)", got, OriginReused)
	}
	if got := origins["slow-log"].Origin; got != OriginReused {
		t.Fatalf("slow origin = %q, want %q (resumed inside its pinned scope)", got, OriginReused)
	}
	if origins["volatile-entities"].Candidate != "export-1-g2" || origins["fast-entities"].Candidate != "export-1-g1" {
		t.Fatalf("review candidates = %+v, want volatile on g2 and fast on g1", review1.Scopes)
	}
	if !strings.Contains(origins["volatile-entities"].Reason, "expired") {
		t.Fatalf("refetched reason %q should name the expiration", origins["volatile-entities"].Reason)
	}
	if origins["fast-entities"].Records != 3 || origins["volatile-entities"].Records != 2 || origins["slow-log"].Records != 6 {
		t.Fatalf("review record counts = %+v, want fast=3 volatile=2 slow=6", review1.Scopes)
	}

	// Freeze delivery D1 and save it.
	delivery1, err := review1.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery1.Records) != 11 {
		t.Fatalf("delivery1 frozen records = %d, want 11", len(delivery1.Records))
	}
	if err := deliveryStore.SaveDelivery(ctx, delivery1); err != nil {
		t.Fatal(err)
	}

	// Interleaved source changes after the freeze: the slow source gains two
	// new records and a volatile entity moves to a new version. The fast
	// source keeps its old results.
	for _, id := range []string{"g", "h"} {
		if err := slow2.Append(id, "v1", map[string]any{"i": id}); err != nil {
			t.Fatal(err)
		}
	}
	volatile2.Upsert(Entity{ID: "d2", Version: "v2", Payload: map[string]any{"n": 22}})

	// Phase 2: a second joint export re-imports all sources. This must not
	// silently change the already frozen D1.
	runner2 := JointRunner{
		PlanID:      "export-2",
		Connectors:  []Connector{fast2, volatile2, slow2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: filepath.Join(stateDir, "checkpoint-2.json")},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan2, err := runner2.Run(ctx)
	if err != nil {
		t.Fatalf("export-2 = %v", err)
	}
	if !plan2.Complete {
		t.Fatal("export-2 should be complete")
	}

	batches = readOutputBatches(t, outputDir)
	review2, err := ReviewDelivery(plan2, batches, DeliverySelection{})
	if err != nil {
		t.Fatalf("ReviewDelivery export-2 = %v", err)
	}
	for _, scope := range review2.Scopes {
		if scope.Origin != OriginFresh {
			t.Fatalf("export-2 scope %+v: a fresh plan must observe every source fresh", scope)
		}
	}
	delivery2, err := review2.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery2.Records) != 13 {
		t.Fatalf("delivery2 frozen records = %d, want 13", len(delivery2.Records))
	}
	if err := deliveryStore.SaveDelivery(ctx, delivery2); err != nil {
		t.Fatal(err)
	}

	// The re-import and the new records must not have touched D1.
	if err := VerifyDelivery(delivery1, batches); err != nil {
		t.Fatalf("delivery1 changed after re-import: %v", err)
	}

	// The comparison view names, per source, what was added and what changed.
	comparison, err := CompareDeliveries(delivery1, delivery2)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Sources) != 2 {
		t.Fatalf("comparison sources = %+v, want changes only for slow-log and volatile-entities", comparison.Sources)
	}
	if comparison.Sources[0].SourceID != "slow-log" || comparison.Sources[1].SourceID != "volatile-entities" {
		t.Fatalf("comparison sources = %+v, want slow-log then volatile-entities", comparison.Sources)
	}
	added := comparison.Sources[0].Changes
	if len(added) != 2 || added[0].Change != "added" || added[1].Change != "added" {
		t.Fatalf("slow-log changes = %+v, want two additions", added)
	}
	if added[0].ExternalID != "g" || added[1].ExternalID != "h" {
		t.Fatalf("added records = %q, %q, want g and h", added[0].ExternalID, added[1].ExternalID)
	}
	if !strings.Contains(added[0].Reason, "after the first delivery was frozen") {
		t.Fatalf("addition reason %q should explain the record arrived after the freeze", added[0].Reason)
	}
	changed := comparison.Sources[1].Changes
	if len(changed) != 1 || changed[0].Change != "changed" || changed[0].ExternalID != "d2" {
		t.Fatalf("volatile changes = %+v, want one content change on d2", changed)
	}
	if changed[0].OldVersion != "v1" || changed[0].NewVersion != "v2" || !strings.Contains(changed[0].Reason, "v1") {
		t.Fatalf("change = %+v, want version v1 -> v2 with a reason", changed[0])
	}

	// Save and reopen: a fresh store re-loads both deliveries, and both
	// still verify against the delivered batches on disk.
	reopened := FileDeliveryStore{Dir: deliveriesDir}
	loaded1, err := reopened.LoadDelivery(ctx, delivery1.DeliveryID)
	if err != nil || loaded1 == nil {
		t.Fatalf("reload delivery1 = %v, %v", loaded1, err)
	}
	loaded2, err := reopened.LoadDelivery(ctx, delivery2.DeliveryID)
	if err != nil || loaded2 == nil {
		t.Fatalf("reload delivery2 = %v, %v", loaded2, err)
	}
	batches = readOutputBatches(t, outputDir)
	if err := VerifyDelivery(loaded1, batches); err != nil {
		t.Fatalf("re-verify delivery1 after reopen = %v", err)
	}
	if err := VerifyDelivery(loaded2, batches); err != nil {
		t.Fatalf("re-verify delivery2 after reopen = %v", err)
	}
	recomparison, err := CompareDeliveries(loaded1, loaded2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recomparison.Sources) != 2 || len(recomparison.Sources[0].Changes) != 2 || len(recomparison.Sources[1].Changes) != 1 {
		t.Fatalf("reopened comparison = %+v, want the same changes as before reopen", recomparison.Sources)
	}

	// Tampering with a delivered batch is detected, not silently accepted.
	tampered := false
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "batch-") {
			continue
		}
		path := filepath.Join(outputDir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var batch Batch
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatal(err)
		}
		if batch.PlanID != "export-1" || batch.SourceID != "slow-log" || len(batch.Records) == 0 {
			continue
		}
		batch.Records[0].PayloadHash = "tampered"
		data, err = json.MarshalIndent(batch, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		tampered = true
		break
	}
	if !tampered {
		t.Fatal("no export-1 slow-log batch found to tamper with")
	}
	batches = readOutputBatches(t, outputDir)
	if err := VerifyDelivery(loaded1, batches); err == nil {
		t.Fatal("VerifyDelivery accepted a tampered delivered batch")
	}
}

// TestDeliveryReviewSelectionAndIncomplete covers the operator's record
// range selection and the refusal to review an incomplete plan.
func TestDeliveryReviewSelectionAndIncomplete(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	ctx := context.Background()

	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	runner := JointRunner{
		PlanID:      "export-sel",
		Connectors:  []Connector{fast, volatile, slow},
		Plans:       FileJointPlanStore{Dir: stateDir},
		Checkpoints: FileCheckpointStore{Path: filepath.Join(stateDir, "checkpoint.json")},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	if _, err := ReviewDelivery(&JointPlan{PlanID: "export-sel"}, nil, DeliverySelection{}); err == nil {
		t.Fatal("ReviewDelivery accepted an incomplete plan")
	}
	plan, err := runner.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	batches := readOutputBatches(t, outputDir)

	// The operator selects only the slow source and at most two records.
	review, err := ReviewDelivery(plan, batches, DeliverySelection{Sources: []string{"slow-log"}, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(review.Scopes) != 1 || review.Scopes[0].SourceID != "slow-log" || review.Scopes[0].Records != 2 {
		t.Fatalf("selected review = %+v, want only slow-log with 2 records", review.Scopes)
	}
	delivery, err := review.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if len(delivery.Records) != 2 {
		t.Fatalf("frozen records = %d, want the selected 2", len(delivery.Records))
	}
	for _, record := range delivery.Records {
		if record.Source != "slow-log" {
			t.Fatalf("frozen record %q comes from %q, want only slow-log", record.DataID, record.Source)
		}
	}
	store := FileDeliveryStore{Dir: t.TempDir()}
	if err := store.SaveDelivery(ctx, delivery); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadDelivery(ctx, delivery.DeliveryID)
	if err != nil || loaded == nil {
		t.Fatalf("reload = %v, %v", loaded, err)
	}
	if err := VerifyDelivery(loaded, batches); err != nil {
		t.Fatalf("VerifyDelivery on selected range = %v", err)
	}
	if missing, err := store.LoadDelivery(ctx, "no-such-delivery"); err != nil || missing != nil {
		t.Fatalf("missing delivery = %v, %v, want nil, nil", missing, err)
	}
}

// TestCompareDeliveriesRemoved covers the removal branch of the comparison
// view with two synthetic frozen deliveries.
func TestCompareDeliveriesRemoved(t *testing.T) {
	record := func(source, id, version string) FrozenRecord {
		r, err := NewRecord(source, "k", id, version, map[string]any{"id": id})
		if err != nil {
			t.Fatal(err)
		}
		return freezeRecord(r)
	}
	from := &Delivery{DeliveryID: "d1", Records: []FrozenRecord{record("s", "x", "v1"), record("s", "y", "v1")}}
	to := &Delivery{DeliveryID: "d2", Records: []FrozenRecord{record("s", "x", "v1")}}
	comparison, err := CompareDeliveries(from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(comparison.Sources) != 1 || len(comparison.Sources[0].Changes) != 1 {
		t.Fatalf("comparison = %+v, want one change", comparison)
	}
	change := comparison.Sources[0].Changes[0]
	if change.Change != "removed" || change.ExternalID != "y" || change.OldVersion != "v1" {
		t.Fatalf("change = %+v, want removal of y", change)
	}
	if !strings.Contains(change.Reason, "absent") {
		t.Fatalf("removal reason %q should explain the record disappeared", change.Reason)
	}
	if _, err := CompareDeliveries(from, nil); err == nil {
		t.Fatal("CompareDeliveries accepted a missing delivery")
	}
}
