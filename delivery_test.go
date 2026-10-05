package datalink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// cancelOnOpenConnector cancels the run context when the runner tries to
// open this source's snapshot, simulating a source that never gets started
// before the run is interrupted.
type cancelOnOpenConnector struct {
	Connector
	cancel context.CancelFunc
}

func (c *cancelOnOpenConnector) OpenSnapshot(ctx context.Context, resume Cursor) (Snapshot, error) {
	c.cancel()
	return nil, ctx.Err()
}

func freezeAndSave(t *testing.T, plan *JointPlan, outputDir string, store DeliveryStore) *Delivery {
	t.Helper()
	delivery, err := FreezeDelivery(plan, readOutputBatches(t, outputDir))
	if err != nil {
		t.Fatalf("FreezeDelivery = %v", err)
	}
	if err := store.SaveDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("SaveDelivery = %v", err)
	}
	return delivery
}

// TestDeliveryReviewFreezeCompareAndReverify drives the interleaved demo:
// one source keeps its previously confirmed result, one source's observation
// scope expires and is re-selected, and one source contributes newly added
// records. The delivery is reviewed, frozen, and compared against a later
// delivery; after a real save-and-reopen both deliveries still re-verify.
func TestDeliveryReviewFreezeCompareAndReverify(t *testing.T) {
	outputDir := t.TempDir()
	snapshotRoot := t.TempDir()
	stateDir := t.TempDir()
	checkpointPath := filepath.Join(stateDir, "checkpoint.json")
	planStore := FileJointPlanStore{Dir: stateDir}
	deliveryStore := FileDeliveryStore{Dir: stateDir}

	// Phase 1: fast and volatile confirm their scopes; the run is cancelled
	// before the slow source even opens a snapshot, so its scope stays
	// pending with no observation range selected yet.
	runCtx, cancel := context.WithCancel(context.Background())
	fast, volatile, slow := buildJointSources(t, snapshotRoot)
	runner := JointRunner{
		PlanID:      "export-a",
		Connectors:  []Connector{fast, volatile, &cancelOnOpenConnector{Connector: slow, cancel: cancel}},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err := runner.Run(runCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted run error = %v, want context.Canceled", err)
	}
	if !plan.Scope("fast-entities").Complete || !plan.Scope("volatile-entities").Complete {
		t.Fatalf("fast and volatile scopes should be complete, got %+v", plan.Scopes)
	}
	if plan.Scope("slow-log").Complete {
		t.Fatal("slow scope must still be pending")
	}

	// While the run is interrupted: the volatile source's observation scope
	// expires, and the slow source keeps accepting new records.
	if err := os.RemoveAll(volatile.SnapshotDir); err != nil {
		t.Fatal(err)
	}

	// Phase 2: rebuild every object from disk. The slow source has two extra
	// records appended before its first scope is ever selected.
	fast2, volatile2, slow2 := buildJointSources(t, snapshotRoot)
	if err := slow2.Append("g", "v1", map[string]any{"i": 6}); err != nil {
		t.Fatal(err)
	}
	if err := slow2.Append("h", "v1", map[string]any{"i": 7}); err != nil {
		t.Fatal(err)
	}
	resumed := JointRunner{
		PlanID:      "export-a",
		Connectors:  []Connector{fast2, volatile2, slow2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	plan, err = resumed.Run(context.Background())
	if err != nil {
		t.Fatalf("resumed run = %v", err)
	}
	if !plan.Complete || plan.Generation != 2 {
		t.Fatalf("plan complete=%t generation=%d, want complete generation 2", plan.Complete, plan.Generation)
	}

	// The operator reviews the delivery before freezing it: fast reused its
	// earlier confirmation, volatile was re-selected after its scope
	// expired, and slow was read fresh including the newly appended records.
	batches := readOutputBatches(t, outputDir)
	review, err := BuildDeliveryReview(plan, batches)
	if err != nil {
		t.Fatalf("BuildDeliveryReview = %v", err)
	}
	if !review.Complete || review.Candidate != "export-a-g2" {
		t.Fatalf("review = %+v, want complete candidate export-a-g2", review)
	}
	items := map[string]DeliveryReviewItem{}
	for _, item := range review.Items {
		items[item.SourceID] = item
	}
	fastItem := items["fast-entities"]
	if fastItem.Origin != ScopeOriginReused || fastItem.Records != 3 || fastItem.Candidate != "export-a-g1" {
		t.Fatalf("fast review item = %+v, want reused g1 scope with 3 records", fastItem)
	}
	if !strings.Contains(fastItem.Reason, "reused") {
		t.Fatalf("fast reason %q should explain the reuse", fastItem.Reason)
	}
	volatileItem := items["volatile-entities"]
	if volatileItem.Origin != ScopeOriginRescoped || volatileItem.Records != 2 || volatileItem.Candidate != "export-a-g2" {
		t.Fatalf("volatile review item = %+v, want rescoped g2 scope with 2 records", volatileItem)
	}
	if !strings.Contains(volatileItem.Reason, "expired") {
		t.Fatalf("volatile reason %q should name the expired scope", volatileItem.Reason)
	}
	slowItem := items["slow-log"]
	if slowItem.Origin != ScopeOriginFresh || slowItem.Records != 8 || slowItem.Candidate != "export-a-g2" {
		t.Fatalf("slow review item = %+v, want fresh g2 scope with 8 records", slowItem)
	}

	// Freeze the reviewed record set into the first delivery.
	delivery1 := freezeAndSave(t, plan, outputDir, deliveryStore)
	if delivery1.DeliveryID != "export-a@export-a-g2" {
		t.Fatalf("delivery id = %q, want export-a@export-a-g2", delivery1.DeliveryID)
	}
	if len(delivery1.Records) != 13 {
		t.Fatalf("frozen records = %d, want 13 (3 fast + 2 volatile + 8 slow)", len(delivery1.Records))
	}
	if len(delivery1.Review.Items) != 3 {
		t.Fatalf("frozen review items = %d, want 3", len(delivery1.Review.Items))
	}

	// After the freeze the sources keep changing: fast loses an entity,
	// volatile rewrites one entity's content, and the slow log grows.
	fast2.Delete("u3")
	volatile2.Upsert(Entity{ID: "d1", Version: "v2", Payload: map[string]any{"n": 10}})
	if err := slow2.Append("i", "v1", map[string]any{"i": 8}); err != nil {
		t.Fatal(err)
	}

	// A new import (a fresh joint plan) picks up the changed sources and is
	// frozen into a second delivery.
	second := JointRunner{
		PlanID:      "export-b",
		Connectors:  []Connector{fast2, volatile2, slow2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	planB, err := second.Run(context.Background())
	if err != nil {
		t.Fatalf("second import = %v", err)
	}
	if !planB.Complete {
		t.Fatal("second import should complete")
	}
	delivery2 := freezeAndSave(t, planB, outputDir, deliveryStore)
	if len(delivery2.Records) != 13 {
		t.Fatalf("second delivery records = %d, want 13 (2 fast + 2 volatile + 9 slow)", len(delivery2.Records))
	}

	// The comparison view points out, per source, what was added, what
	// disappeared, and what changed content, each with its reason.
	comparisons, err := CompareDeliveries(delivery1, delivery2)
	if err != nil {
		t.Fatalf("CompareDeliveries = %v", err)
	}
	bySource := map[string]SourceComparison{}
	for _, comparison := range comparisons {
		bySource[comparison.SourceID] = comparison
	}
	fastDiff := bySource["fast-entities"]
	if len(fastDiff.Removed) != 1 || fastDiff.Removed[0].ExternalID != "u3" || len(fastDiff.Added) != 0 || len(fastDiff.Changed) != 0 {
		t.Fatalf("fast diff = %+v, want only u3 removed", fastDiff)
	}
	if !strings.Contains(fastDiff.Removed[0].Reason, "absent") {
		t.Fatalf("removal reason %q should explain the disappearance", fastDiff.Removed[0].Reason)
	}
	volatileDiff := bySource["volatile-entities"]
	if len(volatileDiff.Changed) != 1 || volatileDiff.Changed[0].ExternalID != "d1" ||
		volatileDiff.Changed[0].OldVersion != "v1" || volatileDiff.Changed[0].NewVersion != "v2" {
		t.Fatalf("volatile diff = %+v, want only d1 changed v1 -> v2", volatileDiff)
	}
	if !strings.Contains(volatileDiff.Changed[0].Reason, "content changed") {
		t.Fatalf("change reason %q should explain the content change", volatileDiff.Changed[0].Reason)
	}
	slowDiff := bySource["slow-log"]
	if len(slowDiff.Added) != 1 || slowDiff.Added[0].ExternalID != "i" || len(slowDiff.Removed) != 0 || len(slowDiff.Changed) != 0 {
		t.Fatalf("slow diff = %+v, want only record i added", slowDiff)
	}
	if !strings.Contains(slowDiff.Added[0].Reason, "present only in newer delivery") {
		t.Fatalf("addition reason %q should explain the addition", slowDiff.Added[0].Reason)
	}

	// Even more source growth and another re-import happen after both
	// deliveries were frozen; none of it may touch the frozen content.
	if err := slow2.Append("j", "v1", map[string]any{"i": 9}); err != nil {
		t.Fatal(err)
	}
	if err := slow2.Append("k", "v1", map[string]any{"i": 10}); err != nil {
		t.Fatal(err)
	}
	third := JointRunner{
		PlanID:      "export-c",
		Connectors:  []Connector{fast2, volatile2, slow2},
		Plans:       planStore,
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        FileSink{Dir: outputDir},
		PageSize:    2,
	}
	if _, err := third.Run(context.Background()); err != nil {
		t.Fatalf("third import = %v", err)
	}

	// Save-and-reopen: rebuild every object from disk and re-verify both
	// frozen deliveries against the current state of the output directory.
	reopenedPlans := FileJointPlanStore{Dir: stateDir}
	reopenedDeliveries := FileDeliveryStore{Dir: stateDir}
	diskBatches := readOutputBatches(t, outputDir)
	for _, id := range []string{delivery1.DeliveryID, delivery2.DeliveryID} {
		delivery, err := reopenedDeliveries.LoadDelivery(context.Background(), id)
		if err != nil || delivery == nil {
			t.Fatalf("LoadDelivery(%q) = %v, %v", id, delivery, err)
		}
		loadedPlan, err := reopenedPlans.LoadPlan(context.Background(), delivery.PlanID)
		if err != nil || loadedPlan == nil {
			t.Fatalf("LoadPlan(%q) = %v, %v", delivery.PlanID, loadedPlan, err)
		}
		if err := VerifyDelivery(delivery, loadedPlan, diskBatches); err != nil {
			t.Fatalf("VerifyDelivery(%q) after reopen = %v", id, err)
		}
	}

	// The saved review still explains the first delivery after the reopen.
	reloaded1, err := reopenedDeliveries.LoadDelivery(context.Background(), delivery1.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	reloadedItems := map[string]DeliveryReviewItem{}
	for _, item := range reloaded1.Review.Items {
		reloadedItems[item.SourceID] = item
	}
	if reloadedItems["fast-entities"].Origin != ScopeOriginReused ||
		reloadedItems["volatile-entities"].Origin != ScopeOriginRescoped ||
		reloadedItems["slow-log"].Origin != ScopeOriginFresh {
		t.Fatalf("reloaded review origins = %+v, want reused/rescoped/fresh", reloaded1.Review.Items)
	}

	// The comparison is reproducible from the reloaded deliveries alone.
	reloaded2, err := reopenedDeliveries.LoadDelivery(context.Background(), delivery2.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	recomputed, err := CompareDeliveries(reloaded1, reloaded2)
	if err != nil {
		t.Fatal(err)
	}
	if len(recomputed) != len(comparisons) {
		t.Fatalf("recomputed comparison has %d sources, want %d", len(recomputed), len(comparisons))
	}
	for i := range recomputed {
		if len(recomputed[i].Added) != len(comparisons[i].Added) ||
			len(recomputed[i].Removed) != len(comparisons[i].Removed) ||
			len(recomputed[i].Changed) != len(comparisons[i].Changed) {
			t.Fatalf("recomputed comparison for %q differs: %+v vs %+v",
				comparisons[i].SourceID, recomputed[i], comparisons[i])
		}
	}
}

// TestFreezeDeliveryRejectsIncompletePlan proves a delivery cannot be frozen
// before every selected scope confirmed its observation range.
func TestFreezeDeliveryRejectsIncompletePlan(t *testing.T) {
	plan := &JointPlan{PlanID: "p", Generation: 1}
	if _, err := FreezeDelivery(plan, nil); err == nil {
		t.Fatal("FreezeDelivery accepted an incomplete plan")
	}
	if _, err := BuildDeliveryReview(nil, nil); err == nil {
		t.Fatal("BuildDeliveryReview accepted a missing plan")
	}
	if err := VerifyDelivery(nil, plan, nil); err == nil {
		t.Fatal("VerifyDelivery accepted a missing delivery")
	}
	if _, err := CompareDeliveries(nil, &Delivery{}); err == nil {
		t.Fatal("CompareDeliveries accepted a missing delivery")
	}
}
