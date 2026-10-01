package datalink

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestResumeAfterFullProcessRebuild simulates a real process exit: the scan
// crashes after the second batch is written but before its checkpoint is
// durable, then every object (sources, sink, runner) is rebuilt from scratch
// with only the checkpoint file, output dir, and snapshot store surviving.
// The rebuilt sources see mutations made after the crash, yet the resumed scan
// must finish the original snapshot rather than switching to the latest data.
func TestResumeAfterFullProcessRebuild(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	snapshotDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")

	buildSources := func() (*EntityMapSource, *AppendLogSource) {
		entities := NewEntityMapSource("entities", "user", []Entity{
			{ID: "a", Version: "v1", Payload: map[string]any{"n": 1}},
			{ID: "b", Version: "v1", Payload: map[string]any{"n": 2}},
			{ID: "c", Version: "v1", Payload: map[string]any{"n": 3}},
			{ID: "d", Version: "v1", Payload: map[string]any{"n": 4}},
			{ID: "e", Version: "v1", Payload: map[string]any{"n": 5}},
		})
		entities.SnapshotDir = snapshotDir
		log := NewAppendLogSource("events", "event")
		log.SnapshotDir = snapshotDir
		for i := 0; i < 3; i++ {
			if err := log.Append(string(rune('a'+i)), "v1", map[string]any{"i": i}); err != nil {
				t.Fatal(err)
			}
		}
		return entities, log
	}

	entities, log := buildSources()
	crash := errors.New("simulated process exit")
	sink := &failOnceAfterWriteSink{next: FileSink{Dir: outputDir}, failAfterWrites: 2, err: crash}
	runner := Runner{
		Connectors:  []Connector{entities, log},
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        sink,
		PageSize:    2,
	}
	if err := runner.Run(ctx); !errors.Is(err, crash) {
		t.Fatalf("first run error = %v, want simulated crash", err)
	}

	// The process "exits" here: every object above is discarded. The source
	// data keeps changing before the replacement process starts.
	entities, log = buildSources()
	entities.Delete("d")
	entities.Upsert(Entity{ID: "a", Version: "v2", Payload: map[string]any{"n": 99}})
	entities.Upsert(Entity{ID: "f", Version: "v1", Payload: map[string]any{"n": 6}})
	if err := log.Append("d", "v1", map[string]any{"i": 3}); err != nil {
		t.Fatal(err)
	}

	sink.failAfterWrites = 0
	resumed := Runner{
		Connectors:  []Connector{entities, log},
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        sink,
		PageSize:    2,
	}
	if err := resumed.Run(ctx); err != nil {
		t.Fatalf("resume run = %v", err)
	}

	records := readOutputRecords(t, outputDir)
	want := []string{
		"entities/a", "entities/b", "entities/c", "entities/d", "entities/e",
		"events/a", "events/b", "events/c", "events/d",
	}
	assertQualifiedIDs(t, records, want)

	// The retried second batch must be the identical deterministic batch, so
	// the sink stays idempotent and no record appears twice.
	seen := map[string]int{}
	for _, record := range records {
		seen[record.DataID]++
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("record %s written %d times, want exactly 1", id, count)
		}
	}
	// The resumed pages still come from the pre-crash snapshot: entity "a"
	// keeps version v1 even though the live source now has v2.
	for _, record := range records {
		if record.Identity.Source == "entities" && record.Identity.ExternalID == "a" && record.Identity.Version != "v1" {
			t.Fatalf("resumed scan picked up post-crash version %q of entity a", record.Identity.Version)
		}
	}

	cursors, err := FileCheckpointStore{Path: checkpointPath}.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for sourceID, cursor := range cursors {
		if !cursor.Complete {
			t.Fatalf("source %s checkpoint = %+v, want complete", sourceID, cursor)
		}
	}
}

// TestSnapshotUnavailableReportsRecoverableRange drops the durable snapshot
// store before rebuilding, so the checkpointed snapshot is truly gone. The
// connector must fail loudly with the recoverable range instead of silently
// continuing on newer data.
func TestSnapshotUnavailableReportsRecoverableRange(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	snapshotDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")

	entities := NewEntityMapSource("entities", "user", []Entity{
		{ID: "a", Version: "v1", Payload: map[string]any{"n": 1}},
		{ID: "b", Version: "v1", Payload: map[string]any{"n": 2}},
		{ID: "c", Version: "v1", Payload: map[string]any{"n": 3}},
	})
	entities.SnapshotDir = snapshotDir
	crash := errors.New("simulated process exit")
	sink := &failOnceAfterWriteSink{next: FileSink{Dir: outputDir}, failAfterWrites: 2, err: crash}
	runner := Runner{
		Connectors:  []Connector{entities},
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        sink,
		PageSize:    1,
	}
	if err := runner.Run(ctx); !errors.Is(err, crash) {
		t.Fatalf("first run error = %v, want simulated crash", err)
	}

	// Lose the durable snapshot store, then rebuild every object.
	if err := os.RemoveAll(snapshotDir); err != nil {
		t.Fatal(err)
	}
	rebuilt := NewEntityMapSource("entities", "user", []Entity{
		{ID: "a", Version: "v1", Payload: map[string]any{"n": 1}},
		{ID: "b", Version: "v1", Payload: map[string]any{"n": 2}},
		{ID: "c", Version: "v1", Payload: map[string]any{"n": 3}},
	})
	rebuilt.SnapshotDir = snapshotDir
	sink.failAfterWrites = 0
	resumed := Runner{
		Connectors:  []Connector{rebuilt},
		Checkpoints: FileCheckpointStore{Path: checkpointPath},
		Sink:        sink,
		PageSize:    1,
	}
	err := resumed.Run(ctx)
	var unavailable *SnapshotUnavailableError
	if !errors.As(err, &unavailable) {
		t.Fatalf("resume error = %v, want SnapshotUnavailableError", err)
	}
	if unavailable.SourceID != "entities" || unavailable.Token != "1" {
		t.Fatalf("error = %+v, want source entities at token 1", unavailable)
	}
	if !strings.Contains(err.Error(), "clear this source's checkpoint") {
		t.Fatalf("error %q does not explain how to recover", err)
	}

	// The failed resume must not have written anything beyond the batches that
	// were already durable before the crash.
	records := readOutputRecords(t, outputDir)
	assertQualifiedIDs(t, records, []string{"entities/a", "entities/b"})
}
