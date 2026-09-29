package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestSnapshotPaginationIsStableWhenSourceChanges(t *testing.T) {
	ctx := context.Background()
	entitySource := NewEntityMapSource("entities", "user", []Entity{
		{ID: "a", Version: "v1", Payload: map[string]any{"name": "Ada"}},
		{ID: "b", Version: "v1", Payload: map[string]any{"name": "Bao"}},
		{ID: "c", Version: "v1", Payload: map[string]any{"name": "Cy"}},
	})
	logSource := NewAppendLogSource("events", "event")
	for i, name := range []string{"e1", "e2", "e3"} {
		if err := logSource.Append(name, "v1", map[string]any{"seq": i + 1}); err != nil {
			t.Fatal(err)
		}
	}

	entitySnapshot, err := entitySource.OpenSnapshot(ctx, Cursor{})
	if err != nil {
		t.Fatal(err)
	}
	entityPage1, err := entitySnapshot.Read(ctx, Cursor{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	logSnapshot, err := logSource.OpenSnapshot(ctx, Cursor{})
	if err != nil {
		t.Fatal(err)
	}
	logPage1, err := logSnapshot.Read(ctx, Cursor{}, 2)
	if err != nil {
		t.Fatal(err)
	}

	entitySource.Delete("b")
	entitySource.Upsert(Entity{ID: "a", Version: "v2", Payload: map[string]any{"name": "Ada changed"}})
	entitySource.Upsert(Entity{ID: "d", Version: "v1", Payload: map[string]any{"name": "New"}})
	if err := logSource.Append("e4", "v1", map[string]any{"seq": 4}); err != nil {
		t.Fatal(err)
	}
	if err := logSource.Append("e5", "v1", map[string]any{"seq": 5}); err != nil {
		t.Fatal(err)
	}

	entityPage2, err := entitySnapshot.Read(ctx, entityPage1.Next, 2)
	if err != nil {
		t.Fatal(err)
	}
	logPage2, err := logSnapshot.Read(ctx, logPage1.Next, 2)
	if err != nil {
		t.Fatal(err)
	}

	assertIDs(t, append(entityPage1.Records, entityPage2.Records...), []string{"a", "b", "c"})
	assertIDs(t, append(logPage1.Records, logPage2.Records...), []string{"e1", "e2", "e3"})
	if got := entityPage2.Records[0].Identity.Version; got != "v1" {
		t.Fatalf("snapshot returned changed version %q, want v1", got)
	}
}

func TestRunnerCrashAfterOutputBeforeCheckpointHasIdentifiableDuplicate(t *testing.T) {
	ctx := context.Background()
	outputDir := t.TempDir()
	checkpointPath := filepath.Join(t.TempDir(), "checkpoint.json")
	entitySource := NewEntityMapSource("entities", "user", []Entity{
		{ID: "a", Version: "v1", Payload: map[string]any{"n": 1}},
		{ID: "b", Version: "v1", Payload: map[string]any{"n": 2}},
		{ID: "c", Version: "v1", Payload: map[string]any{"n": 3}},
		{ID: "d", Version: "v1", Payload: map[string]any{"n": 4}},
		{ID: "e", Version: "v1", Payload: map[string]any{"n": 5}},
	})
	logSource := NewAppendLogSource("events", "event")
	for i := 0; i < 4; i++ {
		if err := logSource.Append(string(rune('a'+i)), "v1", map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}

	store := FileCheckpointStore{Path: checkpointPath}
	sink := &failOnceAfterWriteSink{next: FileSink{Dir: outputDir}, failAfterWrites: 1, err: errors.New("simulated crash")}
	runner := Runner{
		Connectors:  []Connector{entitySource, logSource},
		Checkpoints: store,
		Sink:        sink,
		PageSize:    2,
	}
	if err := runner.Run(ctx); !errors.Is(err, sink.err) {
		t.Fatalf("first run error = %v, want simulated crash", err)
	}
	if _, err := os.Stat(checkpointPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint path err = %v, want no checkpoint", err)
	}

	sink.failAfterWrites = 0
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("resume run = %v", err)
	}

	records := readOutputRecords(t, outputDir)
	want := []string{
		"events/a", "events/b", "events/c", "events/d",
		"entities/a", "entities/b", "entities/c", "entities/d", "entities/e",
	}
	assertQualifiedIDs(t, records, want)
	cursors, err := store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for sourceID, cursor := range cursors {
		if !cursor.Complete {
			t.Fatalf("source %s checkpoint = %+v, want complete", sourceID, cursor)
		}
	}
}

type failOnceAfterWriteSink struct {
	next            Sink
	failAfterWrites int
	writes          int
	err             error
}

func (s *failOnceAfterWriteSink) WriteBatch(ctx context.Context, batch Batch) error {
	if err := s.next.WriteBatch(ctx, batch); err != nil {
		return err
	}
	s.writes++
	if s.failAfterWrites > 0 && s.writes == s.failAfterWrites {
		return s.err
	}
	return nil
}

func assertIDs(t *testing.T, records []Record, want []string) {
	t.Helper()
	got := make([]string, len(records))
	for i, record := range records {
		got[i] = record.Identity.ExternalID
	}
	assertStrings(t, got, want)
}

func assertQualifiedIDs(t *testing.T, records []Record, want []string) {
	t.Helper()
	got := make([]string, len(records))
	for i, record := range records {
		got[i] = record.Identity.Source + "/" + record.Identity.ExternalID
	}
	sort.Strings(got)
	sort.Strings(want)
	assertStrings(t, got, want)
}

func assertStrings(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func readOutputRecords(t *testing.T, dir string) []Record {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var records []Record
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "batch-") {
			continue
		}
		var batch Batch
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &batch); err != nil {
			t.Fatal(err)
		}
		records = append(records, batch.Records...)
	}
	return records
}
