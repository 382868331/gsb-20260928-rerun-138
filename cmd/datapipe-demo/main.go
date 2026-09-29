package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"datapipe/internal/datapipe"
	"datapipe/internal/source"
)

func main() {
	stateDir := flag.String("state-dir", "./runtime-demo", "directory for output, progress, and source snapshots")
	crashAfterBatch := flag.Int64("crash-after-batch", 0, "exit after this total batch number (0 disables injection)")
	mutateOnResume := flag.Bool("mutate-on-resume", false, "add and delete fictional source rows after a saved batch")
	flag.Parse()

	ctx := context.Background()
	if err := run(ctx, *stateDir, *crashAfterBatch, *mutateOnResume); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, stateDir string, crashAfterBatch int64, mutateOnResume bool) error {
	progressDir := filepath.Join(stateDir, "progress")
	outputDir := filepath.Join(stateDir, "output")
	snapshotDir := filepath.Join(stateDir, "snapshots")

	connectors := []datapipe.Connector{
		source.NewSnapshotTable("local-users", "users", snapshotDir, 2, fictionalUsers()),
		source.NewEventLog("local-events", "events", snapshotDir, 2, fictionalEvents()),
	}
	if mutateOnResume {
		if err := mutateSources(progressDir, connectors); err != nil {
			return err
		}
	}

	for _, connector := range connectors {
		runner := datapipe.Runner{
			Connector: connector,
			Progress:  datapipe.NewFileProgressStore(progressDir),
			Output:    newCrashableSink(datapipe.NewJSONLOutputSink(outputDir, connector.Name()), crashAfterBatch),
		}
		checkpoint, err := runner.Run(ctx)
		if err != nil {
			return fmt.Errorf("%s: %w", connector.Name(), err)
		}
		fmt.Printf("%s complete: snapshot=%s batches=%d records=%d\n", connector.Name(), checkpoint.SnapshotID, checkpoint.Batches, checkpoint.Records)
	}
	return nil
}

type crashableSink struct {
	next            datapipe.OutputSink
	crashAfterBatch int64
	batches         int64
}

func newCrashableSink(next datapipe.OutputSink, crashAfterBatch int64) datapipe.OutputSink {
	if crashAfterBatch == 0 {
		return next
	}
	return &crashableSink{next: next, crashAfterBatch: crashAfterBatch}
}

func (s *crashableSink) WriteBatch(ctx context.Context, records []datapipe.Record) error {
	if err := s.next.WriteBatch(ctx, records); err != nil {
		return err
	}
	s.batches++
	if s.batches == s.crashAfterBatch {
		panic(fmt.Sprintf("injected crash: output batch %d is durable, checkpoint is still one batch behind", s.batches))
	}
	return nil
}

func mutateSources(progressDir string, connectors []datapipe.Connector) error {
	users := connectors[0].(*source.SnapshotTable)
	progress := datapipe.NewFileProgressStore(progressDir)
	usersCheckpoint, err := progress.Load(context.Background(), users.Name())
	if err != nil {
		return err
	}
	if usersCheckpoint != nil && !usersCheckpoint.Completed {
		users.Add(source.LocalRecord{
			ID:       "u-added-while-paging",
			Revision: "1",
			Payload:  map[string]any{"name": "Added During Paging"},
		})
		users.Delete("u-3")
	}
	return nil
}

func fictionalUsers() []source.LocalRecord {
	return []source.LocalRecord{
		{ID: "u-1", Revision: "r1", Payload: map[string]any{"name": "Ada", "team": "platform"}},
		{ID: "u-2", Revision: "r1", Payload: map[string]any{"name": "Lin", "team": "data"}},
		{ID: "u-3", Revision: "r2", Payload: map[string]any{"name": "Mina", "team": "data"}},
		{ID: "u-4", Revision: "r1", Payload: map[string]any{"name": "Omar", "team": "billing"}},
		{ID: "u-5", Revision: "r3", Payload: map[string]any{"name": "Yuki", "team": "ops"}},
	}
}

func fictionalEvents() []source.LocalRecord {
	return []source.LocalRecord{
		{ID: "e-01", Revision: "1", Payload: map[string]any{"type": "login", "sequence": 1}},
		{ID: "e-02", Revision: "1", Payload: map[string]any{"type": "click", "sequence": 2}},
		{ID: "e-03", Revision: "1", Payload: map[string]any{"type": "export", "sequence": 3}},
		{ID: "e-04", Revision: "1", Payload: map[string]any{"type": "login", "sequence": 4}},
		{ID: "e-05", Revision: "1", Payload: map[string]any{"type": "click", "sequence": 5}},
	}
}
