package testsupport

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"datapipe/internal/datapipe"
)

type failAfterBatchesSink struct {
	delegate datapipe.OutputSink
	failAt   int64
	batches  int64
}

func (s *failAfterBatchesSink) WriteBatch(ctx context.Context, records []datapipe.Record) error {
	s.batches++
	if err := s.delegate.WriteBatch(ctx, records); err != nil {
		return err
	}
	if s.batches == s.failAt {
		return fmt.Errorf("simulated crash after durable output batch %d", s.batches)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func RunUntilFailureAndResume(t *testing.T, connector datapipe.Connector, expectedRecords, duplicateBatchSize, recordsBeforeFailure int64) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	progress := datapipe.NewFileProgressStore(filepath.Join(dir, "progress"))
	outputPath := filepath.Join(dir, "output", connector.Name()+".jsonl")
	base := datapipe.NewJSONLOutputSink(filepath.Join(dir, "output"), connector.Name())
	if base.Path != outputPath {
		t.Fatalf("output path = %q, want %q", base.Path, outputPath)
	}

	failing := &failAfterBatchesSink{delegate: base, failAt: 2}
	_, err := datapipe.Runner{Connector: connector, Progress: progress, Output: failing}.Run(ctx)
	if err == nil {
		t.Fatal("expected injected failure")
	}

	checkpoint, err := progress.Load(ctx, connector.Name())
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.Completed {
		t.Fatal("checkpoint must not be complete after failure before save")
	}
	if checkpoint.Records != recordsBeforeFailure {
		t.Fatalf("progress = %d records, want %d before retried batch", checkpoint.Records, recordsBeforeFailure)
	}

	_, err = datapipe.Runner{Connector: connector, Progress: progress, Output: base}.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := datapipe.ReadJSONL(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(raw)) != expectedRecords+int64(duplicateBatchSize) {
		t.Fatalf("raw output = %d records, want %d with one repeated batch", len(raw), expectedRecords+int64(duplicateBatchSize))
	}
	unique := make(map[string]struct{}, len(raw))
	for _, record := range raw {
		unique[record.Identity.Key()] = struct{}{}
	}
	if int64(len(unique)) != expectedRecords {
		t.Fatalf("unique output = %d records, want %d", len(unique), expectedRecords)
	}

	_, err = datapipe.Runner{Connector: connector, Progress: progress, Output: base}.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	completedRaw, err := datapipe.ReadJSONL(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(completedRaw) != len(raw) {
		t.Fatal("completed checkpoint must not replay the source")
	}
}
