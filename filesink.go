package datalink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type FileSink struct {
	Dir string
}

func (s FileSink) WriteBatch(_ context.Context, batch Batch) error {
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(batch, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	name := filepath.Join(s.Dir, fmt.Sprintf("batch-%s.json", shortID(batch.BatchID, 32)))
	if existing, err := os.ReadFile(name); err == nil {
		var existingBatch Batch
		if err := json.Unmarshal(existing, &existingBatch); err != nil {
			return err
		}
		if !sameBatchSemantics(existingBatch, batch) {
			return fmt.Errorf("batch id %q reused with different content", batch.BatchID)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	tmp, err := os.CreateTemp(s.Dir, ".batch-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(encoded); err != nil {
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
	return os.Rename(tmpName, name)
}

func shortID(value string, length int) string {
	if len(value) <= length {
		return value
	}
	return value[:length]
}

func deterministicBatchID(batch Batch) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "batch\x00%s\x00%s\x00%d\n", batch.SourceID, batch.StartToken, len(batch.Records))
	if batch.PlanID != "" {
		// Joint export batches are scoped to one plan candidate and
		// observation snapshot, so re-selecting a source's scope under a
		// new candidate never collides with the confirmed old material,
		// even when the record contents are identical.
		_, _ = fmt.Fprintf(h, "joint\x00%s\x00%s\x00%s\n", batch.PlanID, batch.Candidate, batch.Snapshot)
	}
	for _, record := range batch.Records {
		_, _ = fmt.Fprintf(h, "%s\n", record.DataID)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sameBatchSemantics(left, right Batch) bool {
	if left.SourceID != right.SourceID || left.StartToken != right.StartToken || len(left.Records) != len(right.Records) {
		return false
	}
	for i := range left.Records {
		leftJSON, _ := json.Marshal(left.Records[i])
		rightJSON, _ := json.Marshal(right.Records[i])
		if !bytes.Equal(leftJSON, rightJSON) {
			return false
		}
	}
	return true
}
