package datalink

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// SnapshotStore persists snapshot record sets so a scan can resume with an old
// cursor after every object is recreated (process restart). Without it,
// snapshots live only in process memory and a resume cursor is meaningless.
type SnapshotStore struct {
	Dir string
}

func (s SnapshotStore) path(sourceID, snapshotID string) string {
	return filepath.Join(s.Dir, sourceID, snapshotID+".json")
}

func (s SnapshotStore) Save(sourceID, snapshotID string, records []Record) error {
	if s.Dir == "" {
		return nil
	}
	path := s.path(sourceID, snapshotID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(append(data, '\n')); err != nil {
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
	return os.Rename(tmpName, path)
}

func (s SnapshotStore) Load(sourceID, snapshotID string) ([]Record, error) {
	if s.Dir == "" {
		return nil, os.ErrNotExist
	}
	data, err := os.ReadFile(s.path(sourceID, snapshotID))
	if err != nil {
		return nil, err
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("snapshot %q of source %q is corrupted: %w", snapshotID, sourceID, err)
	}
	return records, nil
}

// SnapshotExpiredError reports that a resume cursor can no longer be honored
// because its snapshot is gone from memory and durable storage. It states the
// recoverable scope explicitly: records up to CompletedToken were already
// emitted from the lost snapshot, and the remaining records cannot be
// recovered by resuming; the source must be rescanned from the start. The
// runner propagates this error instead of silently skipping the rest.
type SnapshotExpiredError struct {
	SourceID       string
	SnapshotID     string
	CompletedToken string
}

func (e *SnapshotExpiredError) Error() string {
	return fmt.Sprintf(
		"source %q snapshot %q expired: records before offset %q were already emitted, "+
			"the remaining records of that scan cannot be resumed; restart the scan for this source",
		e.SourceID, e.SnapshotID, e.CompletedToken)
}

func IsSnapshotExpired(err error) bool {
	var expired *SnapshotExpiredError
	return errors.As(err, &expired)
}
