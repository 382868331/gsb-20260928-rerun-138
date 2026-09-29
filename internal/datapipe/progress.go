package datapipe

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type ProgressStore interface {
	Load(ctx context.Context, source string) (*Checkpoint, error)
	Save(ctx context.Context, checkpoint Checkpoint) error
}

type FileProgressStore struct {
	Dir string
}

func NewFileProgressStore(dir string) FileProgressStore {
	return FileProgressStore{Dir: dir}
}

func (s FileProgressStore) path(source string) string {
	return filepath.Join(s.Dir, source+".checkpoint.json")
}

func (s FileProgressStore) Load(_ context.Context, source string) (*Checkpoint, error) {
	data, err := os.ReadFile(s.path(source))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint for %s: %w", source, err)
	}
	var checkpoint Checkpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return nil, fmt.Errorf("decode checkpoint for %s: %w", source, err)
	}
	if checkpoint.Source != source {
		return nil, fmt.Errorf("checkpoint source mismatch: got %q want %q", checkpoint.Source, source)
	}
	return &checkpoint, nil
}

func (s FileProgressStore) Save(_ context.Context, checkpoint Checkpoint) error {
	if checkpoint.Source == "" {
		return fmt.Errorf("save checkpoint: missing source")
	}
	if checkpoint.SnapshotID == "" {
		return fmt.Errorf("save checkpoint for %s: missing snapshot id", checkpoint.Source)
	}
	if err := os.MkdirAll(s.Dir, 0o755); err != nil {
		return fmt.Errorf("create checkpoint directory: %w", err)
	}
	data, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	target := s.path(checkpoint.Source)
	tmp, err := os.CreateTemp(s.Dir, ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary checkpoint: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write checkpoint: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("replace checkpoint: %w", err)
	}
	return nil
}
