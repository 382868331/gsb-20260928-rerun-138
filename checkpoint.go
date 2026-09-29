package datalink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type CheckpointStore interface {
	Load(ctx context.Context) (map[string]Cursor, error)
	Save(ctx context.Context, cursors map[string]Cursor) error
}

type FileCheckpointStore struct {
	Path string
}

func (s FileCheckpointStore) Load(context.Context) (map[string]Cursor, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return map[string]Cursor{}, nil
	}
	if err != nil {
		return nil, err
	}
	var cursors map[string]Cursor
	if len(data) > 0 {
		if err := json.Unmarshal(data, &cursors); err != nil {
			return nil, err
		}
	}
	if cursors == nil {
		cursors = map[string]Cursor{}
	}
	return cursors, nil
}

func (s FileCheckpointStore) Save(_ context.Context, cursors map[string]Cursor) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cursors, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.Path), ".checkpoint-*")
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
	return os.Rename(tmpName, s.Path)
}

type MemoryCheckpointStore struct {
	Data map[string]Cursor
}

func (s *MemoryCheckpointStore) Load(context.Context) (map[string]Cursor, error) {
	if s.Data == nil {
		return map[string]Cursor{}, nil
	}
	copied := make(map[string]Cursor, len(s.Data))
	for k, v := range s.Data {
		copied[k] = v
	}
	return copied, nil
}

func (s *MemoryCheckpointStore) Save(_ context.Context, cursors map[string]Cursor) error {
	copied := make(map[string]Cursor, len(cursors))
	for k, v := range cursors {
		copied[k] = v
	}
	s.Data = copied
	return nil
}

var ErrCheckpointMissing = errors.New("checkpoint cursor is missing")

func checkpointPath(path, suffix string) string {
	if suffix == "" {
		return path
	}
	return fmt.Sprintf("%s.%s", path, suffix)
}
