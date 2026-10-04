package datalink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
)

type Entity struct {
	ID      string         `json:"id"`
	Version string         `json:"version"`
	Payload map[string]any `json:"payload"`
}

// SnapshotUnavailableError is returned when a checkpoint references a snapshot
// that no longer exists in memory or in the durable snapshot store. The scan
// cannot be silently continued on newer data: batches already written up to
// Token remain valid, but the remaining records of that snapshot are
// unrecoverable. To make progress, clear the checkpoint for this source and
// restart a full scan from a fresh snapshot.
type SnapshotUnavailableError struct {
	SourceID   string
	SnapshotID string
	Token      string
}

func (e *SnapshotUnavailableError) Error() string {
	return fmt.Sprintf(
		"source %q: snapshot %q is no longer available; output up to cursor token %q is valid, but the remaining records of that snapshot cannot be recovered; clear this source's checkpoint to restart a full scan",
		e.SourceID, e.SnapshotID, e.Token)
}

func snapshotUnavailable(sourceID string, resume Cursor) error {
	return &SnapshotUnavailableError{
		SourceID:   sourceID,
		SnapshotID: resume.SnapshotID,
		Token:      resume.Token,
	}
}

// snapshotStore persists snapshot record sets so a scan can resume after the
// process exits and every object is rebuilt. An empty dir disables durability
// and falls back to process-memory snapshots only.
type snapshotStore struct {
	dir string
}

func (s snapshotStore) path(id string) string {
	return filepath.Join(s.dir, "snapshot-"+id+".json")
}

func (s snapshotStore) save(id string, records []Record) error {
	if s.dir == "" {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	data = append(data, 10)
	tmp, err := os.CreateTemp(s.dir, ".snapshot-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
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
	return os.Rename(tmpName, s.path(id))
}

func (s snapshotStore) load(id string) ([]Record, bool, error) {
	if s.dir == "" {
		return nil, false, nil
	}
	data, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var records []Record
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, false, fmt.Errorf("snapshot %q is corrupted: %w", id, err)
	}
	return records, true, nil
}

type EntityMapSource struct {
	mu        sync.Mutex
	sourceID  string
	kind      string
	entities  map[string]Entity
	snapshots map[string][]Record
	// SnapshotDir, when set, durably stores snapshots so a scan resumes after
	// the process exits and all objects are rebuilt with the same directory.
	SnapshotDir string
}

func NewEntityMapSource(sourceID, kind string, entities []Entity) *EntityMapSource {
	source := &EntityMapSource{
		sourceID:  sourceID,
		kind:      kind,
		entities:  make(map[string]Entity, len(entities)),
		snapshots: make(map[string][]Record),
	}
	for _, entity := range entities {
		source.entities[entity.ID] = entity
	}
	return source
}

func (s *EntityMapSource) SourceID() string { return s.sourceID }

func (s *EntityMapSource) Upsert(entity Entity) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entities[entity.ID] = entity
}

func (s *EntityMapSource) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entities, id)
}

func (s *EntityMapSource) OpenSnapshot(_ context.Context, resume Cursor) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if resume.Token != "" || resume.SnapshotID != "" {
		records, ok := s.snapshots[resume.SnapshotID]
		if !ok {
			loaded, found, err := (snapshotStore{dir: s.SnapshotDir}).load(resume.SnapshotID)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, snapshotUnavailable(s.sourceID, resume)
			}
			records = loaded
			s.snapshots[resume.SnapshotID] = records
		}
		return &pageSnapshot{id: resume.SnapshotID, records: records}, nil
	}
	ids := make([]string, 0, len(s.entities))
	for id := range s.entities {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	records := make([]Record, 0, len(ids))
	for _, id := range ids {
		entity := s.entities[id]
		record, err := NewRecord(s.sourceID, s.kind, entity.ID, entity.Version, entity.Payload)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	id := randomSnapshotID(s.sourceID)
	if err := (snapshotStore{dir: s.SnapshotDir}).save(id, records); err != nil {
		return nil, err
	}
	s.snapshots[id] = records
	return &pageSnapshot{id: id, records: records}, nil
}

type AppendLogSource struct {
	mu        sync.Mutex
	sourceID  string
	kind      string
	records   []Record
	snapshots map[string][]Record
	// SnapshotDir, when set, durably stores snapshots so a scan resumes after
	// the process exits and all objects are rebuilt with the same directory.
	SnapshotDir string
}

func NewAppendLogSource(sourceID, kind string) *AppendLogSource {
	return &AppendLogSource{sourceID: sourceID, kind: kind, snapshots: make(map[string][]Record)}
}

func (s *AppendLogSource) SourceID() string { return s.sourceID }

func (s *AppendLogSource) Append(id, version string, payload any) error {
	record, err := NewRecord(s.sourceID, s.kind, id, version, payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, record)
	return nil
}

func (s *AppendLogSource) OpenSnapshot(_ context.Context, resume Cursor) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if resume.Token != "" || resume.SnapshotID != "" {
		records, ok := s.snapshots[resume.SnapshotID]
		if !ok {
			loaded, found, err := (snapshotStore{dir: s.SnapshotDir}).load(resume.SnapshotID)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, snapshotUnavailable(s.sourceID, resume)
			}
			records = loaded
			s.snapshots[resume.SnapshotID] = records
		}
		return &pageSnapshot{id: resume.SnapshotID, records: records}, nil
	}
	id := randomSnapshotID(s.sourceID)
	records := append([]Record(nil), s.records...)
	if err := (snapshotStore{dir: s.SnapshotDir}).save(id, records); err != nil {
		return nil, err
	}
	s.snapshots[id] = records
	return &pageSnapshot{id: id, records: records}, nil
}

type pageSnapshot struct {
	id      string
	records []Record
}

func (s *pageSnapshot) ID() string { return s.id }

func (s *pageSnapshot) Read(_ context.Context, cursor Cursor, limit int) (Page, error) {
	if cursor.SnapshotID != "" && cursor.SnapshotID != s.id {
		return Page{}, fmt.Errorf("cursor belongs to snapshot %q, cannot read it from snapshot %q; an offset is valid only inside its own observation scope", cursor.SnapshotID, s.id)
	}
	offset := 0
	if cursor.Token != "" {
		var err error
		offset, err = strconv.Atoi(cursor.Token)
		if err != nil {
			return Page{}, fmt.Errorf("invalid offset cursor %q: %w", cursor.Token, err)
		}
	}
	if offset < 0 || offset > len(s.records) {
		return Page{}, fmt.Errorf("offset %d is outside snapshot %q", offset, s.id)
	}
	if limit <= 0 {
		return Page{}, fmt.Errorf("limit must be positive")
	}
	end := offset + limit
	if end > len(s.records) {
		end = len(s.records)
	}
	records := append([]Record(nil), s.records[offset:end]...)
	next := Cursor{SnapshotID: s.id, Token: strconv.Itoa(end)}
	return Page{Records: records, Next: next, HasMore: end < len(s.records)}, nil
}

func (s *pageSnapshot) Close() error { return nil }

func randomSnapshotID(prefix string) string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b[:]))
}
