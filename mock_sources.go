package datalink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Entity struct {
	ID      string         `json:"id"`
	Version string         `json:"version"`
	Payload map[string]any `json:"payload"`
}

type EntityMapSource struct {
	mu        sync.Mutex
	sourceID  string
	kind      string
	entities  map[string]Entity
	snapshots map[string][]Record
	store     SnapshotStore
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

// UseSnapshotStore enables durable snapshot persistence so a scan can resume
// with an old cursor after this process exits and all objects are rebuilt.
func (s *EntityMapSource) UseSnapshotStore(store SnapshotStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

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
		records, err := s.loadSnapshotLocked(resume)
		if err != nil {
			return nil, err
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
	s.snapshots[id] = records
	if err := s.store.Save(s.sourceID, id, records); err != nil {
		return nil, err
	}
	return &pageSnapshot{id: id, records: records}, nil
}

func (s *EntityMapSource) loadSnapshotLocked(resume Cursor) ([]Record, error) {
	if records, ok := s.snapshots[resume.SnapshotID]; ok {
		return records, nil
	}
	records, err := s.store.Load(s.sourceID, resume.SnapshotID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &SnapshotExpiredError{
				SourceID:       s.sourceID,
				SnapshotID:     resume.SnapshotID,
				CompletedToken: resume.Token,
			}
		}
		return nil, err
	}
	s.snapshots[resume.SnapshotID] = records
	return records, nil
}

type AppendLogSource struct {
	mu        sync.Mutex
	sourceID  string
	kind      string
	dataFile  string
	records   []Record
	snapshots map[string][]Record
	store     SnapshotStore
}

func NewAppendLogSource(sourceID, kind string) *AppendLogSource {
	return &AppendLogSource{sourceID: sourceID, kind: kind, snapshots: make(map[string][]Record)}
}

func (s *AppendLogSource) SourceID() string { return s.sourceID }

// UseDataFile backs the log with a durable file so appended records survive a
// real process exit; a rebuilt source object reads them back on the next scan.
func (s *AppendLogSource) UseDataFile(path string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dataFile = path
}

// UseSnapshotStore enables durable snapshot persistence so a scan can resume
// with an old cursor after this process exits and all objects are rebuilt.
func (s *AppendLogSource) UseSnapshotStore(store SnapshotStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store = store
}

func (s *AppendLogSource) Append(id, version string, payload any) error {
	record, err := NewRecord(s.sourceID, s.kind, id, version, payload)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dataFile != "" {
		line, err := json.Marshal(record)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(s.dataFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		if _, err := f.Write(append(line, '\n')); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
	}
	s.records = append(s.records, record)
	return nil
}

func (s *AppendLogSource) OpenSnapshot(_ context.Context, resume Cursor) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if resume.Token != "" || resume.SnapshotID != "" {
		records, err := s.loadSnapshotLocked(resume)
		if err != nil {
			return nil, err
		}
		return &pageSnapshot{id: resume.SnapshotID, records: records}, nil
	}
	id := randomSnapshotID(s.sourceID)
	records := append([]Record(nil), s.records...)
	if s.dataFile != "" {
		loaded, err := readRecordLines(s.dataFile)
		if err != nil {
			return nil, err
		}
		records = loaded
		s.records = append([]Record(nil), loaded...)
	}
	s.snapshots[id] = records
	if err := s.store.Save(s.sourceID, id, records); err != nil {
		return nil, err
	}
	return &pageSnapshot{id: id, records: records}, nil
}

func readRecordLines(path string) ([]Record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var records []Record
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var record Record
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("append log %q is corrupted: %w", path, err)
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *AppendLogSource) loadSnapshotLocked(resume Cursor) ([]Record, error) {
	if records, ok := s.snapshots[resume.SnapshotID]; ok {
		return records, nil
	}
	records, err := s.store.Load(s.sourceID, resume.SnapshotID)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, &SnapshotExpiredError{
				SourceID:       s.sourceID,
				SnapshotID:     resume.SnapshotID,
				CompletedToken: resume.Token,
			}
		}
		return nil, err
	}
	s.snapshots[resume.SnapshotID] = records
	return records, nil
}

type pageSnapshot struct {
	id      string
	records []Record
}

func (s *pageSnapshot) ID() string { return s.id }

func (s *pageSnapshot) Read(_ context.Context, cursor Cursor, limit int) (Page, error) {
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
