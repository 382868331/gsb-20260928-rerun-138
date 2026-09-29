package source

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"datapipe/internal/datapipe"
)

type localRecord struct {
	ID        string         `json:"id"`
	Revision  string         `json:"revision"`
	Payload   map[string]any `json:"payload"`
	CreatedAt string         `json:"created_at"`
}

// LocalRecord is the fictional fixture type used by demo programs and tests.
type LocalRecord = localRecord

type persistedEventSnapshot struct {
	Events    []localRecord `json:"events"`
	HighWater int64         `json:"high_water"`
}

func marshalRecord(source, entity string, item localRecord) (datapipe.Record, error) {
	payload, err := json.Marshal(item.Payload)
	if err != nil {
		return datapipe.Record{}, fmt.Errorf("encode payload for %s: %w", item.ID, err)
	}
	return datapipe.Record{
		Identity: datapipe.Identity{
			Source: source,
			Entity: entity,
			ID:     item.ID,
		},
		Revision:  item.Revision,
		Payload:   payload,
		CreatedAt: item.CreatedAt,
	}, nil
}

func snapshotHash(records []localRecord) string {
	ids := make([]string, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID+"@"+record.Revision)
	}
	sum := sha256.Sum256([]byte(fmt.Sprint(ids)))
	return hex.EncodeToString(sum[:12])
}

func encodeCursor(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(data), nil
}

func decodeCursor[T any](encoded string, value *T) error {
	if encoded == "" {
		return nil
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decode cursor: %w", err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode cursor: %w", err)
	}
	return nil
}

func writeSnapshotFile(dir, source, snapshotID string, value any) error {
	if dir == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Join(dir, source), 0o755); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode source snapshot: %w", err)
	}
	target := filepath.Join(dir, source, snapshotID+".json")
	tmp, err := os.CreateTemp(filepath.Join(dir, source), ".snapshot-*.tmp")
	if err != nil {
		return fmt.Errorf("create source snapshot: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()
		return fmt.Errorf("write source snapshot: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync source snapshot: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close source snapshot: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("replace source snapshot: %w", err)
	}
	return nil
}

func readSnapshotFile[T any](dir, source, snapshotID string) (T, error) {
	var zero T
	target := filepath.Join(dir, source, snapshotID+".json")
	data, err := os.ReadFile(target)
	if err != nil {
		return zero, fmt.Errorf("read source snapshot %s: %w", snapshotID, err)
	}
	var value T
	if err := json.Unmarshal(data, &value); err != nil {
		return zero, fmt.Errorf("decode source snapshot %s: %w", snapshotID, err)
	}
	return value, nil
}

// SnapshotTable is a fictional local key/value source. Begin copies the current
// table; later adds and deletes cannot alter the ordered snapshot being read.
type SnapshotTable struct {
	mu        sync.RWMutex
	name      string
	entity    string
	pageSize  int
	records   map[string]localRecord
	order     []string
	snapshots map[string][]localRecord
	dir       string
}

func NewSnapshotTable(name, entity, snapshotDir string, pageSize int, initial []localRecord) *SnapshotTable {
	table := &SnapshotTable{
		name:      name,
		entity:    entity,
		pageSize:  pageSize,
		records:   make(map[string]localRecord),
		snapshots: make(map[string][]localRecord),
		dir:       snapshotDir,
	}
	for _, record := range initial {
		table.records[record.ID] = record
		table.order = append(table.order, record.ID)
	}
	return table
}

func (t *SnapshotTable) Add(record localRecord) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.records[record.ID]; !exists {
		t.order = append(t.order, record.ID)
	}
	t.records[record.ID] = record
}

func (t *SnapshotTable) Delete(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.records, id)
	nextOrder := t.order[:0]
	for _, existingID := range t.order {
		if existingID != id {
			nextOrder = append(nextOrder, existingID)
		}
	}
	t.order = nextOrder
}

func (t *SnapshotTable) Name() string { return t.name }

func (t *SnapshotTable) Begin(context.Context) (datapipe.Scan, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	records := make([]localRecord, 0, len(t.order))
	for _, id := range t.order {
		if record, ok := t.records[id]; ok {
			records = append(records, record)
		}
	}
	snapshotID := snapshotHash(records)
	t.snapshots[snapshotID] = records
	if err := writeSnapshotFile(t.dir, t.name, snapshotID, records); err != nil {
		return nil, err
	}
	return &tableScan{table: t, snapshotID: snapshotID, offset: 0}, nil
}

func (t *SnapshotTable) Resume(_ context.Context, checkpoint datapipe.Checkpoint) (datapipe.Scan, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	records, ok := t.snapshots[checkpoint.SnapshotID]
	if !ok && t.dir != "" {
		var err error
		records, err = readSnapshotFile[[]localRecord](t.dir, t.name, checkpoint.SnapshotID)
		if err != nil {
			return nil, err
		}
		t.snapshots[checkpoint.SnapshotID] = records
		ok = true
	}
	if !ok {
		return nil, fmt.Errorf("snapshot %q is no longer available; restart requires a new source snapshot", checkpoint.SnapshotID)
	}
	var cursor struct {
		Offset int `json:"offset"`
	}
	if err := decodeCursor(checkpoint.Cursor, &cursor); err != nil {
		return nil, err
	}
	return &tableScan{table: t, snapshotID: checkpoint.SnapshotID, offset: cursor.Offset}, nil
}

type tableScan struct {
	table      *SnapshotTable
	snapshotID string
	offset     int
}

func (s *tableScan) SnapshotID() string { return s.snapshotID }

func (s *tableScan) Next(context.Context) (datapipe.Page, error) {
	s.table.mu.RLock()
	records := s.table.snapshots[s.snapshotID]
	if s.offset > len(records) {
		s.table.mu.RUnlock()
		return datapipe.Page{}, errors.New("snapshot cursor is past the end")
	}
	end := min(s.offset+s.table.pageSize, len(records))
	pageRecords := append([]localRecord(nil), records[s.offset:end]...)
	s.table.mu.RUnlock()

	out := make([]datapipe.Record, 0, len(pageRecords))
	for _, item := range pageRecords {
		record, err := marshalRecord(s.table.name, s.table.entity, item)
		if err != nil {
			return datapipe.Page{}, err
		}
		out = append(out, record)
	}
	s.offset = end
	var next string
	if end < len(records) {
		var err error
		next, err = encodeCursor(struct {
			Offset int `json:"offset"`
		}{Offset: end})
		if err != nil {
			return datapipe.Page{}, err
		}
	}
	return datapipe.Page{Records: out, Next: next}, nil
}

func (s *tableScan) Close() error { return nil }

// EventLog is a fictional append-only source. The snapshot fixes a high-water
// sequence. New events after Begin wait for the next run; deletion tombstones
// cannot erase already committed event identities.
type EventLog struct {
	mu        sync.Mutex
	name      string
	entity    string
	pageSize  int
	events    []localRecord
	snapshots map[string]eventSnapshot
	dir       string
}

type eventSnapshot struct {
	Events    []localRecord
	HighWater int64
}

func NewEventLog(name, entity, snapshotDir string, pageSize int, initial []localRecord) *EventLog {
	return &EventLog{
		name:      name,
		entity:    entity,
		pageSize:  pageSize,
		events:    append([]localRecord(nil), initial...),
		snapshots: make(map[string]eventSnapshot),
		dir:       snapshotDir,
	}
}

func (l *EventLog) Append(record localRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, record)
}

func (l *EventLog) DeleteUpTo(sequence int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if int(sequence) > len(l.events) {
		sequence = int64(len(l.events))
	}
	l.events = append([]localRecord(nil), l.events[sequence:]...)
}

func (l *EventLog) Name() string { return l.name }

func (l *EventLog) Begin(context.Context) (datapipe.Scan, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	events := append([]localRecord(nil), l.events...)
	highWater := int64(len(events))
	id := snapshotHash(events)
	snapshot := eventSnapshot{Events: events, HighWater: highWater}
	l.snapshots[id] = snapshot
	persisted := persistedEventSnapshot{Events: events, HighWater: highWater}
	if err := writeSnapshotFile(l.dir, l.name, id, persisted); err != nil {
		return nil, err
	}
	return &eventScan{log: l, snapshotID: id, afterSeq: 0, highWater: highWater}, nil
}

func (l *EventLog) Resume(_ context.Context, checkpoint datapipe.Checkpoint) (datapipe.Scan, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	snapshot, ok := l.snapshots[checkpoint.SnapshotID]
	if !ok && l.dir != "" {
		var err error
		persisted, readErr := readSnapshotFile[persistedEventSnapshot](l.dir, l.name, checkpoint.SnapshotID)
		err = readErr
		if err != nil {
			return nil, err
		}
		snapshot = eventSnapshot{Events: persisted.Events, HighWater: persisted.HighWater}
		l.snapshots[checkpoint.SnapshotID] = snapshot
		ok = true
	}
	if !ok {
		return nil, fmt.Errorf("event snapshot %q expired; resume requires retaining the source snapshot", checkpoint.SnapshotID)
	}
	var cursor struct {
		AfterSeq  int64 `json:"after_seq"`
		HighWater int64 `json:"high_water"`
	}
	if err := decodeCursor(checkpoint.Cursor, &cursor); err != nil {
		return nil, err
	}
	if cursor.HighWater != 0 && cursor.HighWater != snapshot.HighWater {
		return nil, fmt.Errorf("event cursor high water %d does not match snapshot %d", cursor.HighWater, snapshot.HighWater)
	}
	return &eventScan{log: l, snapshotID: checkpoint.SnapshotID, afterSeq: cursor.AfterSeq, highWater: snapshot.HighWater}, nil
}

type eventScan struct {
	log        *EventLog
	snapshotID string
	afterSeq   int64
	highWater  int64
}

func (s *eventScan) SnapshotID() string { return s.snapshotID }

func (s *eventScan) Next(context.Context) (datapipe.Page, error) {
	s.log.mu.Lock()
	snapshot := s.log.snapshots[s.snapshotID]
	s.log.mu.Unlock()
	if s.afterSeq > s.highWater {
		return datapipe.Page{}, errors.New("event cursor is past high water")
	}
	end := min(s.afterSeq+int64(s.log.pageSize), s.highWater)
	out := make([]datapipe.Record, 0, end-s.afterSeq)
	for seq := s.afterSeq; seq < end; seq++ {
		record, err := marshalRecord(s.log.name, s.log.entity, snapshot.Events[seq])
		if err != nil {
			return datapipe.Page{}, err
		}
		out = append(out, record)
	}
	s.afterSeq = end
	var next string
	if end < s.highWater {
		var err error
		next, err = encodeCursor(struct {
			AfterSeq  int64 `json:"after_seq"`
			HighWater int64 `json:"high_water"`
		}{AfterSeq: end, HighWater: s.highWater})
		if err != nil {
			return datapipe.Page{}, err
		}
	}
	return datapipe.Page{Records: out, Next: next}, nil
}

func (s *eventScan) Close() error { return nil }

func SortedRecordIDs(records []datapipe.Record) []string {
	ids := make([]string, len(records))
	for i, record := range records {
		ids[i] = record.Identity.ID
	}
	sort.Strings(ids)
	return ids
}
