package datalink

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Identity is stable for one concrete version of a source record. A change to
// the source entity's version produces a new identity, while a retry of the
// same snapshot page produces exactly the same identity.
type Identity struct {
	Source     string `json:"source"`
	Kind       string `json:"kind"`
	ExternalID string `json:"external_id"`
	Version    string `json:"version"`
}

func (i Identity) DataID() string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s", i.Source, i.Kind, i.ExternalID, i.Version)
	return hex.EncodeToString(h.Sum(nil))
}

type Record struct {
	Identity    Identity        `json:"identity"`
	DataID      string          `json:"data_id"`
	PayloadHash string          `json:"payload_hash"`
	Payload     json.RawMessage `json:"payload"`
}

func NewRecord(source, kind, externalID, version string, payload any) (Record, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Record{}, err
	}
	h := sha256.Sum256(raw)
	identity := Identity{
		Source:     source,
		Kind:       kind,
		ExternalID: externalID,
		Version:    version,
	}
	return Record{
		Identity:    identity,
		DataID:      identity.DataID(),
		PayloadHash: hex.EncodeToString(h[:]),
		Payload:     raw,
	}, nil
}

// Cursor is owned by a connector. The runner must not interpret Token; it only
// persists it and hands it back to the same SourceID. SnapshotID makes the
// meaning of Token explicit. An offset is valid only inside that snapshot.
type Cursor struct {
	SnapshotID string `json:"snapshot_id"`
	Token      string `json:"token"`
	Complete   bool   `json:"complete"`
}

type Page struct {
	Records []Record `json:"records"`
	Next    Cursor   `json:"next"`
	HasMore bool     `json:"has_more"`
}

type Snapshot interface {
	ID() string
	Read(ctx context.Context, cursor Cursor, limit int) (Page, error)
	Close() error
}

// Connector resumes an opaque connector-owned cursor or starts a fresh snapshot
// when cursor is the zero value. New sources implement this interface without
// changing checkpoint schema or the runner.
type Connector interface {
	SourceID() string
	OpenSnapshot(ctx context.Context, resume Cursor) (Snapshot, error)
}

type Batch struct {
	BatchID    string   `json:"batch_id"`
	Sequence   uint64   `json:"sequence"`
	SourceID   string   `json:"source_id"`
	Snapshot   string   `json:"snapshot_id"`
	StartToken string   `json:"start_token"`
	Records    []Record `json:"records"`
}

type Sink interface {
	// WriteBatch must be atomic and idempotent for the same Batch.ID. A retry
	// after a crash may call it again before the corresponding checkpoint is
	// durable.
	WriteBatch(ctx context.Context, batch Batch) error
}
