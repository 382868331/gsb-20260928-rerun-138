package datapipe

import "context"

// Checkpoint is the only state required to resume a source. The runner and
// progress store never interpret Cursor; its format belongs to the connector.
type Checkpoint struct {
	Source       string    `json:"source"`
	SnapshotID   string    `json:"snapshot_id"`
	Cursor       string    `json:"cursor,omitempty"`
	LastIdentity *Identity `json:"last_identity,omitempty"`
	Batches      int64     `json:"batches"`
	Records      int64     `json:"records"`
	Completed    bool      `json:"completed"`
}

type Scan interface {
	SnapshotID() string
	// Next returns one connector-defined page. An empty next cursor ends scan.
	Next(ctx context.Context) (Page, error)
	Close() error
}

type Connector interface {
	Name() string
	// Begin opens a new immutable snapshot. Resume continues an existing one.
	// Ordinary mutable page numbers are not part of this interface.
	Begin(ctx context.Context) (Scan, error)
	Resume(ctx context.Context, checkpoint Checkpoint) (Scan, error)
}
