package datalink

import (
	"context"
	"fmt"
)

type Runner struct {
	Connectors  []Connector
	Checkpoints CheckpointStore
	Sink        Sink
	PageSize    int
}

func (r Runner) Run(ctx context.Context) error {
	if r.PageSize <= 0 {
		return fmt.Errorf("PageSize must be positive")
	}
	if r.Checkpoints == nil {
		return fmt.Errorf("Checkpoints is required")
	}
	if r.Sink == nil {
		return fmt.Errorf("Sink is required")
	}
	cursors, err := r.Checkpoints.Load(ctx)
	if err != nil {
		return err
	}
	var sequence uint64
	for _, source := range r.Connectors {
		if source == nil {
			return fmt.Errorf("connector cannot be nil")
		}
		cursor := cursors[source.SourceID()]
		if cursor.Complete {
			continue
		}
		snapshot, err := source.OpenSnapshot(ctx, cursor)
		if err != nil {
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				_ = snapshot.Close()
				return err
			}
			start := cursor
			page, err := snapshot.Read(ctx, cursor, r.PageSize)
			if err != nil {
				_ = snapshot.Close()
				return err
			}
			sequence++
			batch := Batch{
				Sequence:   sequence,
				SourceID:   source.SourceID(),
				Snapshot:   snapshot.ID(),
				StartToken: start.Token,
				Records:    page.Records,
			}
			batch.BatchID = deterministicBatchID(batch)
			if err := r.Sink.WriteBatch(ctx, batch); err != nil {
				_ = snapshot.Close()
				return err
			}
			if page.HasMore {
				cursor = page.Next
			} else {
				cursor = page.Next
				cursor.SnapshotID = snapshot.ID()
				cursor.Complete = true
			}
			cursors[source.SourceID()] = cursor
			if err := r.Checkpoints.Save(ctx, cursors); err != nil {
				_ = snapshot.Close()
				return err
			}
			if !page.HasMore {
				break
			}
		}
		if err := snapshot.Close(); err != nil {
			return err
		}
	}
	return nil
}
