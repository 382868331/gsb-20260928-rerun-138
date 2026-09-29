package datapipe

import "context"

type Runner struct {
	Connector Connector
	Progress  ProgressStore
	Output    OutputSink
}

// Run transfers pages using at-least-once coordination:
// 1. persist an output batch; 2. advance the cursor.
// A crash between those steps therefore causes an identifiable repeat, never a
// silently skipped identity.
func (r Runner) Run(ctx context.Context) (*Checkpoint, error) {
	saved, err := r.Progress.Load(ctx, r.Connector.Name())
	if err != nil {
		return nil, err
	}
	if saved != nil && saved.Completed {
		return saved, nil
	}

	var scan Scan
	if saved == nil {
		scan, err = r.Connector.Begin(ctx)
	} else {
		scan, err = r.Connector.Resume(ctx, *saved)
	}
	if err != nil {
		return nil, err
	}
	defer scan.Close()

	checkpoint := Checkpoint{
		Source:     r.Connector.Name(),
		SnapshotID: scan.SnapshotID(),
	}
	if saved != nil {
		checkpoint = *saved
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		page, err := scan.Next(ctx)
		if err != nil {
			return nil, err
		}
		if len(page.Records) > 0 {
			if err := r.Output.WriteBatch(ctx, page.Records); err != nil {
				return nil, err
			}
			last := page.Records[len(page.Records)-1].Identity
			checkpoint.LastIdentity = &last
			checkpoint.Records += int64(len(page.Records))
			checkpoint.Batches++
		}
		checkpoint.Cursor = page.Next
		if page.Next == "" {
			checkpoint.Completed = true
		}
		if err := r.Progress.Save(ctx, checkpoint); err != nil {
			return nil, err
		}
		if checkpoint.Completed {
			return &checkpoint, nil
		}
	}
}
