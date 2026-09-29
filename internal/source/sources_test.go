package source

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"datapipe/internal/datapipe"
	"datapipe/internal/testsupport"
)

func testTableRecords() []localRecord {
	return []localRecord{
		{ID: "u-1", Revision: "r1", Payload: map[string]any{"name": "Ada"}, CreatedAt: "2026-01-01T00:00:00Z"},
		{ID: "u-2", Revision: "r1", Payload: map[string]any{"name": "Lin"}, CreatedAt: "2026-01-01T00:00:00Z"},
		{ID: "u-3", Revision: "r2", Payload: map[string]any{"name": "Mina"}, CreatedAt: "2026-01-01T00:00:00Z"},
		{ID: "u-4", Revision: "r1", Payload: map[string]any{"name": "Omar"}, CreatedAt: "2026-01-01T00:00:00Z"},
		{ID: "u-5", Revision: "r3", Payload: map[string]any{"name": "Yuki"}, CreatedAt: "2026-01-01T00:00:00Z"},
	}
}

func testEventRecords(count int) []localRecord {
	records := make([]localRecord, count)
	for i := range records {
		records[i] = localRecord{
			ID:        fmt.Sprintf("e-%02d", i+1),
			Revision:  "1",
			Payload:   map[string]any{"sequence": i + 1},
			CreatedAt: "2026-01-01T00:00:00Z",
		}
	}
	return records
}

func TestSnapshotTableIgnoresChangesAfterBeginAcrossCrash(t *testing.T) {
	table := NewSnapshotTable("local-users", "users", "", 2, testTableRecords())
	changingConnector := &changeAfterFirstFailure{
		connector: table,
		change: func() {
			table.Delete("u-3")
			table.Add(localRecord{ID: "u-6", Revision: "1", Payload: map[string]any{"name": "New"}})
		},
	}
	testsupport.RunUntilFailureAndResume(t, changingConnector, 5, 2, 2)

	scan, err := table.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if scan.SnapshotID() == changingConnector.firstSnapshot {
		t.Fatal("later run must use a fresh snapshot and see current source state")
	}
}

func TestEventLogHighWaterIgnoresAppendsAndDeletesAcrossCrash(t *testing.T) {
	log := NewEventLog("local-events", "events", "", 2, testEventRecords(5))
	changingConnector := &changeAfterFirstFailure{
		connector: log,
		change: func() {
			log.Append(localRecord{ID: "e-06", Revision: "1", Payload: map[string]any{"sequence": 6}})
			log.DeleteUpTo(2)
		},
	}
	testsupport.RunUntilFailureAndResume(t, changingConnector, 5, 2, 2)
}

func TestIdentitiesAreSourceEntityID(t *testing.T) {
	table := NewSnapshotTable("local-users", "users", "", 10, testTableRecords())
	scan, err := table.Begin(context.Background())
	if err != nil {
		panic(err)
	}
	page, err := scan.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := datapipe.Identity{Source: "local-users", Entity: "users", ID: "u-1"}
	if got := page.Records[0].Identity; got != want {
		t.Fatalf("identity = %+v, want %+v", got, want)
	}
}

func TestEventCursorEncodesHighWater(t *testing.T) {
	log := NewEventLog("local-events", "events", "", 2, testEventRecords(5))
	scan, err := log.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	page, err := scan.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if page.Next == "" || page.Next == "2" {
		t.Fatalf("cursor must be source-owned and opaque, got %q", page.Next)
	}
}

type changeAfterFirstFailure struct {
	connector     datapipe.Connector
	change        func()
	firstSnapshot string
	changed       bool
}

func (c *changeAfterFirstFailure) Name() string { return c.connector.Name() }

func (c *changeAfterFirstFailure) Begin(ctx context.Context) (datapipe.Scan, error) {
	scan, err := c.connector.Begin(ctx)
	if err != nil {
		return nil, err
	}
	c.firstSnapshot = scan.SnapshotID()
	return &changeScan{Scan: scan}, nil
}

func (c *changeAfterFirstFailure) Resume(ctx context.Context, checkpoint datapipe.Checkpoint) (datapipe.Scan, error) {
	if !c.changed {
		c.changed = true
		c.change()
	}
	return c.connector.Resume(ctx, checkpoint)
}

type changeScan struct {
	datapipe.Scan
}

func TestDifferentSourceStatesHaveDifferentSnapshots(t *testing.T) {
	table := NewSnapshotTable("x", "y", "", 2, testTableRecords())
	a, err := table.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	table.Add(localRecord{ID: "different", Revision: "1"})
	b, err := table.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.SnapshotID() == b.SnapshotID() {
		t.Fatal("different source states should produce different snapshot ids")
	}
}

func TestSnapshotsCanResumeFromDiskAfterProcessRestart(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first := NewSnapshotTable("restart-users", "users", filepath.Join(dir, "snapshots"), 2, testTableRecords())
	scan, err := first.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	page, err := scan.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := datapipe.Checkpoint{
		Source:     first.Name(),
		SnapshotID: scan.SnapshotID(),
		Cursor:     page.Next,
		Records:    int64(len(page.Records)),
	}

	second := NewSnapshotTable("restart-users", "users", filepath.Join(dir, "snapshots"), 2, testTableRecords())
	second.Delete("u-1")
	second.Delete("u-2")
	second.Add(localRecord{ID: "new", Revision: "1"})
	resumed, err := second.Resume(ctx, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	restPage, err := resumed.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := restPage.Records[0].Identity.ID; got != "u-3" {
		t.Fatalf("first resumed id = %q, want snapshot id u-3", got)
	}
}

func TestSortedRecordIDs(t *testing.T) {
	records := []datapipe.Record{
		{Identity: datapipe.Identity{Source: "local-users", Entity: "users", ID: "b"}},
		{Identity: datapipe.Identity{Source: "local-users", Entity: "users", ID: "a"}},
	}
	if got := SortedRecordIDs(records); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("ids = %v", got)
	}
}
