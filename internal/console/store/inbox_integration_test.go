//go:build integration

package store_test

// TestInboxStore* require a real PostgreSQL. They pin the transactional outbox contract:
// dedup at the receipt gate, in-order projection, dead-letter without head-of-line blocking,
// cross-replica skip-locked claiming, checkpoints and the retention sweep.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/EsDmitrii/kconmon-ng/api/proto"
	"github.com/EsDmitrii/kconmon-ng/internal/console/store"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// newInboxDB opens a migrated store and hands back its InboxStore + EventStore pair.
func newInboxDB(t *testing.T) (store.InboxStore, store.EventStore) {
	t.Helper()
	db := newEventStoreDB(t)
	m := newTestMetrics()
	return store.NewInboxStore(db, m), store.NewEventStore(db, m)
}

func eventReceipt(t *testing.T, epoch string, seq int64, id string) store.InboxReceipt {
	t.Helper()
	ts := time.Date(2026, 3, 4, 5, 6, int(seq%60), 0, time.UTC)
	ev := &pb.Event{
		EventId:   id,
		Seq:       uint64(seq),
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Event_TopologyChanged{
			TopologyChanged: &pb.TopologyChanged{Reason: "agent_registered", NodeName: fmt.Sprintf("node-%d", seq)},
		},
	}
	raw, err := proto.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return store.InboxReceipt{EventID: id, Epoch: epoch, Seq: seq, EventTime: ts, Raw: raw}
}

// projectRaw mirrors the cmd/console adapter: raw pb.Event → the durable record.
func projectRaw(kind string, raw []byte, pre store.EventRecord) (store.EventRecord, error) {
	if kind == store.InboxKindBaseline {
		return pre, nil
	}
	var ev pb.Event
	if err := proto.Unmarshal(raw, &ev); err != nil {
		return store.EventRecord{}, fmt.Errorf("unmarshal event: %w: %w", store.ErrPermanentProjection, err)
	}
	if ev.GetPayload() == nil {
		return store.EventRecord{}, fmt.Errorf("%w: no payload", store.ErrPermanentProjection)
	}
	return store.EventRecord{
		EventSeq:  int64(ev.GetSeq()),
		EventTime: ev.GetTimestamp().AsTime(),
		Type:      "topology_changed",
		Severity:  "info",
		Scope:     "cluster",
		Summary:   fmt.Sprintf("event %d", ev.GetSeq()),
		Details:   json.RawMessage(`{}`),
	}, nil
}

func TestInboxReceiveIsTheDedupGate(t *testing.T) {
	inbox, _ := newInboxDB(t)
	ctx := context.Background()
	r := eventReceipt(t, "ep-1", 1, "evt-1")
	inserted, err := inbox.ReceiveEvent(ctx, r)
	if err != nil || !inserted {
		t.Fatalf("first receipt = (%v,%v), want (true,nil)", inserted, err)
	}
	again, err := inbox.ReceiveEvent(ctx, r)
	if err != nil || again {
		t.Fatalf("duplicate receipt = (%v,%v), want (false,nil)", again, err)
	}
}

func TestInboxProjectPendingWritesHistoryAndIsIdempotent(t *testing.T) {
	inbox, events := newInboxDB(t)
	ctx := context.Background()
	r := eventReceipt(t, "ep-1", 1, "evt-1")
	if _, err := inbox.ReceiveEvent(ctx, r); err != nil {
		t.Fatal(err)
	}

	projected, dead, err := inbox.ProjectPending(ctx, 10, projectRaw)
	if err != nil || len(dead) != 0 || len(projected) != 1 {
		t.Fatalf("ProjectPending = (%+v,%+v,%v)", projected, dead, err)
	}
	if !projected[0].HistoryInserted {
		t.Error("HistoryInserted=false for the first projection of a unique event")
	}

	// A second pass has nothing to do: projected rows never re-drive anything.
	again, dead, err := inbox.ProjectPending(ctx, 10, projectRaw)
	if err != nil || len(dead) != 0 || len(again) != 0 {
		t.Fatalf("second pass = (%+v,%+v,%v), want empty", again, dead, err)
	}

	page, err := events.ListEvents(ctx, store.EventFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].EventSeq != 1 {
		t.Fatalf("history page = %+v, want the one projected event", page.Events)
	}

	cp, err := inbox.Checkpoint(ctx, "ep-1")
	if err != nil || cp != 1 {
		t.Fatalf("checkpoint = %d/%v, want 1", cp, err)
	}
}

// TestInboxDeadLetterDoesNotBlockHead pins the terminal-state contract: an event that can never
// project is marked dead and later events keep flowing past it in seq order.
func TestInboxDeadLetterDoesNotBlockHead(t *testing.T) {
	inbox, events := newInboxDB(t)
	ctx := context.Background()

	good1 := eventReceipt(t, "ep-1", 1, "evt-good-1")
	bad := store.InboxReceipt{EventID: "evt-bad", Epoch: "ep-1", Seq: 2,
		EventTime: time.Date(2026, 3, 4, 5, 6, 2, 0, time.UTC), Raw: []byte("not a protobuf")}
	good2 := eventReceipt(t, "ep-1", 3, "evt-good-2")
	for _, r := range []store.InboxReceipt{good1, bad, good2} {
		if _, err := inbox.ReceiveEvent(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	projected, dead, err := inbox.ProjectPending(ctx, 10, projectRaw)
	if err != nil {
		t.Fatalf("ProjectPending: %v", err)
	}
	if len(dead) != 1 || dead[0].EventID != "evt-bad" {
		t.Fatalf("dead = %+v, want the one bad receipt", dead)
	}
	if len(projected) != 2 || projected[0].Seq != 1 || projected[1].Seq != 3 {
		t.Fatalf("projected = %+v, want seqs [1 3] in order", projected)
	}

	received, deadCount, err := inbox.InboxCounts(ctx)
	if err != nil || received != 0 || deadCount != 1 {
		t.Fatalf("counts = (%d,%d,%v)", received, deadCount, err)
	}
	page, _ := events.ListEvents(ctx, store.EventFilter{Limit: 10})
	if len(page.Events) != 2 {
		t.Fatalf("history must hold only the projectable events, got %d rows", len(page.Events))
	}
	// A dead event still occupies its seq in the checkpoint.
	if cp, _ := inbox.Checkpoint(ctx, "ep-1"); cp != 3 {
		t.Fatalf("checkpoint = %d, want 3", cp)
	}
}

// TestInboxTransientProjectionFailurePreservesOrder rolls the whole batch back on a non-fatal
// projection error: nothing may become visible as history, and retry re-enters at the head row.
func TestInboxTransientProjectionFailurePreservesOrder(t *testing.T) {
	inbox, events := newInboxDB(t)
	ctx := context.Background()
	for seq := int64(1); seq <= 3; seq++ {
		r := eventReceipt(t, "ep-1", seq, fmt.Sprintf("evt-%d", seq))
		if _, err := inbox.ReceiveEvent(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	transient := errors.New("database hiccup")
	failSeq2Once := func(kind string, raw []byte, pre store.EventRecord) (store.EventRecord, error) {
		var ev pb.Event
		if err := proto.Unmarshal(raw, &ev); err != nil {
			t.Errorf("unmarshal: %v", err)
			return store.EventRecord{}, err
		}
		if ev.GetSeq() == 2 {
			return store.EventRecord{}, transient
		}
		return projectRaw(kind, raw, pre)
	}

	if _, _, err := inbox.ProjectPending(ctx, 10, failSeq2Once); !errors.Is(err, transient) {
		t.Fatalf("want transient error wrapped, got %v", err)
	}
	page, _ := events.ListEvents(ctx, store.EventFilter{Limit: 10})
	if len(page.Events) != 0 {
		t.Fatalf("rolled-back batch must leave no history, got %d rows", len(page.Events))
	}

	projected, _, err := inbox.ProjectPending(ctx, 10, projectRaw)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if len(projected) != 3 || projected[0].Seq != 1 || projected[2].Seq != 3 {
		t.Fatalf("retry projected = %+v, want seqs [1 2 3]", projected)
	}
}

// TestInboxConcurrentProjectionsClaimDisjointBatches pins FOR UPDATE SKIP LOCKED: while one
// projection pass holds the claimed rows open, a second pass on another connection claims none,
// and after both finish every row is projected with exactly one history row.
func TestInboxConcurrentProjectionsClaimDisjointBatches(t *testing.T) {
	db := newEventStoreDB(t)
	m := newTestMetrics()
	inboxA := store.NewInboxStore(db, m)
	inboxB := store.NewInboxStore(db, m)
	ctx := context.Background()

	for seq := int64(1); seq <= 5; seq++ {
		r := eventReceipt(t, "ep-1", seq, fmt.Sprintf("evt-%d", seq))
		if _, err := inboxA.ReceiveEvent(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	hold := make(chan struct{})
	entered := make(chan struct{})
	releaseSecond := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	var firstCount int
	go func() {
		defer wg.Done()
		first, _, err := inboxA.ProjectPending(ctx, 10, func(kind string, raw []byte, pre store.EventRecord) (store.EventRecord, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-hold
			return projectRaw(kind, raw, pre)
		})
		if err != nil {
			t.Errorf("first projection: %v", err)
		}
		firstCount = len(first)
	}()

	go func() {
		defer wg.Done()
		<-entered
		second, dead, err := inboxB.ProjectPending(ctx, 10, projectRaw)
		if err != nil || len(dead) != 0 || len(second) != 0 {
			t.Errorf("locked-out projection = (%+v,%+v,%v), want empty", second, dead, err)
		}
		close(releaseSecond)
	}()

	select {
	case <-releaseSecond:
	case <-time.After(5 * time.Second):
		t.Fatal("second projection did not return while rows were locked")
	}
	close(hold)
	wg.Wait()
	if firstCount != 5 {
		t.Fatalf("first pass projected %d rows, want 5", firstCount)
	}

	// A subsequent pass finds everything projected: no double history.
	leftover, _, err := inboxA.ProjectPending(ctx, 10, projectRaw)
	if err != nil || len(leftover) != 0 {
		t.Fatalf("post-concurrency pass = (%+v,%v)", leftover, err)
	}
}

// TestInboxBaselineReceiptProjectsFoldSeed verifies synthesized baselines take the same
// receive → project path but land as topology_baseline rows (excluded from the events listing).
func TestInboxBaselineReceiptProjectsFoldSeed(t *testing.T) {
	inbox, events := newInboxDB(t)
	ctx := context.Background()
	ts := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	b := store.InboxBaseline{
		EventID:   "baseline:ep-1:" + fmt.Sprint(ts.UnixNano()),
		Epoch:     "ep-1",
		EventTime: ts,
		Record: store.EventRecord{
			EventSeq: 0, EventTime: ts, Type: "topology_baseline", Severity: "info",
			Scope: "cluster", Summary: "baseline", Details: json.RawMessage(`{"nodes":[],"agents":[]}`),
		},
	}
	if inserted, err := inbox.ReceiveBaseline(ctx, b); err != nil || !inserted {
		t.Fatalf("ReceiveBaseline = (%v,%v)", inserted, err)
	}
	projected, _, err := inbox.ProjectPending(ctx, 10, projectRaw)
	if err != nil || len(projected) != 1 || projected[0].Kind != store.InboxKindBaseline {
		t.Fatalf("baseline projection = %+v, err=%v", projected, err)
	}
	page, err := events.ListEvents(ctx, store.EventFilter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 0 {
		t.Fatalf("baselines must never be listed as events, got %d rows", len(page.Events))
	}
	snap, err := events.TopologyAt(ctx, ts.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if snap.LastChange.IsZero() {
		t.Fatal("the fold must start from the projected baseline")
	}
}

// TestInboxCheckpointIsScopedPerEpoch ensures a reconnect asks for the cursor of THE incarnation
// named in the header, never another era's numbering.
func TestInboxCheckpointIsScopedPerEpoch(t *testing.T) {
	inbox, _ := newInboxDB(t)
	ctx := context.Background()
	for _, r := range []struct {
		epoch string
		seq   int64
		id    string
	}{
		{"ep-a", 1, "a1"}, {"ep-a", 2, "a2"}, {"ep-b", 1, "b1"},
	} {
		rec := eventReceipt(t, r.epoch, r.seq, r.id)
		if _, err := inbox.ReceiveEvent(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		epoch string
		want  uint64
	}{
		{"ep-a", 2}, {"ep-b", 1}, {"ep-unknown", 0},
	} {
		if got, _ := inbox.Checkpoint(ctx, tc.epoch); got != tc.want {
			t.Errorf("Checkpoint(%q) = %d, want %d", tc.epoch, got, tc.want)
		}
	}
}
