package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/EsDmitrii/kconmon-ng/api/proto"
	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func outboxMetrics() *metrics.Metrics {
	return metrics.New("kconmon_ng", prometheus.NewRegistry())
}

// stubStore is a minimal ReceiptStore for outbox unit tests.
type stubStore struct {
	mu          sync.Mutex
	rows        []stubRow
	failReceive bool
	projectErr  error
	deadErr     bool
}

type stubRow struct {
	r      Receipt
	status string
}

func (s *stubStore) ReceiveEvent(_ context.Context, r Receipt) (bool, error) {
	if s.failReceive {
		return false, errors.New("db down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ex := range s.rows {
		if ex.r.ID == r.ID {
			return false, nil
		}
	}
	s.rows = append(s.rows, stubRow{r: r, status: "received"})
	return true, nil
}

func (s *stubStore) ReceiveBaseline(_ context.Context, b BaselineReceipt) (bool, error) {
	if s.failReceive {
		return false, errors.New("db down")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	id := b.ID
	for _, ex := range s.rows {
		if ex.r.ID == id {
			return false, nil
		}
	}
	s.rows = append(s.rows, stubRow{
		r:      Receipt{ID: b.ID, Epoch: b.Epoch, Seq: 0, Timestamp: b.Timestamp, Raw: nil},
		status: "received",
	})
	return true, nil
}

func (s *stubStore) ProjectPending(_ context.Context, batch int, proj Projector) ([]ProjectedEvent, []DeadEvent, error) {
	s.mu.Lock()
	var due []*stubRow
	for i := range s.rows {
		if s.rows[i].status == "received" {
			due = append(due, &s.rows[i])
		}
	}
	s.mu.Unlock()
	if len(due) > batch {
		due = due[:batch]
	}
	out := make([]ProjectedEvent, 0, len(due))
	var dead []DeadEvent
	for _, r := range due {
		live, err := proj(KindEvent, r.r.Raw, LiveEvent{})
		if err != nil {
			if errors.Is(err, ErrPermanent) || s.deadErr {
				r.status = "dead"
				dead = append(dead, DeadEvent{ID: r.r.ID, Seq: r.r.Seq, Reason: err.Error()})
				continue
			}
			return nil, nil, s.projectErr
		}
		r.status = "projected"
		out = append(out, ProjectedEvent{ID: r.r.ID, Epoch: r.r.Epoch, Seq: r.r.Seq, Kind: KindEvent, Live: live, History: true})
	}
	return out, dead, nil
}

func (s *stubStore) Checkpoint(_ context.Context, epoch string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var highest uint64
	for _, r := range s.rows {
		if r.r.Epoch == epoch && r.r.Seq > highest {
			highest = r.r.Seq
		}
	}
	return highest, nil
}

func (s *stubStore) Counts(_ context.Context) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var received, dead int64
	for _, r := range s.rows {
		switch r.status {
		case "received":
			received++
		case "dead":
			dead++
		}
	}
	return received, dead, nil
}

func (s *stubStore) count(status string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.rows {
		if r.status == status {
			n++
		}
	}
	return n
}

// memSpool is a SpoolKV fake with ListPrefix support.
type memSpool struct {
	mu sync.Mutex
	m  map[string][]byte
}

func newMemSpool() *memSpool { return &memSpool{m: map[string][]byte{}} }

func (s *memSpool) Get(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	return v, ok, nil
}
func (s *memSpool) Set(_ context.Context, key string, val []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]byte(nil), val...)
	s.m[key] = cp
	return nil
}
func (s *memSpool) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.m, key)
	return nil
}
func (s *memSpool) ListPrefix(_ context.Context, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for k := range s.m {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k)
		}
	}
	return out, nil
}

func testReceipt(t *testing.T, seq uint64) Receipt {
	t.Helper()
	const epoch = "ep"
	ts := time.Date(2026, 5, 1, 10, 0, int(seq), 0, time.UTC)
	ev := &pb.Event{
		EventId:   fmt.Sprintf("evt-%s-%d", epoch, seq),
		Seq:       seq,
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Event_TopologyChanged{
			TopologyChanged: &pb.TopologyChanged{Reason: "agent_registered", NodeName: "node-a"},
		},
	}
	raw, err := proto.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return Receipt{ID: ev.EventId, Epoch: epoch, Seq: seq, Timestamp: ts, Raw: raw}
}

// waitMsg reads one bus message or fails.
func waitMsg(t *testing.T, ch <-chan cache.Message) cache.Message {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(3 * time.Second):
		t.Fatal("no bus message")
		return cache.Message{}
	}
}

func TestOutboxConfirmedFrameFollowsProjection(t *testing.T) {
	store := &stubStore{}
	bus := cache.NewInProcessBus()
	msgs, unsub := bus.Subscribe("live")
	defer unsub()
	o := newOutbox(store, bus, nil, outboxMetrics())

	o.receive(context.Background(), testReceipt(t, 1))
	o.drainProjected(context.Background())

	msg := waitMsg(t, msgs)
	if msg.Type != "event" {
		t.Fatalf("frame type = %q, want event", msg.Type)
	}
	var live LiveEvent
	if err := json.Unmarshal(msg.Data, &live); err != nil {
		t.Fatal(err)
	}
	if live.Persisted == nil || !*live.Persisted {
		t.Fatalf("persisted = %v, want true", live.Persisted)
	}
	if store.count("projected") != 1 {
		t.Fatal("row not projected")
	}
}

func TestOutboxUnconfirmedThenRecoveredUpgradesFrame(t *testing.T) {
	store := &stubStore{failReceive: true}
	bus := cache.NewInProcessBus()
	msgs, unsub := bus.Subscribe("live")
	defer unsub()
	spool := newMemSpool()
	o := newOutbox(store, bus, spool, outboxMetrics())

	o.receive(context.Background(), testReceipt(t, 1))
	pending := waitMsg(t, msgs)
	if pending.Type != "event_pending" {
		t.Fatalf("type = %q, want event_pending", pending.Type)
	}
	keys, _ := spool.ListPrefix(context.Background(), pendingSpoolPrefix)
	if len(keys) != 1 {
		t.Fatalf("durable spool has %d keys, want 1", len(keys))
	}

	// Storage recovers: the next cycle re-receipts and projects.
	store.failReceive = false
	o.retryPending(context.Background())
	o.drainProjected(context.Background())

	confirmed := waitMsg(t, msgs)
	if confirmed.Type != "event" {
		t.Fatalf("type = %q, want event", confirmed.Type)
	}
	keys, _ = spool.ListPrefix(context.Background(), pendingSpoolPrefix)
	if len(keys) != 0 {
		t.Fatalf("spool still has %d keys after catch-up", len(keys))
	}
	if o.pendingCount() != 0 {
		t.Fatal("memory spool did not drain")
	}
}

func TestOutboxRecoversSpoolOnStartup(t *testing.T) {
	store := &stubStore{}
	bus := cache.NewInProcessBus()
	msgs, unsub := bus.Subscribe("live")
	defer unsub()
	spool := newMemSpool()

	// A previous process died mid-outage: the receipt is only in the durable spool.
	r := testReceipt(t, 9)
	data, err := json.Marshal(pendingReceipt(r))
	if err != nil {
		t.Fatal(err)
	}
	if err := spool.Set(context.Background(), pendingSpoolPrefix+r.ID, data, time.Hour); err != nil {
		t.Fatal(err)
	}

	o := newOutbox(store, bus, spool, outboxMetrics())
	ctx := t.Context()
	go o.run(ctx)

	msg := waitMsg(t, msgs)
	if msg.Type != "event" {
		t.Fatalf("recovered event type = %q, want event", msg.Type)
	}
	if store.count("projected") != 1 {
		t.Fatal("the recovered receipt was not projected")
	}
}

func TestOutboxDeadLetterIsDiagnosedNotFannedOut(t *testing.T) {
	store := &stubStore{deadErr: true}
	bus := cache.NewInProcessBus()
	msgs, unsub := bus.Subscribe("live")
	defer unsub()
	o := newOutbox(store, bus, nil, outboxMetrics())
	// A projector that cannot project this payload.
	o.project = func(string, []byte, LiveEvent) (LiveEvent, error) {
		return LiveEvent{}, fmt.Errorf("%w: unknown payload", ErrPermanent)
	}

	o.receive(context.Background(), testReceipt(t, 1))
	o.drainProjected(context.Background())

	select {
	case msg := <-msgs:
		t.Fatalf("dead event must not be fanned out, got %s", msg.Type)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestOutboxSpoolCapDropsBoundedLoss(t *testing.T) {
	store := &stubStore{failReceive: true}
	bus := cache.NewInProcessBus()
	_, unsub := bus.Subscribe("live")
	defer unsub()
	o := newOutbox(store, bus, nil, outboxMetrics())
	o.spoolCap = 2

	o.handleUnreachable(context.Background(), testReceipt(t, 1), errors.New("db down"))
	o.handleUnreachable(context.Background(), testReceipt(t, 2), errors.New("db down"))
	o.handleUnreachable(context.Background(), testReceipt(t, 3), errors.New("db down"))

	if o.pendingCount() != 2 {
		t.Fatalf("pending = %d, want cap 2", o.pendingCount())
	}
}
