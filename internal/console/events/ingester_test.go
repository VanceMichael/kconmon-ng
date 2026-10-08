package events_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/EsDmitrii/kconmon-ng/api/proto"
	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/controllerclient"
	"github.com/EsDmitrii/kconmon-ng/internal/console/events"
	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// liveBusTopic is ws.TopicLive spelled out; this package cannot import internal/console/ws (ws
// imports this one so the Hub can dedupe by LiveEvent.ID).
const liveBusTopic = "live"

// preconditionGrace is for the tests that only need the ingester to reach a healthy stream before
// getting on with their real subject.
const preconditionGrace = 20 * time.Millisecond

// waitFor polls cond until it holds, or fails the test. Used instead of sleeps
// because the ingester's first reconnect only happens after a 1s backoff.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after 10s waiting for %s", what)
}

// listenLoopback opens a loopback listener on an OS-chosen port. It goes through
// net.ListenConfig because the repo's linter (noctx) requires the context-aware
// form, exactly as internal/console/httpapi/server.go's Run does.
func listenLoopback(t *testing.T) (net.Listener, error) {
	t.Helper()
	var lc net.ListenConfig
	return lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
}

// fakeControllerAPI is the controller's HTTP side: just enough of
// GET /api/v1/version for the ingester's capability precheck, with the
// capability list swappable at runtime.
type fakeControllerAPI struct {
	mu           sync.Mutex
	capabilities []string
	calls        atomic.Int64
	down         atomic.Bool
	// topology, when set, is served on GET /api/v1/topology; nil answers 404 like a route this
	// fake does not have, so the tests that predate baselines see no extra sink writes.
	topology  atomic.Pointer[controllerclient.Topology]
	topoCalls atomic.Int64
}

func (f *fakeControllerAPI) setCapabilities(caps ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.capabilities = caps
}

// setDown makes the version endpoint fail like a controller that is down or
// broken, as opposed to one that is up and simply not offering events.
func (f *fakeControllerAPI) setDown(down bool) { f.down.Store(down) }

func (f *fakeControllerAPI) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/topology" {
			topo := f.topology.Load()
			if topo == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.topoCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(topo)
			return
		}
		if r.URL.Path != "/api/v1/version" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.calls.Add(1)
		if f.down.Load() {
			// 500, not 503: 503 means "not the leader" and controllerclient
			// retries it internally, which is a different scenario.
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		f.mu.Lock()
		caps := append([]string(nil), f.capabilities...)
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version": "1.6.0", "commit": "abc123", "capabilities": caps,
		})
	})
}

// startFakeController starts the fake controller HTTP API and returns it plus a
// real controllerclient pointed at it.
func startFakeController(t *testing.T, caps ...string) (*fakeControllerAPI, *controllerclient.Client) {
	t.Helper()
	api := &fakeControllerAPI{capabilities: caps}
	srv := httptest.NewServer(api.handler())
	t.Cleanup(srv.Close)
	return api, controllerclient.New(srv.URL, 5*time.Second)
}

// fakeEventStream is a real gRPC EventStream server. WatchEvents forwards
// whatever the test sends on events, and returns an error on demand so the
// reconnect path can be exercised.
type fakeEventStream struct {
	pb.UnimplementedEventStreamServer
	events chan *pb.Event
	fail   chan struct{}
	calls  atomic.Int64
}

func newFakeEventStream() *fakeEventStream {
	return &fakeEventStream{events: make(chan *pb.Event), fail: make(chan struct{})}
}

func (f *fakeEventStream) WatchEvents(_ *pb.WatchEventsRequest, stream pb.EventStream_WatchEventsServer) error {
	f.calls.Add(1)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-f.fail:
			return grpcstatus.Error(codes.Internal, "induced stream failure")
		case ev := <-f.events:
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

// send hands ev to the currently-served stream, failing the test if no stream
// picks it up.
func (f *fakeEventStream) send(t *testing.T, ev *pb.Event) {
	t.Helper()
	select {
	case f.events <- ev:
	case <-time.After(5 * time.Second):
		t.Fatal("no WatchEvents stream consumed the event within 5s")
	}
}

// breakStream makes the stream currently being served return an error, exactly
// as a controller restart or a lost leader lease would.
func (f *fakeEventStream) breakStream(t *testing.T) {
	t.Helper()
	select {
	case f.fail <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatal("no WatchEvents stream was being served, so none could be broken")
	}
}

// startFakeEventStream serves the fake on a loopback TCP listener and returns it with its address.
func startFakeEventStream(t *testing.T) (*fakeEventStream, string) {
	t.Helper()
	fake := newFakeEventStream()
	return fake, serveEventStream(t, fake)
}

// serveEventStream runs any EventStreamServer on a loopback listener and returns
// its address.
func serveEventStream(t *testing.T, srv pb.EventStreamServer) string {
	t.Helper()
	lis, err := listenLoopback(t)
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	gs := grpc.NewServer()
	pb.RegisterEventStreamServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String()
}

// rejectDelay is how long rejectingEventStream holds an accepted stream before refusing it; two
// orders of magnitude below the ingester's connect grace.
const rejectDelay = 50 * time.Millisecond

// rejectingEventStream accepts the subscription and then refuses it; the client sees this at its
// first Recv, never at the WatchEvents call.
type rejectingEventStream struct {
	pb.UnimplementedEventStreamServer
	calls atomic.Int64
}

func (f *rejectingEventStream) WatchEvents(_ *pb.WatchEventsRequest, stream pb.EventStream_WatchEventsServer) error {
	f.calls.Add(1)
	select {
	case <-time.After(rejectDelay):
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	return grpcstatus.Error(codes.Unavailable, "not the leader")
}

// countingListener accepts and immediately closes every connection, counting
// them. It is how a test asserts "the ingester did not dial" rather than the
// much weaker "the ingester did not succeed".
func countingListener(t *testing.T) (addr string, accepted *atomic.Int64) {
	t.Helper()
	lis, err := listenLoopback(t)
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	t.Cleanup(func() { _ = lis.Close() })
	accepted = &atomic.Int64{}
	go func() {
		for {
			conn, aerr := lis.Accept()
			if aerr != nil {
				return
			}
			accepted.Add(1)
			_ = conn.Close()
		}
	}()
	return lis.Addr().String(), accepted
}

// fakeInboxRow is one receipt in the fakeReceiptStore.
type fakeInboxRow struct {
	id     string
	epoch  string
	seq    uint64
	kind   string
	raw    []byte
	live   events.LiveEvent
	status string // received|projected|dead
	reason string
}

// fakeReceiptStore is an in-memory events.ReceiptStore modelling the real outbox semantics:
// dedup at the receipt gate, ordered claim/projection, permanent vs transient errors, checkpoints.
type fakeReceiptStore struct {
	mu        sync.Mutex
	rows      []*fakeInboxRow
	fail      atomic.Bool   // ReceiveEvent/ReceiveBaseline fail transiently while true
	conflict  atomic.Bool   // every receipt loses the event_id gate
	block     chan struct{} // projection blocks until this closes, but only while rows are due
	projected int
}

func (f *fakeReceiptStore) ReceiveEvent(_ context.Context, r events.Receipt) (bool, error) {
	if f.fail.Load() {
		return false, errors.New("database is down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conflict.Load() {
		return false, nil
	}
	if f.findByIDLocked(r.ID) != nil {
		return false, nil
	}
	f.rows = append(f.rows, &fakeInboxRow{id: r.ID, epoch: r.Epoch, seq: r.Seq, kind: events.KindEvent, raw: r.Raw, status: "received"})
	return true, nil
}

func (f *fakeReceiptStore) ReceiveBaseline(_ context.Context, b events.BaselineReceipt) (bool, error) {
	if f.fail.Load() {
		return false, errors.New("database is down")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.conflict.Load() || f.findByIDLocked(b.ID) != nil {
		return false, nil
	}
	f.rows = append(f.rows, &fakeInboxRow{
		id: b.ID, epoch: b.Epoch, seq: 0, kind: events.KindBaseline,
		live: b.Event, status: "received",
	})
	return true, nil
}

func (f *fakeReceiptStore) ProjectPending(ctx context.Context, batch int, proj events.Projector) ([]events.ProjectedEvent, []events.DeadEvent, error) {
	if f.block != nil {
		snapshot := f.dueCount()
		if snapshot > 0 {
			select {
			case <-f.block:
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	due := f.dueLocked()
	sortInboxRows(due)
	if len(due) > batch {
		due = due[:batch]
	}
	out := make([]events.ProjectedEvent, 0, len(due))
	var dead []events.DeadEvent
	for _, r := range due {
		live, err := proj(r.kind, r.raw, r.live)
		if err != nil {
			if errors.Is(err, events.ErrPermanent) {
				r.status = "dead"
				r.reason = err.Error()
				dead = append(dead, events.DeadEvent{ID: r.id, Seq: r.seq, Reason: r.reason})
				continue
			}
			return nil, nil, err
		}
		r.status = "projected"
		r.live = live
		f.projected++
		out = append(out, events.ProjectedEvent{
			ID: r.id, Epoch: r.epoch, Seq: r.seq, Kind: r.kind, Live: live, History: true,
		})
	}
	return out, dead, nil
}

func (f *fakeReceiptStore) Checkpoint(_ context.Context, epoch string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var highest uint64
	for _, r := range f.rows {
		if r.epoch == epoch && r.seq > highest {
			highest = r.seq
		}
	}
	return highest, nil
}

func (f *fakeReceiptStore) Counts(_ context.Context) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var received, dead int64
	for _, r := range f.rows {
		switch r.status {
		case "received":
			received++
		case "dead":
			dead++
		}
	}
	return received, dead, nil
}

func (f *fakeReceiptStore) findByIDLocked(id string) *fakeInboxRow {
	for _, r := range f.rows {
		if r.id == id {
			return r
		}
	}
	return nil
}

func (f *fakeReceiptStore) dueLocked() []*fakeInboxRow {
	var due []*fakeInboxRow
	for _, r := range f.rows {
		if r.status == "received" {
			due = append(due, r)
		}
	}
	return due
}

func (f *fakeReceiptStore) dueCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dueLocked())
}

func sortInboxRows(rows []*fakeInboxRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].epoch != rows[j].epoch {
			return rows[i].epoch < rows[j].epoch
		}
		return rows[i].seq < rows[j].seq
	})
}

// projectedEvents returns projected EVENT rows in projection order.
func (f *fakeReceiptStore) projectedEvents() []events.LiveEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []events.LiveEvent
	for _, r := range f.rows {
		if r.kind == events.KindEvent && r.status == "projected" {
			out = append(out, r.live)
		}
	}
	return out
}

// projectedBaselines returns projected BASELINE rows.
func (f *fakeReceiptStore) projectedBaselines() []events.LiveEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []events.LiveEvent
	for _, r := range f.rows {
		if r.kind == events.KindBaseline && r.status == "projected" {
			out = append(out, r.live)
		}
	}
	return out
}

// statusCounts reports how many rows are in each inbox status.
func (f *fakeReceiptStore) statusCounts() map[string]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[string]int{}
	for _, r := range f.rows {
		out[r.status]++
	}
	return out
}

// newTestMetrics gives every test a fresh registry: promauto panics on a
// duplicate registration.
func newTestMetrics() *metrics.Metrics {
	return metrics.New("kconmon_ng", prometheus.NewRegistry())
}

// capturedLog is one record a capturingHandler kept.
type capturedLog struct {
	level slog.Level
	msg   string
}

// capturingHandler records what the ingester logs so a test can assert the
// LEVEL, not just the text. The ingester logs from its own goroutine, so every
// access is locked.
type capturingHandler struct {
	mu      sync.Mutex
	records []capturedLog
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, capturedLog{level: r.Level, msg: r.Message})
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

// findLevel returns the level a message containing substr was logged at, and
// whether such a message was logged at all.
func (h *capturingHandler) findLevel(substr string) (slog.Level, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, rec := range h.records {
		if strings.Contains(rec.msg, substr) {
			return rec.level, true
		}
	}
	return 0, false
}

// captureLogs swaps in a capturing default logger for the duration of one test.
// Tests in a package run sequentially, so replacing the default logger is safe —
// same reasoning as discardLogs in internal/console/cache/inprocess_test.go.
func captureLogs(t *testing.T) *capturingHandler {
	t.Helper()
	h := &capturingHandler{}
	previous := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return h
}

// The metric label is the same (reason="capability") for a controller that is up and not offering
// events and for one that is unreachable.
func TestIngesterLogsAMissingCapabilityAtInfoAndABrokenPrecheckAtWarn(t *testing.T) {
	tests := []struct {
		name      string
		down      bool
		substr    string
		wantLevel slog.Level
		absent    string
	}{
		{
			name:      "controller up but not offering events logs at info",
			down:      false,
			substr:    "not offering realtime events",
			wantLevel: slog.LevelInfo,
			absent:    "precheck failed",
		},
		{
			name:      "controller down logs the precheck failure at warn",
			down:      true,
			substr:    "precheck failed",
			wantLevel: slog.LevelWarn,
			absent:    "not offering realtime events",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			api, ctrl := startFakeController(t) // never advertises "events"
			api.setDown(tc.down)
			addr, _ := countingListener(t)

			m := newTestMetrics()
			ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { defer close(done); ing.Run(ctx) }()

			waitFor(t, "a capability-gated retry", func() bool {
				return testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("capability")) >= 1
			})
			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Run did not return after ctx cancel")
			}

			level, ok := logs.findLevel(tc.substr)
			if !ok {
				t.Fatalf("nothing was logged containing %q", tc.substr)
			}
			if level != tc.wantLevel {
				t.Errorf("%q logged at %v, want %v", tc.substr, level, tc.wantLevel)
			}
			if _, found := logs.findLevel(tc.absent); found {
				t.Errorf("%q was logged, but this case is the other one", tc.absent)
			}
		})
	}
}

func TestIngesterWithoutGRPCAddrIsDisabled(t *testing.T) {
	ing := events.NewIngester(nil, "", cache.NewInProcessBus(), newTestMetrics())

	done := make(chan struct{})
	go func() { defer close(done); ing.Run(context.Background()) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run must return immediately when grpcAddr is empty")
	}
	if ing.Healthy() {
		t.Error("a disabled ingester must never report healthy")
	}
}

// The capability precheck runs before EVERY dial, so a controller that does not
// advertise "events" must never see a gRPC connection at all.
func TestIngesterNeverDialsWithoutTheEventsCapability(t *testing.T) {
	api, ctrl := startFakeController(t) // no capabilities advertised

	// A listener nothing should ever connect to; every accept is counted.
	addr, accepted := countingListener(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	capabilityRetries := m.IngesterReconnects.WithLabelValues("capability")
	waitFor(t, "two capability-gated retries", func() bool {
		return testutil.ToFloat64(capabilityRetries) >= 2
	})

	if got := accepted.Load(); got != 0 {
		t.Errorf("the ingester opened %d connections without the capability, want 0", got)
	}
	if got := testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("dial")); got != 0 {
		t.Errorf("reason=dial counted %v times, want 0 — a missing capability is not a dial failure", got)
	}
	if got := api.calls.Load(); got < 2 {
		t.Errorf("the version endpoint was probed %d times, want at least 2 — the precheck runs before every attempt", got)
	}
	if ing.Healthy() {
		t.Error("Healthy must stay false while the capability is absent")
	}
	if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 0 {
		t.Errorf("ingester_connected = %v, want 0", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestIngesterPublishesLiveEventsToTheBus(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m)
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)
	if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 1 {
		t.Errorf("ingester_connected = %v, want 1 while the stream is up", got)
	}

	fake.send(t, &pb.Event{
		Seq:       17,
		Timestamp: timestamppb.New(time.Unix(0, 1753400000000000000).UTC()),
		Payload: &pb.Event_CheckObserved{CheckObserved: &pb.CheckObserved{
			TaskId: "t-1", CheckType: "tcp", SourceNode: "node-a", DestinationNode: "node-b",
			Plane: "pod", Success: false, DurationNs: 1200000, Error: "dial timeout",
		}},
	})

	select {
	case msg := <-msgs:
		if msg.Type != "event" {
			t.Errorf("bus message type = %q, want %q", msg.Type, "event")
		}
		var live events.LiveEvent
		if err := json.Unmarshal(msg.Data, &live); err != nil {
			t.Fatalf("bus payload is not a LiveEvent: %v (%s)", err, msg.Data)
		}
		if live.ID != "17-1753400000000000000" {
			t.Errorf("LiveEvent.ID = %q, want %q", live.ID, "17-1753400000000000000")
		}
		if live.Type != "check_observed" || live.Severity != "error" || live.Scope != "node-a→node-b" {
			t.Errorf("unexpected projection: type=%q severity=%q scope=%q", live.Type, live.Severity, live.Scope)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the event never reached the bus")
	}

	waitFor(t, "events_received_total{type=check_observed}", func() bool {
		return testutil.ToFloat64(m.EventsReceived.WithLabelValues("check_observed")) == 1
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	waitFor(t, "ingester_connected to fall back to 0 after shutdown", func() bool {
		return testutil.ToFloat64(m.IngesterConnected.WithLabelValues()) == 0
	})
}

func TestIngesterReconnectsAfterStreamError(t *testing.T) {
	api, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the first stream", ing.Healthy)
	fake.breakStream(t)

	waitFor(t, "Healthy to flip false after the stream broke", func() bool { return !ing.Healthy() })
	waitFor(t, "a reason=stream reconnect to be counted", func() bool {
		return testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("stream")) >= 1
	})
	waitFor(t, "Healthy to flip back true after the backoff", ing.Healthy)

	// The server-side handler is entered asynchronously with respect to the
	// client's stream creation, so the ingester can already be Healthy before
	// gRPC has dispatched WatchEvents on the fake. Poll rather than sample.
	waitFor(t, "WatchEvents to be served a second time", func() bool {
		return fake.calls.Load() >= 2
	})
	if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 1 {
		t.Errorf("ingester_connected = %v, want 1 after the reconnect", got)
	}
	if got := testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("capability")); got != 0 {
		t.Errorf("reason=capability counted %v times, want 0 — the capability was present throughout", got)
	}
	if got := api.calls.Load(); got < 2 {
		t.Errorf("the version endpoint was probed %d times, want at least 2 — the precheck also runs before a reconnect", got)
	}

	// The reconnected stream is a working one, not just an established socket.
	fake.send(t, &pb.Event{Seq: 2, Timestamp: timestamppb.Now(), Payload: &pb.Event_TopologyChanged{
		TopologyChanged: &pb.TopologyChanged{Reason: "agent_registered", NodeName: "node-a"},
	}})
	waitFor(t, "events_received_total{type=topology_changed}", func() bool {
		return testutil.ToFloat64(m.EventsReceived.WithLabelValues("topology_changed")) >= 1
	})

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// A stream that reached the connected state resets the reconnect backoff: an outage earlier in the
// process's life must not make every later disconnect wait the full ceiling, since events emitted
// in that gap are lost.
func TestIngesterResetsTheBackoffAfterAHealthyStream(t *testing.T) {
	api, ctrl := startFakeController(t, "events")
	api.setDown(true)
	fake, addr := startFakeEventStream(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)
	ing.SetConnectGrace(preconditionGrace)
	const initial, ceiling = 25 * time.Millisecond, 1600 * time.Millisecond
	ing.SetBackoff(initial, ceiling)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	// Six failed prechecks double the backoff 25ms -> 1.6s, the ceiling.
	waitFor(t, "six failed capability prechecks", func() bool {
		return testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("capability")) >= 6
	})
	api.setDown(false)
	waitFor(t, "the stream after the outage", ing.Healthy)

	broke := time.Now()
	fake.breakStream(t)
	waitFor(t, "WatchEvents to be served a second time", func() bool { return fake.calls.Load() >= 2 })
	if took := time.Since(broke); took >= ceiling/2 {
		t.Errorf("reconnect after a healthy stream took %v, want about %v: the backoff was not reset", took, initial)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// A controller that only turns events on later must be picked up by the very
// next retry, because the precheck runs before every dial rather than once.
func TestIngesterConnectsOnceTheCapabilityAppears(t *testing.T) {
	api, ctrl := startFakeController(t) // starts with no capabilities
	fake, addr := startFakeEventStream(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "at least one capability-gated retry", func() bool {
		return testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("capability")) >= 1
	})
	if fake.calls.Load() != 0 {
		t.Fatalf("WatchEvents was called %d times before the capability appeared, want 0", fake.calls.Load())
	}

	api.setCapabilities("events")
	waitFor(t, "the ingester to connect after the capability appeared", ing.Healthy)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// A controller that is DOWN is a different situation from one that is up and not offering events.
func TestIngesterTreatsAControllerOutageAsACapabilityRetryWithoutDialing(t *testing.T) {
	api, ctrl := startFakeController(t, "events")
	api.setDown(true)

	addr, accepted := countingListener(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "two retries while the controller is down", func() bool {
		return testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("capability")) >= 2
	})

	if got := accepted.Load(); got != 0 {
		t.Errorf("the ingester opened %d gRPC connections while the controller was down, want 0", got)
	}
	for _, reason := range []string{"dial", "stream"} {
		if got := testutil.ToFloat64(m.IngesterReconnects.WithLabelValues(reason)); got != 0 {
			t.Errorf("reason=%s counted %v times, want 0 — the precheck failed before any dial", reason, got)
		}
	}
	if ing.Healthy() {
		t.Error("Healthy must stay false while the controller is down")
	}
	if got := api.calls.Load(); got < 2 {
		t.Errorf("the version endpoint was probed %d times, want at least 2", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// A non-leader controller accepts the subscription and rejects it at once; healthy must never go
// true, because derives the console's browser-facing capabilities from it and the realtime badge
// would otherwise flicker on every retry cycle.
func TestIngesterNeverReportsHealthyAgainstARejectingController(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	rejecting := &rejectingEventStream{}
	addr := serveEventStream(t, rejecting)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	// Observe until three whole attempts have been rejected, asserting throughout that the ingester
	// never once reported healthy.
	const wantRejections = 3
	deadline := time.Now().Add(30 * time.Second)
	for testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("stream")) < wantRejections {
		if ing.Healthy() {
			t.Fatal("Healthy went true against a controller that rejects every stream")
		}
		if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 0 {
			t.Fatalf("ingester_connected = %v while every stream is being rejected, want 0", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d rejected attempts, saw %v",
				wantRejections, testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("stream")))
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The rejection arrives at the first Recv, so it is a stream failure and not
	// a dial failure — the distinction the corrected comment in attempt records.
	if got := rejecting.calls.Load(); got < wantRejections {
		t.Errorf("WatchEvents was attempted %d times, want at least %d", got, wantRejections)
	}
	if got := testutil.ToFloat64(m.IngesterReconnects.WithLabelValues("dial")); got != 0 {
		t.Errorf("reason=dial counted %v times, want 0 — the stream was accepted, then refused", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// Invariant: a grace timer left over from an attempt that has already ended must never promote the
// ingester.
func TestIngesterGraceTimerCannotResurrectAFinishedAttempt(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), m)
	const grace = 300 * time.Millisecond
	ing.SetConnectGrace(grace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	// Cancel while the stream is being served but before the grace elapses, so
	// the attempt ends with its timer still armed.
	waitFor(t, "the stream to be served", func() bool { return fake.calls.Load() >= 1 })
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	if ing.Healthy() {
		t.Fatal("Healthy must be false immediately after Run returns")
	}

	time.Sleep(3 * grace)

	if ing.Healthy() {
		t.Error("a stale grace timer resurrected Healthy after the attempt had ended")
	}
	if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 0 {
		t.Errorf("ingester_connected = %v after shutdown, want 0", got)
	}
}

// The other half of the promotion rule: an event received promotes the ingester immediately rather
// than making it wait out the grace period.
func TestIngesterBecomesHealthyOnTheFirstEventAheadOfTheGrace(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m)
	ing.SetConnectGrace(10 * time.Minute) // only an event can promote in time

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	fake.send(t, &pb.Event{Seq: 1, Timestamp: fixedTime(), Payload: &pb.Event_TopologyChanged{
		TopologyChanged: &pb.TopologyChanged{Reason: "agent_registered", NodeName: "node-a"},
	}})

	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("the event never reached the bus")
	}

	// Promotion happens before the event is published, so by the time the bus
	// message is observable this is already true — no polling needed, which is
	// what makes the assertion about ordering rather than about timing.
	if !ing.Healthy() {
		t.Error("Healthy must be true once an event has been received and published")
	}
	if got := testutil.ToFloat64(m.IngesterConnected.WithLabelValues()); got != 1 {
		t.Errorf("ingester_connected = %v, want 1", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	waitFor(t, "ingester_connected to fall back to 0 after shutdown", func() bool {
		return testutil.ToFloat64(m.IngesterConnected.WithLabelValues()) == 0
	})
	if ing.Healthy() {
		t.Error("Healthy must be false after Run returns")
	}
}

// topologyEvent builds a minimal pb.Event with the given seq, useful for the
// persistence tests below where the payload content itself is not the point.
func topologyEvent(seq uint64) *pb.Event {
	return &pb.Event{
		Seq:       seq,
		Timestamp: timestamppb.Now(),
		Payload: &pb.Event_TopologyChanged{TopologyChanged: &pb.TopologyChanged{
			Reason: "agent_registered", NodeName: "node-a",
		}},
	}
}

// TestIngesterPersistsAfterPublish is the core receive-first wiring: the bus frame leaves only
// after the event has been durably receipted AND projected, and it arrives marked persisted.
func TestIngesterPersistsAfterPublish(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	store := &fakeReceiptStore{}
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	fake.send(t, topologyEvent(42))

	var msg cache.Message
	select {
	case msg = <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("the event never reached the bus")
	}
	var busEvent events.LiveEvent
	if err := json.Unmarshal(msg.Data, &busEvent); err != nil {
		t.Fatalf("bus payload is not a LiveEvent: %v", err)
	}
	if msg.Type != "event" {
		t.Errorf("frame type = %q, want confirmed %q", msg.Type, "event")
	}
	if busEvent.Persisted == nil || !*busEvent.Persisted {
		t.Errorf("persisted = %v, want true: a confirmed frame follows the history commit", busEvent.Persisted)
	}

	waitFor(t, "one event to be projected", func() bool { return len(store.projectedEvents()) == 1 })

	got := store.projectedEvents()[0]
	if got.Seq != busEvent.Seq || !got.Timestamp.Equal(busEvent.Timestamp) ||
		got.Type != busEvent.Type || string(got.Details) != string(busEvent.Details) {
		t.Errorf("projected %+v, want it to match the bus event %+v", got, busEvent)
	}
	if counts := store.statusCounts(); counts["projected"] != 1 || counts["received"] != 0 {
		t.Errorf("inbox status counts = %v, want one projected and none received", counts)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// Scope and summary come from it too, but Details is the load-bearing one -- it is the ONLY thing
// store.foldTopology reads.
func TestIngesterPersistsTopologyAttribution(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	_, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	store := &fakeReceiptStore{}
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	fake.send(t, &pb.Event{Seq: 7, Timestamp: fixedTime(), Payload: &pb.Event_TopologyChanged{
		TopologyChanged: &pb.TopologyChanged{
			Reason: "agent_registered", NodeName: "node-a", AgentId: "agent-a", Zone: "zone-a",
		},
	}})

	waitFor(t, "one event to be projected", func() bool { return len(store.projectedEvents()) == 1 })

	got := store.projectedEvents()[0]
	if got.Scope != "node-a" {
		t.Errorf("scope = %q, want the node the change was about", got.Scope)
	}
	if got.Summary != "topology changed: agent_registered (node-a)" {
		t.Errorf("summary = %q", got.Summary)
	}
	const wantDetails = `{"reason":"agent_registered","nodeName":"node-a","agentId":"agent-a","zone":"zone-a"}`
	if string(got.Details) != wantDetails {
		t.Errorf("details =\n  %s\nwant\n  %s", got.Details, wantDetails)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestIngesterStorageOutageMarksUnconfirmedThenCatchesUp is the divergence fix: while the receipt
// gate is unreachable every event still reaches the feed but as an explicitly UNCONFIRMED frame;
// when storage returns the same ids are projected and re-delivered as confirmed, and no history
// is missing.
func TestIngesterStorageOutageMarksUnconfirmedThenCatchesUp(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	store := &fakeReceiptStore{}
	store.fail.Store(true)
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	for seq := uint64(1); seq <= 3; seq++ {
		fake.send(t, topologyEvent(seq))
		var msg cache.Message
		select {
		case msg = <-msgs:
		case <-time.After(5 * time.Second):
			t.Fatalf("event seq=%d never reached the bus", seq)
		}
		if msg.Type != "event_pending" {
			t.Fatalf("seq=%d frame type = %q, want event_pending while storage is down", seq, msg.Type)
		}
		var live events.LiveEvent
		if err := json.Unmarshal(msg.Data, &live); err != nil {
			t.Fatal(err)
		}
		if live.Persisted == nil || *live.Persisted {
			t.Fatalf("seq=%d persisted = %v, want false", seq, live.Persisted)
		}
	}

	waitFor(t, `events_receipts_total{result="error"} == 3`, func() bool {
		return testutil.ToFloat64(m.EventsReceipts.WithLabelValues("error")) == 3
	})
	waitFor(t, "events_unconfirmed gauge == 3", func() bool {
		return testutil.ToFloat64(m.EventsUnconfirmed.WithLabelValues()) == 3
	})
	if !ing.Healthy() {
		t.Error("a storage outage must not knock the ingester unhealthy")
	}

	// Storage returns: the retained events must be receipted, projected and re-delivered CONFIRMED
	// under the same ids.
	store.fail.Store(false)

	confirmed := make(map[uint64]bool)
	deadline := time.After(10 * time.Second)
	for len(confirmed) < 3 {
		select {
		case msg := <-msgs:
			if msg.Type != "event" {
				t.Fatalf("caught-up frame type = %q, want event", msg.Type)
			}
			var live events.LiveEvent
			if err := json.Unmarshal(msg.Data, &live); err != nil {
				t.Fatal(err)
			}
			if live.Persisted == nil || !*live.Persisted {
				t.Fatalf("caught-up frame for seq=%d is still unconfirmed", live.Seq)
			}
			confirmed[live.Seq] = true
		case <-deadline:
			t.Fatalf("only %d/3 events caught up as confirmed", len(confirmed))
		}
	}
	waitFor(t, "the unconfirmed backlog to drain to zero", func() bool {
		return testutil.ToFloat64(m.EventsUnconfirmed.WithLabelValues()) == 0
	})
	if projected := len(store.projectedEvents()); projected != 3 {
		t.Errorf("projected %d events, want 3", projected)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestIngesterDuplicateReceiptDrivesNothingTwice is the dedup contract at the receipt gate: a
// reconnect replay (or a duplicate stream) that redelivers the SAME event id must produce exactly
// one history row and one live frame, and a conflict receipt is not logged as a warning.
func TestIngesterDuplicateReceiptDrivesNothingTwice(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	logs := captureLogs(t)
	store := &fakeReceiptStore{}
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	ev := topologyEvent(7)
	fake.send(t, ev)
	select {
	case <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("the event never reached the bus")
	}
	waitFor(t, "the first receipt to project", func() bool { return len(store.projectedEvents()) == 1 })

	// Redelivery of the identical message, as a resume replay would send it.
	fake.send(t, ev)
	waitFor(t, `events_receipts_total{result="conflict"} == 1`, func() bool {
		return testutil.ToFloat64(m.EventsReceipts.WithLabelValues("conflict")) == 1
	})

	// Give any illegal second fanout time to land.
	time.Sleep(100 * time.Millisecond)
	select {
	case extra := <-msgs:
		t.Fatalf("duplicate receipt drove a second frame: %s", extra.Data)
	default:
	}
	if projected := len(store.projectedEvents()); projected != 1 {
		t.Errorf("projected %d rows for one event id, want 1", projected)
	}

	logs.mu.Lock()
	for _, rec := range logs.records {
		if rec.level >= slog.LevelWarn && strings.Contains(rec.msg, "conflict") {
			t.Errorf("conflict receipt logged at %v: %q — duplicate delivery is normal, not a warning", rec.level, rec.msg)
		}
	}
	logs.mu.Unlock()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestIngesterWithoutSinkIsUnchanged(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m) // no Option at all
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	fake.send(t, topologyEvent(9))
	var msg cache.Message
	select {
	case msg = <-msgs:
	case <-time.After(5 * time.Second):
		t.Fatal("the event never reached the bus")
	}
	if msg.Type != "event" {
		t.Errorf("frame type = %q, want plain event with no history product", msg.Type)
	}
	var live events.LiveEvent
	if err := json.Unmarshal(msg.Data, &live); err != nil {
		t.Fatal(err)
	}
	if live.Persisted != nil {
		t.Errorf("persisted = %v, want omitted when no database is configured", *live.Persisted)
	}

	for _, result := range []string{"ok", "conflict", "error"} {
		if got := testutil.ToFloat64(m.EventsPersisted.WithLabelValues(result)); got != 0 {
			t.Errorf("events_persisted_total{result=%s} = %v, want 0 with no receipt store configured", result, got)
		}
		if got := testutil.ToFloat64(m.EventsReceipts.WithLabelValues(result)); got != 0 {
			t.Errorf("events_receipts_total{result=%s} = %v, want 0 with no receipt store configured", result, got)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestIngesterProjectionIsDecoupledFromReceipt proves the pipeline is receive-first but not
// projection-blocking: a wedged projection keeps events 'received' (durable, ordered) while the
// stream keeps accepting them, and once projection resumes the backlog drains in seq order.
func TestIngesterProjectionIsDecoupledFromReceipt(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	store := &fakeReceiptStore{block: make(chan struct{})}
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	fake.send(t, topologyEvent(1))
	// The outbox is now blocked INSIDE projection for seq 1; wait until it owns the backlog.
	waitFor(t, "seq 1 to be received while projection is blocked", func() bool {
		return store.statusCounts()["received"] == 1
	})

	// The stream must still accept and durably receipt seq 2 instead of wedging on the projection.
	fake.send(t, topologyEvent(2))
	waitFor(t, "seq 2 to be received behind seq 1", func() bool {
		return store.statusCounts()["received"] == 2
	})
	if !ing.Healthy() {
		t.Fatal("the stream must stay established while projection is blocked")
	}
	select {
	case <-msgs:
		t.Fatal("no frame may be confirmed before the projection transaction commits")
	case <-time.After(100 * time.Millisecond):
	}

	close(store.block)

	var seqs []uint64
	for len(seqs) < 2 {
		select {
		case msg := <-msgs:
			var live events.LiveEvent
			if err := json.Unmarshal(msg.Data, &live); err != nil {
				t.Fatal(err)
			}
			seqs = append(seqs, live.Seq)
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d/2 frames confirmed after unblock", len(seqs))
		}
	}
	if seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("frames confirmed out of order: %v, want [1 2]", seqs)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// failingBus wraps a real bus and makes every Publish fail, while Subscribe keeps working.
type failingBus struct {
	inner    cache.Bus
	err      error
	attempts atomic.Int64
}

// CrossReplica mirrors the real Redis bus this double stands in for: the failure it injects is a
// publish failure, not an absence of reach.
func (b *failingBus) CrossReplica() bool { return true }

func (b *failingBus) Publish(context.Context, string, cache.Message) error {
	b.attempts.Add(1)
	return b.err
}

func (b *failingBus) Subscribe(topic string) (<-chan cache.Message, func()) {
	return b.inner.Subscribe(topic)
}

// TestIngesterBusErrorStillProjects is the other half of the outage pair: with bus.Publish
// failing on every call, all three events must still be receipted and PROJECTED into history
// (REST/Time Machine stay correct), counted EventsPersisted{result="ok"}, and the stream lives.
func TestIngesterBusErrorStillProjects(t *testing.T) {
	_, ctrl := startFakeController(t, "events")
	fake, addr := startFakeEventStream(t)

	bus := &failingBus{inner: cache.NewInProcessBus(), err: errors.New("valkey unreachable")}
	store := &fakeReceiptStore{}
	m := newTestMetrics()
	ing := events.NewIngester(ctrl, addr, bus, m, events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the ingester to establish a stream", ing.Healthy)

	for seq := uint64(1); seq <= 3; seq++ {
		fake.send(t, topologyEvent(seq))
	}

	waitFor(t, "all three events to project", func() bool { return len(store.projectedEvents()) == 3 })
	waitFor(t, `events_persisted_total{result="ok"} == 3`, func() bool {
		return testutil.ToFloat64(m.EventsPersisted.WithLabelValues("ok")) == 3
	})

	// Every projected event was still offered to the bus exactly once: history-first never means
	// the live feed gets skipped.
	if got := bus.attempts.Load(); got != 3 {
		t.Errorf("bus.Publish called %d times, want 3", got)
	}
	seen := make(map[uint64]bool, 3)
	for _, ev := range store.projectedEvents() {
		seen[ev.Seq] = true
	}
	for seq := uint64(1); seq <= 3; seq++ {
		if !seen[seq] {
			t.Errorf("event seq=%d never projected", seq)
		}
	}
	if !ing.Healthy() {
		t.Error("a bus failure must not tear the stream down")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestIngesterPersistsATopologyBaselineOnConnect: events only say what changed, so a console that
// starts recording next to a running fleet must also write down what was already there. Without the
// baseline the Time Machine folded the stand to 8 of its 11 nodes and lost the bare host entirely.
func TestIngesterPersistsATopologyBaselineOnConnect(t *testing.T) {
	api, ctrl := startFakeController(t, "events")
	snapshotAt := time.Date(2026, 9, 15, 8, 30, 0, 123456789, time.UTC)
	api.topology.Store(&controllerclient.Topology{
		Nodes: []controllerclient.Node{
			{Name: "worker2", Zone: "zone-a", Ready: true},
			{Name: "worker10", Zone: "zone-d", Ready: false},
		},
		Agents: []controllerclient.Agent{
			{ID: "worker2-agent", NodeName: "worker2", PodIP: "10.0.0.2", Zone: "zone-a", Capabilities: []string{"plane:pod"}},
			{ID: "edge-agent", NodeName: "edge-host-01", Zone: "office", Labels: map[string]string{"kconmon-ng.io/external": "true"}},
		},
		Timestamp: snapshotAt,
	})
	_, addr := startFakeEventStream(t)

	bus := cache.NewInProcessBus()
	msgs, unsubscribe := bus.Subscribe(liveBusTopic)
	defer unsubscribe()

	store := &fakeReceiptStore{}
	ing := events.NewIngester(ctrl, addr, bus, newTestMetrics(), events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the baseline to be projected", func() bool { return len(store.projectedBaselines()) == 1 })

	got := store.projectedBaselines()[0]
	if got.Type != events.TypeTopologyBaseline || got.Seq != 0 || got.Scope != "cluster" || got.Severity != events.SeverityInfo {
		t.Errorf("baseline = type %q seq %d scope %q severity %q, want topology_baseline/0/cluster/info",
			got.Type, got.Seq, got.Scope, got.Severity)
	}
	if !got.Timestamp.Equal(snapshotAt) {
		t.Errorf("baseline timestamp = %v, want the controller's snapshot time %v: events carry the controller clock too", got.Timestamp, snapshotAt)
	}
	var details struct {
		Nodes []struct {
			Name  string `json:"name"`
			Zone  string `json:"zone"`
			Ready bool   `json:"ready"`
		} `json:"nodes"`
		Agents []struct {
			ID       string            `json:"id"`
			NodeName string            `json:"nodeName"`
			Zone     string            `json:"zone"`
			Labels   map[string]string `json:"labels"`
		} `json:"agents"`
	}
	if err := json.Unmarshal(got.Details, &details); err != nil {
		t.Fatalf("baseline details are not JSON: %v (%s)", err, got.Details)
	}
	if len(details.Nodes) != 2 || details.Nodes[1].Name != "worker10" || details.Nodes[1].Ready {
		t.Errorf("baseline nodes = %+v, want worker2 and a not-ready worker10", details.Nodes)
	}
	if len(details.Agents) != 2 || details.Agents[1].NodeName != "edge-host-01" ||
		details.Agents[1].Labels["kconmon-ng.io/external"] != "true" {
		t.Errorf("baseline agents = %+v, want worker2-agent and edge-agent with its external label", details.Agents)
	}

	select {
	case msg := <-msgs:
		t.Errorf("the baseline reached the live bus (%s); it is history, not something that happened", msg.Data)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

// TestIngesterRefreshesTheBaselineWhileConnected: retention prunes by age, so a baseline written only
// when the stream came up would age out under a console that stays connected for longer than
// database.retentionDays, and the fold would silently go back to events alone.
func TestIngesterRefreshesTheBaselineWhileConnected(t *testing.T) {
	api, ctrl := startFakeController(t, "events")
	// Distinct per-fetch timestamps: the receipt id is deterministic for one stamped snapshot, so a
	// clock that ticks produces distinct baseline rows, like the controller's real snapshot clock.
	refreshTopo := func() {
		api.topology.Store(&controllerclient.Topology{
			Nodes:     []controllerclient.Node{{Name: "worker2", Zone: "zone-a", Ready: true}},
			Agents:    []controllerclient.Agent{{ID: "worker2-agent", NodeName: "worker2", Zone: "zone-a"}},
			Timestamp: time.Now(),
		})
	}
	refreshTopo()
	_, addr := startFakeEventStream(t)

	store := &fakeReceiptStore{}
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), newTestMetrics(), events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)
	ing.SetBaselineInterval(30 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	// Tick the snapshot clock faster than the ingester's baseline interval.
	tickerDone := make(chan struct{})
	go func() {
		t := time.NewTicker(15 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				refreshTopo()
			case <-tickerDone:
				return
			}
		}
	}()
	defer close(tickerDone)

	waitFor(t, "three baselines on one connection", func() bool { return len(store.projectedBaselines()) >= 3 })
	for _, got := range store.projectedBaselines() {
		if got.Type != events.TypeTopologyBaseline {
			t.Errorf("store got %q, want only topology_baseline rows: the stream sent nothing", got.Type)
		}
	}
	if n := api.topoCalls.Load(); n < 3 {
		t.Errorf("topology fetched %d times, want one per baseline", n)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
	settled := len(store.projectedBaselines())
	time.Sleep(100 * time.Millisecond)
	if len(store.projectedBaselines()) != settled {
		t.Error("a baseline was projected after Run returned")
	}
}

// A controller whose topology route fails costs the Time Machine its baseline, never the stream:
// events keep reaching the sink.
func TestIngesterBaselineFailureDoesNotStopEvents(t *testing.T) {
	_, ctrl := startFakeController(t, "events") // no topology set: GET /api/v1/topology answers 404
	fake, addr := startFakeEventStream(t)

	store := &fakeReceiptStore{}
	ing := events.NewIngester(ctrl, addr, cache.NewInProcessBus(), newTestMetrics(), events.WithReceiptStore(store))
	ing.SetConnectGrace(preconditionGrace)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); ing.Run(ctx) }()

	waitFor(t, "the stream to be healthy", ing.Healthy)
	fake.send(t, &pb.Event{Seq: 1, Timestamp: timestamppb.Now(), Payload: &pb.Event_TopologyChanged{
		TopologyChanged: &pb.TopologyChanged{Reason: "agent_registered", NodeName: "n", AgentId: "a"},
	}})
	waitFor(t, "the event to be projected", func() bool { return len(store.projectedEvents()) == 1 })
	if got := store.projectedEvents()[0].Type; got != events.TypeTopologyChanged {
		t.Errorf("store got %q, want topology_changed", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
