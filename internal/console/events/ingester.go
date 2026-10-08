package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/EsDmitrii/kconmon-ng/api/proto"
	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/controllerclient"
	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"
)

// busTopicLive is the cache.Bus topic the live feed travels on; it MUST equal ws.TopicLive.
const busTopicLive = "live"

// capabilityEvents is the flag the controller must advertise on
// GET /api/v1/version before this console will dial its gRPC event stream.
const capabilityEvents = "events"

// Reconnect reason labels for metrics.IngesterReconnects. Bounded set.
const (
	reasonCapability = "capability"
	reasonDial       = "dial"
	reasonStream     = "stream"
)

// Backoff bounds, identical to the agent's WatchTasks loop
// (internal/agent/agent.go): 1s doubling to a 15s ceiling.
const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 15 * time.Second
)

// connectGrace is how long a stream must survive without the server hanging up before the ingester
// reports healthy on the strength of the connection alone (an event arriving sooner promotes it
// immediately).
const connectGrace = 2 * time.Second

// gRPC keepalive, identical to internal/agent/grpc_client.go's dial options —
// the console is a second long-lived streaming client against the same
// controller Service and has no reason to behave differently.
const (
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 5 * time.Second
)

// WatchEvents response-header names and values; they MUST equal the controller's constants of the
// same names. They travel in gRPC metadata, not the wire message, so an old controller simply
// omits them and the ingester falls back to its legacy identity path.
const (
	eventsEpochHeader  = "x-kconmon-events-epoch"
	eventsResumeHeader = "x-kconmon-resume"
	resumeGap          = "gap"
)

var (
	// errNoCapability is the "the controller is not offering events" case. It is
	// deliberately handled exactly like a failed dial: back off, retry.
	errNoCapability = errors.New(`controller does not advertise the "events" capability`)
	// errNoControllerClient guards the misconfiguration where a gRPC address is
	// set but controller.url is not, so there is nothing to precheck against.
	errNoControllerClient = errors.New("controller HTTP client is not configured, cannot run the capability precheck")
)

// Option configures an Ingester. Variadic, so existing call sites need no edit.
type Option func(*Ingester)

// WithReceiptStore turns on the receive-first pipeline: every event is durably receipted before
// it drives realtime/history/fold, with ordered catch-up across outages, restarts and replicas.
func WithReceiptStore(store ReceiptStore) Option {
	return func(i *Ingester) { i.receiptStore = store }
}

// WithPendingKV provides the optional restart-durable spool consulted while the database is
// unreachable. Without it, unconfirmed catch-up survives only the process lifetime.
func WithPendingKV(kv SpoolKV) Option {
	return func(i *Ingester) { i.pendingKV = kv }
}

// Ingester is one console replica's client for the controller's EventStream.WatchEvents stream.
type Ingester struct {
	ctrl     *controllerclient.Client
	grpcAddr string
	bus      cache.Bus
	metrics  *metrics.Metrics

	// receiptStore is nil unless WithReceiptStore was passed; when set, the outbox built in
	// NewIngester owns persistence and the confirmed fanout together.
	receiptStore ReceiptStore
	pendingKV    SpoolKV
	outbox       *outbox

	// epoch names the controller incarnation the CURRENT attempt streams from ("" for an old
	// controller); the resume checkpoint and baseline receipts are keyed by it.
	epoch atomic.Pointer[string]

	// connectGrace defaults to the connectGrace const. Overridable via
	// export_test.go so a test can stretch it far enough that a promotion can
	// only have come from a received event. Never changed after Run starts.
	connectGrace time.Duration

	// baselineInterval defaults to the baselineInterval const; export_test.go shortens it.
	baselineInterval time.Duration

	// initialBackoff and maxBackoff default to the consts of the same name; export_test.go shortens them.
	initialBackoff time.Duration
	maxBackoff     time.Duration

	connected atomic.Bool
}

// NewIngester returns an ingester for the controller at grpcAddr.
func NewIngester(ctrl *controllerclient.Client, grpcAddr string, bus cache.Bus, m *metrics.Metrics, opts ...Option) *Ingester {
	i := &Ingester{
		ctrl: ctrl, grpcAddr: grpcAddr, bus: bus, metrics: m, connectGrace: connectGrace, baselineInterval: baselineInterval,
		initialBackoff: initialBackoff, maxBackoff: maxBackoff,
	}
	for _, opt := range opts {
		opt(i)
	}
	if i.receiptStore != nil {
		i.outbox = newOutbox(i.receiptStore, bus, i.pendingKV, m)
	}
	return i
}

// currentEpoch returns the incarnation the current (or last) attempt identified, or "" before the
// first header / against an old controller.
func (i *Ingester) currentEpoch() string {
	if p := i.epoch.Load(); p != nil {
		return *p
	}
	return ""
}

// Healthy reports whether this replica currently holds a WatchEvents stream that has proven itself;
// it is the single input to the console's "events" capability flag.
func (i *Ingester) Healthy() bool { return i.connected.Load() }

// Run blocks until ctx is cancelled, reconnecting with the repo's standard 1s→15s doubling backoff
// (the shape in internal/agent/agent.go's WatchTasks loop).
func (i *Ingester) Run(ctx context.Context) {
	if i.grpcAddr == "" {
		slog.Info("realtime event ingestion disabled: no controller gRPC address configured")
		return
	}

	// The outbox outlives every individual stream attempt: receipts left 'received' by a crash,
	// or spooled while the database was down, are drained from here on startup regardless of when
	// the first WatchEvents attempt connects.
	if i.outbox != nil {
		go i.outbox.run(ctx)
	}

	backoff := i.initialBackoff
	for {
		reason, established, err := i.attempt(ctx)
		if ctx.Err() != nil {
			return
		}
		if established {
			// A stream that proved itself ends the outage the backoff was built up for.
			backoff = i.initialBackoff
		}

		i.metrics.IngesterReconnects.WithLabelValues(reason).Inc()
		switch {
		case errors.Is(err, errNoCapability):
			// Only THIS case is benign.
			slog.Info("controller is not offering realtime events yet, retrying",
				"error", err, "backoff", backoff)
		case reason == reasonCapability:
			slog.Warn("controller capability precheck failed, retrying",
				"reason", reason, "error", err, "backoff", backoff)
		default:
			slog.Warn("controller event stream disconnected, reconnecting",
				"reason", reason, "error", err, "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, i.maxBackoff)
	}
}

// attempt performs one whole cycle — capability precheck, dial, consume — and
// returns the metrics reason label to count, whether the stream reached the
// connected state, and the error that ended the attempt. It never returns a nil
// error: the loop above only stops on ctx.
func (i *Ingester) attempt(ctx context.Context) (reason string, established bool, err error) {
	if perr := i.precheck(ctx); perr != nil {
		return reasonCapability, false, perr
	}

	// Same dial options as internal/agent/grpc_client.go. grpc.NewClient is
	// lazy, so a wrong or unreachable address surfaces on the WatchEvents call
	// below rather than here.
	conn, err := grpc.NewClient(i.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                keepaliveTime,
			Timeout:             keepaliveTimeout,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return reasonDial, false, fmt.Errorf("dial controller %s: %w", i.grpcAddr, err)
	}
	defer func() { _ = conn.Close() }()

	// Resume from THIS incarnation's durable checkpoint when one exists. The controller only
	// buffers a bounded ring; a first-ever connection, an old controller (no epoch header) and an
	// unreadable checkpoint all send 0 and heal via the establishment baseline instead.
	knownEpoch := i.currentEpoch()
	var lastSeq uint64
	if i.outbox != nil {
		lastSeq = i.outbox.checkpoint(ctx, knownEpoch)
	}

	// A Go server-streaming client returns from WatchEvents BEFORE the server has accepted the stream.
	stream, err := pb.NewEventStreamClient(conn).WatchEvents(ctx,
		&pb.WatchEventsRequest{LastSeq: lastSeq})
	if err != nil {
		return reasonDial, false, fmt.Errorf("open WatchEvents stream on %s: %w", i.grpcAddr, err)
	}

	// Headers arrive before any data on the wire. A new controller flushes them at stream open;
	// an old controller sends none (the call returns only when its first event flushes the empty
	// set). Reading them on a separate goroutine keeps Recv — and the grace-based health promotion
	// — from blocking on a quiet stream; events and baselines pick the epoch up live.
	go i.watchStreamHeader(ctx, stream, knownEpoch, lastSeq)

	established, err = i.consume(ctx, stream)
	return reasonStream, established, err
}

// watchStreamHeader records the epoch/resume headers of one attempt as soon as they arrive.
func (i *Ingester) watchStreamHeader(ctx context.Context, stream pb.EventStream_WatchEventsClient, knownEpoch string, lastSeq uint64) {
	md, err := stream.Header()
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("could not read the WatchEvents response header; assuming a legacy controller", "error", err)
		}
		return
	}
	var epoch, resume string
	if v := md.Get(eventsEpochHeader); len(v) > 0 {
		epoch = v[0]
	}
	if v := md.Get(eventsResumeHeader); len(v) > 0 {
		resume = v[0]
	}
	i.epoch.Store(&epoch)
	if knownEpoch != "" && epoch != knownEpoch {
		slog.Info("controller event epoch changed; the stream is a new incarnation, resuming from its baseline",
			"old_epoch", knownEpoch, "new_epoch", epoch)
	}
	if resume == resumeGap {
		slog.Warn("controller could not cover the resume gap from its buffer; replayed events start mid-history, recording a fresh baseline",
			"epoch", epoch, "last_seq", lastSeq)
	}
}

// precheck is a: feature-detect before every dial attempt, including every reconnect.
func (i *Ingester) precheck(ctx context.Context) error {
	if i.ctrl == nil {
		return errNoControllerClient
	}
	v, err := i.ctrl.Version(ctx)
	if err != nil {
		return fmt.Errorf("controller capability probe: %w", err)
	}
	if !v.HasCapability(capabilityEvents) {
		return fmt.Errorf("%w (advertised: %v)", errNoCapability, v.Capabilities)
	}
	return nil
}

// consume runs the stream to its end; it does NOT report the ingester healthy just because the
// stream object exists.
func (i *Ingester) consume(ctx context.Context, stream pb.EventStream_WatchEventsClient) (bool, error) {
	gate := &connectGate{ing: i, established: make(chan struct{})}
	defer gate.finish()

	// Events only say what changed, so the Time Machine also needs what was already there when this
	// console started listening. Joined before returning: no baseline outlives its attempt.
	if i.outbox != nil && i.ctrl != nil {
		baselineCtx, stopBaselines := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Go(func() { i.recordBaselines(baselineCtx, gate.established) })
		defer func() {
			stopBaselines()
			wg.Wait()
		}()
	}

	grace := time.AfterFunc(i.connectGrace, gate.markConnected)
	defer grace.Stop()

	for {
		ev, err := stream.Recv()
		if err != nil {
			return gate.wasEstablished(), fmt.Errorf("receive event: %w", err)
		}
		// An event in hand is the strongest possible proof of a live stream, so
		// it promotes the ingester ahead of the grace period.
		gate.markConnected()
		i.publish(ctx, ev, i.currentEpoch())
	}
}

// connectGate owns the connected state of ONE attempt.
type connectGate struct {
	ing *Ingester

	// established is closed by the promotion, the one proof this attempt's stream is live.
	established chan struct{}

	mu       sync.Mutex
	finished bool // the attempt is over; no further promotion is permitted
	up       bool // this attempt promoted the ingester and owes a demotion
}

// markConnected promotes the ingester at most once per attempt, and never after finish.
func (g *connectGate) markConnected() {
	if !g.promote() {
		return
	}
	slog.Info("controller event stream established", "address", g.ing.grpcAddr)
}

// promote reports whether this call is the one that flipped the attempt to
// connected.
func (g *connectGate) promote() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished || g.up {
		return false
	}
	g.up = true
	g.ing.setConnected(true)
	close(g.established)
	return true
}

// wasEstablished reports whether this attempt was ever promoted.
func (g *connectGate) wasEstablished() bool {
	select {
	case <-g.established:
		return true
	default:
		return false
	}
}

// finish closes the attempt out, demoting the ingester if this attempt had
// promoted it. After it returns, Healthy() is false and no late timer can
// change that.
func (g *connectGate) finish() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.finished = true
	if g.up {
		g.up = false
		g.ing.setConnected(false)
	}
}

// setConnected keeps Healthy and the ingester_connected gauge in step; the gauge is written BEFORE
// the flag on purpose.
func (i *Ingester) setConnected(up bool) {
	value := 0.0
	if up {
		value = 1
	}
	i.metrics.IngesterConnected.WithLabelValues().Set(value)
	i.connected.Store(up)
}

// publish routes one received controller event. With the receive-first pipeline it is durably
// RECEIPTED first and the confirmed frame leaves the outbox only after the history transaction
// commits; an unreachable database produces an explicitly unconfirmed frame plus a retained
// spool entry. Without a database it behaves as it always has: one direct live frame, no
// persistence claim.
func (i *Ingester) publish(ctx context.Context, ev *pb.Event, epoch string) {
	live, projErr := ToLiveEvent(ev)

	if i.outbox == nil {
		if projErr != nil {
			slog.Warn("dropping controller event", "seq", ev.GetSeq(), "error", projErr)
			return
		}
		i.metrics.EventsReceived.WithLabelValues(live.Type).Inc()
		i.fanoutDirect(ctx, live)
		return
	}

	// Raw bytes first: even an event this build cannot project is receipted, so it can reach its
	// diagnosable dead terminal state instead of vanishing.
	raw, err := proto.Marshal(ev)
	if err != nil {
		slog.Warn("dropping controller event, marshal failed", "seq", ev.GetSeq(), "error", err)
		return
	}

	if projErr == nil {
		i.metrics.EventsReceived.WithLabelValues(live.Type).Inc()
	}
	ts := ev.GetTimestamp().AsTime().UTC().Truncate(time.Microsecond)
	i.outbox.receive(ctx, Receipt{
		ID:        EventID(ev),
		Epoch:     epoch,
		Seq:       ev.GetSeq(),
		Timestamp: ts,
		Raw:       raw,
	})
}

// fanoutDirect is the database-less path: publish the one live frame straight to the bus. A bus
// failure loses the realtime copy only, exactly as before the receipt pipeline existed.
//nolint:gocritic // the database-less path marshals the value once straight to the bus.
func (i *Ingester) fanoutDirect(ctx context.Context, live LiveEvent) {
	data, err := json.Marshal(live)
	if err != nil {
		slog.Warn("dropping controller event, marshal failed", "id", live.ID, "error", err)
		return
	}
	if err := i.bus.Publish(ctx, busTopicLive, cache.Message{Type: "event", Data: data}); err != nil {
		slog.Warn("publishing live event to the bus failed", "id", live.ID, "error", err)
	}
}
