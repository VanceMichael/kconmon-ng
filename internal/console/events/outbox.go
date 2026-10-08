package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/EsDmitrii/kconmon-ng/internal/console/cache"
	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
)

// msgTypeEvent/msgTypeEventPending are the cache.Message.Type values the hub distinguishes. They
// MUST equal ws.TypeEvent/ws.TypeEventPending — the same cross-package equality busTopicLive keeps
// with ws.TopicLive: the events package cannot import ws.
const (
	msgTypeEvent        = "event"
	msgTypeEventPending = "event_pending"
)

// Kinds of inbox rows, identical strings to the store layer's closed set.
const (
	KindEvent    = "event"
	KindBaseline = "baseline"
)

// Outbox timing and bounds. The tick is the catch-up path (startup recovery, coalesced wakes);
// the wake signal is the hot path and is what makes projection effectively follow receipt at once.
const (
	outboxTick      = 500 * time.Millisecond
	projectBatch    = 64
	pendingCap      = 4096
	pendingSpoolTTL = 72 * time.Hour
)

// pendingSpoolPrefix owns a dedicated KV namespace: restart-durable spool entries. Nothing else
// may use this prefix, which is exactly why ListPrefix (kept off the cache.KV interface) is safe
// here.
const pendingSpoolPrefix = "events:pending:"

// Receipt is one controller event at the durable receipt gate.
type Receipt struct {
	ID        string
	Epoch     string
	Seq       uint64
	Timestamp time.Time
	Raw       []byte
}

// BaselineReceipt is one synthesized topology baseline, already in projected shape.
type BaselineReceipt struct {
	ID        string
	Epoch     string
	Timestamp time.Time
	Event     LiveEvent
}

// ProjectedEvent is what the outbox drained after commit: identity plus the projected event.
type ProjectedEvent struct {
	ID      string
	Epoch   string
	Seq     uint64
	Kind    string
	Live    LiveEvent
	History bool // whether the topology_events twin was newly inserted by this projection
}

// Projector turns a claimed row into its projected shape. prefilled carries the stored fields of
// a baseline row (kind == KindBaseline), which the projector returns verbatim.
type Projector func(kind string, raw []byte, prefilled LiveEvent) (LiveEvent, error)

// ReceiptStore is the durable half of the receive-first pipeline. Implemented by the store
// adapter in cmd/console (neither package may import the other).
type ReceiptStore interface {
	ReceiveEvent(ctx context.Context, r Receipt) (inserted bool, err error)
	ReceiveBaseline(ctx context.Context, b BaselineReceipt) (inserted bool, err error)
	ProjectPending(ctx context.Context, batch int, project Projector) (projected []ProjectedEvent, dead []DeadEvent, err error)
	Checkpoint(ctx context.Context, epoch string) (uint64, error)
	Counts(ctx context.Context) (received, dead int64, err error)
}

// DeadEvent mirrors the store's terminal-state diagnostic.
type DeadEvent struct {
	ID     string
	Seq    uint64
	Reason string
}

// SpoolKV is the optional restart-durable pending spool; both cache KV implementations satisfy
// it via their ListPrefix method (deliberately kept off the cache.KV interface itself).
type SpoolKV interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	ListPrefix(ctx context.Context, prefix string) ([]string, error)
}

// pendingReceipt is the spooled JSON of one event whose durable receipt has not succeeded yet.
type pendingReceipt struct {
	ID        string    `json:"id"`
	Epoch     string    `json:"epoch"`
	Seq       uint64    `json:"seq"`
	Timestamp time.Time `json:"timestamp"`
	Raw       []byte    `json:"raw"`
}

// outbox is the receive-first state machine: receipt gate → ordered projection → one fanout.
type outbox struct {
	store ReceiptStore
	bus   cache.Bus
	kv    SpoolKV
	m     *metrics.Metrics

	wake chan struct{}

	project  Projector
	tick     time.Duration
	spoolCap int

	mu      sync.Mutex
	pending map[string]*pendingReceipt // unconfirmed receipts, keyed by event id
}

func newOutbox(store ReceiptStore, bus cache.Bus, kv SpoolKV, m *metrics.Metrics) *outbox {
	return &outbox{
		store:    store,
		bus:      bus,
		kv:       kv,
		m:        m,
		wake:     make(chan struct{}, 1),
		project:  defaultProjector,
		pending:  make(map[string]*pendingReceipt),
		tick:     outboxTick,
		spoolCap: pendingCap,
	}
}

// defaultProjector is the events package's own projection: baselines pass through, events are
// re-projected from their raw wire bytes.
//nolint:gocritic // prefilled is a value on purpose: the projector never hands back a pointer to
// store-owned data; the projected LiveEvent is a fresh value that gets marshalled.
func defaultProjector(kind string, raw []byte, prefilled LiveEvent) (LiveEvent, error) {
	if kind == KindBaseline {
		return prefilled, nil
	}
	return ProjectRaw(raw)
}

// signal wakes the drain loop; non-blocking and coalescing, like the pushers' nudge channel.
func (o *outbox) signal() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// run owns the outbox lifetime: recover anything a previous process spooled, then retry receipts
// and project committed rows until ctx ends.
func (o *outbox) run(ctx context.Context) {
	o.recoverSpool(ctx)
	ticker := time.NewTicker(o.tick)
	defer ticker.Stop()
	for {
		o.cycle(ctx)
		select {
		case <-ctx.Done():
			return
		case <-o.wake:
		case <-ticker.C:
		}
	}
}

// cycle is one drain pass: spooled receipts first (they must become rows before projection can
// see them), then one projection batch, then the diagnostic gauges.
func (o *outbox) cycle(ctx context.Context) {
	o.retryPending(ctx)
	o.drainProjected(ctx)
	o.observeCounts(ctx)
}

// receive puts one stream event behind the durable gate. The confirmed fanout happens later, in
// drainProjected; when the gate itself is unreachable the event is published UNCONFIRMED and
// spooled for catch-up.
//nolint:gocritic // Receipt is the package's seam value; copied on purpose into the store layer.
func (o *outbox) receive(ctx context.Context, ev Receipt) {
	inserted, err := o.store.ReceiveEvent(ctx, ev)
	switch {
	case err == nil && inserted:
		o.m.EventsReceipts.WithLabelValues("ok").Inc()
		o.signal()
	case err == nil:
		// Duplicate delivery: reconnect replay, a second stream, or another replica. The existing
		// row drives (or already drove) everything exactly once, so this call drives nothing more.
		o.m.EventsReceipts.WithLabelValues("conflict").Inc()
		// With an in-process bus the winner's post-commit publish cannot reach THIS replica's hub;
		// try to claim the row locally so its frame still goes out here. A cross-replica bus needs
		// no help: the winner fans out to every replica.
		if !o.bus.CrossReplica() {
			o.drainProjected(ctx)
		}
	default:
		o.m.EventsReceipts.WithLabelValues("error").Inc()
		o.handleUnreachable(ctx, ev, err)
	}
}

// receiveBaseline receipts a synthesized baseline the same way; baselines never fan out live.
//nolint:gocritic // BaselineReceipt is a value across the seam (same reason as receive).
func (o *outbox) receiveBaseline(ctx context.Context, b BaselineReceipt) {
	inserted, err := o.store.ReceiveBaseline(ctx, b)
	switch {
	case err == nil && inserted:
		o.m.EventsReceipts.WithLabelValues("ok").Inc()
		o.signal()
	case err == nil:
		o.m.EventsReceipts.WithLabelValues("conflict").Inc()
	default:
		// A missed baseline under an outage is replaced at the next stream establishment (every
		// attempt records one): spooling synthesized snapshots would only multiply fold seeds.
		o.m.EventsReceipts.WithLabelValues("error").Inc()
		slog.Warn("failed to receipt a topology baseline; the next stream establishment writes a fresh one",
			"error", err)
	}
}

// checkpoint returns the durable resume cursor for epoch; 0 when the store cannot answer (a fresh
// process or the database being down) — the stream then replays nothing and records a baseline.
func (o *outbox) checkpoint(ctx context.Context, epoch string) uint64 {
	if epoch == "" {
		return 0
	}
	seq, err := o.store.Checkpoint(ctx, epoch)
	if err != nil {
		slog.Warn("could not read the event checkpoint; reconnecting without a resume cursor",
			"epoch", epoch, "error", err)
		return 0
	}
	return seq
}

// handleUnreachable retains the event for later durable receipt and, when it is projectable,
// publishes it as explicitly UNCONFIRMED so the live feed can show it without pretending the
// frame is complete history. A full spool is the one bounded-loss path.
//nolint:gocritic // Receipt is a value across the seam (same reason as receive).
func (o *outbox) handleUnreachable(ctx context.Context, ev Receipt, cause error) {
	slog.Warn("event history unreachable; showing the event live as unconfirmed and retaining it for catch-up",
		"id", ev.ID, "seq", ev.Seq, "error", cause)

	if !o.spool(ev) {
		o.m.EventsUnconfirmedDropped.WithLabelValues().Inc()
		o.setUnconfirmedGauge()
		slog.Error("unconfirmed event spool is full; dropping event until storage recovers",
			"id", ev.ID, "seq", ev.Seq, "cap", o.spoolCap)
		return
	}
	o.setUnconfirmedGauge()

	// Best-effort honest frame: project locally; a payload that cannot project is retained in the
	// spool anyway and will reach its dead terminal state after the database returns.
	live, err := ProjectRaw(ev.Raw)
	if err != nil {
		slog.Warn("unconfirmed event cannot be projected; retained for later diagnosis, not shown",
			"id", ev.ID, "seq", ev.Seq, "error", err)
		return
	}
	notPersisted := false
	live.Persisted = &notPersisted
	live.Epoch = ev.Epoch
	o.publishFrame(ctx, live, msgTypeEventPending)
}

// drainProjected commits one projection batch and fans out every newly projected EVENT after the
// commit; baselines land in history only (they are fold seeds, never live rows). Transient errors
// leave everything received for the next pass, in (epoch, seq) order.
func (o *outbox) drainProjected(ctx context.Context) {
	projected, dead, err := o.store.ProjectPending(ctx, projectBatch, o.project)
	if err != nil {
		slog.Warn("event outbox projection pass failed; received events stay queued and retry in order",
			"error", err)
		return
	}
	for _, d := range dead {
		o.m.EventsDead.WithLabelValues().Inc()
		// WARN, never ERROR: the pipeline is healthy; one payload is not, and it now has a
		// terminal, queryable state instead of an infinite retry.
		slog.Warn("controller event entered the dead terminal state; it will not reach history and no longer blocks later events",
			"id", d.ID, "seq", d.Seq, "reason", d.Reason)
	}
	for i := range projected {
		p := &projected[i]
		if p.Kind == KindBaseline {
			continue
		}
		p.Live.ID = p.ID
		p.Live.Epoch = p.Epoch
		p.Live.Seq = p.Seq
		persisted := true
		p.Live.Persisted = &persisted
		result := "ok"
		if !p.History {
			result = "conflict"
		}
		o.m.EventsPersisted.WithLabelValues(result).Inc()
		o.publishFrame(ctx, p.Live, msgTypeEvent)
	}
	if len(projected) > 0 || len(dead) > 0 {
		o.signal() // another batch may be waiting behind this one
	}
}

// publishFrame publishes one live frame. A bus failure loses the realtime copy only; history is
// already committed and REST/Time Machine remain authoritative.
//nolint:gocritic // LiveEvent is marshalled once here; passing a value keeps ownership local.
func (o *outbox) publishFrame(ctx context.Context, live LiveEvent, msgType string) {
	data, err := json.Marshal(live)
	if err != nil {
		slog.Warn("outbox: dropping live frame, marshal failed", "id", live.ID, "error", err)
		return
	}
	if err := o.bus.Publish(ctx, busTopicLive, cache.Message{Type: msgType, Data: data}); err != nil {
		slog.Warn("outbox: publishing live frame to the bus failed", "id", live.ID, "error", err)
	}
}

// retryPending pushes spooled, still-unconfirmed receipts into the durable gate in (epoch, seq)
// order. The first failure stops the pass: the database is still down, and ordering matters more
// than partial catch-up.
func (o *outbox) retryPending(ctx context.Context) {
	for _, p := range o.snapshotPending() {
		inserted, err := o.store.ReceiveEvent(ctx, Receipt{
			ID: p.ID, Epoch: p.Epoch, Seq: p.Seq, Timestamp: p.Timestamp, Raw: p.Raw,
		})
		if err != nil {
			return
		}
		o.unspool(p.ID)
		o.setUnconfirmedGauge()
		if inserted {
			o.m.EventsReceipts.WithLabelValues("ok").Inc()
		} else {
			o.m.EventsReceipts.WithLabelValues("conflict").Inc()
		}
	}
}

// spool retains an unconfirmed event in memory and, when available, in the restart-durable KV.
// false means the bounded memory queue is full; the event cannot be promised catch-up and is
// dropped instead of displacing an older one (seq order must survive).
//nolint:gocritic // Receipt is a value across the seam (same reason as receive).
func (o *outbox) spool(ev Receipt) bool {
	p := &pendingReceipt{ID: ev.ID, Epoch: ev.Epoch, Seq: ev.Seq, Timestamp: ev.Timestamp, Raw: ev.Raw}

	o.mu.Lock()
	if len(o.pending) >= o.spoolCap {
		o.mu.Unlock()
		return false
	}
	o.pending[ev.ID] = p
	o.mu.Unlock()

	if o.kv != nil {
		if data, err := json.Marshal(p); err == nil {
			if err := o.kv.Set(context.Background(), pendingSpoolPrefix+ev.ID, data, pendingSpoolTTL); err != nil {
				// The in-memory copy still covers recovery within this process; the failure only
				// costs the cross-restart copy.
				slog.Warn("could not write the restart-durable event spool; in-memory catch-up still active",
					"id", ev.ID, "error", err)
			}
		}
	}
	return true
}

// unspool removes one caught-up event from memory and the durable spool.
func (o *outbox) unspool(id string) {
	o.mu.Lock()
	delete(o.pending, id)
	o.mu.Unlock()
	if o.kv != nil {
		if err := o.kv.Delete(context.Background(), pendingSpoolPrefix+id); err != nil {
			slog.Warn("could not delete a caught-up event from the durable spool", "id", id, "error", err)
		}
	}
}

// snapshotPending returns the in-memory spool in (epoch, seq) order.
func (o *outbox) snapshotPending() []*pendingReceipt {
	o.mu.Lock()
	out := make([]*pendingReceipt, 0, len(o.pending))
	for _, p := range o.pending {
		out = append(out, p)
	}
	o.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].Epoch != out[j].Epoch {
			return out[i].Epoch < out[j].Epoch
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// pendingCount reports the in-memory unconfirmed backlog size.
func (o *outbox) pendingCount() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.pending)
}

func (o *outbox) setUnconfirmedGauge() {
	o.m.EventsUnconfirmed.WithLabelValues().Set(float64(o.pendingCount()))
}

// recoverSpool loads entries a previous process (or another replica that is gone) left in the
// durable spool; absent KV it is a no-op. Entries that were never in THIS process's memory are
// adopted directly — their receipts are retried on the first cycle.
func (o *outbox) recoverSpool(ctx context.Context) {
	if o.kv == nil {
		return
	}
	keys, err := o.kv.ListPrefix(ctx, pendingSpoolPrefix)
	if err != nil {
		slog.Warn("could not list the restart-durable event spool; proceeding with an empty backlog",
			"error", err)
		return
	}
	recovered := 0
	for _, key := range keys {
		data, ok, err := o.kv.Get(ctx, key)
		if err != nil || !ok {
			continue
		}
		var p pendingReceipt
		if err := json.Unmarshal(data, &p); err != nil || p.ID == "" {
			// Corrupt spool entry: it can never receipt; drop it rather than retrying forever.
			slog.Warn("dropping an unparseable entry from the durable event spool", "key", key, "error", err)
			_ = o.kv.Delete(ctx, key)
			continue
		}
		o.mu.Lock()
		if _, exists := o.pending[p.ID]; !exists && len(o.pending) < o.spoolCap {
			o.pending[p.ID] = &p
			recovered++
		}
		o.mu.Unlock()
	}
	if recovered > 0 {
		slog.Info("recovered unconfirmed controller events from the durable spool; catching up in order",
			"events", recovered)
		o.setUnconfirmedGauge()
	}
}

// observeCounts refreshes the projection backlog/dead gauges from the durable log.
func (o *outbox) observeCounts(ctx context.Context) {
	received, dead, err := o.store.Counts(ctx)
	if err != nil {
		return
	}
	o.m.EventsPending.WithLabelValues().Set(float64(received))
	o.m.EventsDeadRows.WithLabelValues().Set(float64(dead))
}
