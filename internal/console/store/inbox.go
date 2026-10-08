package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
	"github.com/EsDmitrii/kconmon-ng/internal/console/store/gen"
)

// event_inbox row statuses and kinds; the CHECK constraints in migration 000020 are the same
// closed sets.
const (
	InboxReceived  = "received"
	InboxProjected = "projected"
	InboxDead      = "dead"

	InboxKindEvent    = "event"
	InboxKindBaseline = "baseline"
)

// inboxQuery labels for StoreQueries/StoreQueryDuration; same closed-set convention as events.go.
const (
	queryInsertEventReceipt    = "InsertEventReceipt"
	queryInsertBaselineReceipt = "InsertBaselineReceipt"
	queryClaimDueEvents        = "ClaimDueEvents"
	queryMarkInboxProjected    = "MarkInboxProjected"
	queryMarkInboxDead         = "MarkInboxDead"
	queryInboxCheckpoint       = "InboxCheckpoint"
	queryCountInboxByStatus    = "CountInboxByStatus"
	queryInsertInboxHistory    = "InsertTopologyEvent"
)

// maxInboxErrorLen bounds last_error: a malformed payload's diagnostic string must not grow the
// row without limit; the event id, seq and error class survive truncation.
const maxInboxErrorLen = 2000

// ErrPermanentProjection marks a claimed inbox row that can NEVER project onto topology_events
// (unknown payload type, corrupt bytes): the row goes to the dead terminal state instead of being
// retried. Any other projection error is transient and retried from the queue head.
var ErrPermanentProjection = errors.New("permanent event projection failure")

// InboxReceipt is one raw controller event at the receipt gate.
type InboxReceipt struct {
	EventID   string
	Epoch     string
	Seq       int64
	EventTime time.Time
	Raw       []byte
}

// InboxBaseline is a synthesized topology_baseline, already in projected shape.
type InboxBaseline struct {
	EventID   string
	Epoch     string
	EventTime time.Time
	Record    EventRecord
}

// Projected is one row that completed the received → projected transition in this call.
type Projected struct {
	EventID string
	Epoch   string
	Seq     int64
	Kind    string
	Record  EventRecord
	// HistoryInserted reports whether the topology_events twin was newly added by THIS projection.
	// False is a natural-key conflict: a legacy identity row for the same (seq,time) already made
	// it into history. The inbox marker is still set, so the event drives on exactly once.
	HistoryInserted bool
}

// DeadEvent is one receipt that entered its terminal state in this call: identity, seq and the
// diagnostic reason — enough to log and alert without re-reading the row.
type DeadEvent struct {
	EventID string
	Seq     int64
	Reason  string
}

// ProjectionFunc turns one claimed row into its topology_events record. For a baseline row the
// prefilled record carries the values stored at receipt time. An error wrapping
// ErrPermanentProjection dead-letters the row; any other error aborts the whole batch so it is
// retried in order.
type ProjectionFunc func(kind string, raw []byte, prefilled EventRecord) (EventRecord, error)

// InboxStore is the durable receipt log + transactional outbox the events pipeline drives.
type InboxStore interface {
	// ReceiveEvent durably receipts one controller event; inserted=false means an identical
	// event_id already won the gate (reconnect replay, duplicate stream, another replica).
	ReceiveEvent(ctx context.Context, r InboxReceipt) (inserted bool, err error)
	// ReceiveBaseline receipts one synthesized baseline the same way.
	ReceiveBaseline(ctx context.Context, b InboxBaseline) (inserted bool, err error)
	// ProjectPending claims one oldest-first batch and, in ONE transaction per batch, inserts the
	// topology_events twin and marks each row projected (or dead for permanent failures). Fanout
	// happens only AFTER the returned commit, in the caller.
	ProjectPending(ctx context.Context, batch int, proj ProjectionFunc) (projected []Projected, dead []DeadEvent, err error)
	// Checkpoint is the highest durable event_seq for epoch, in any row state.
	Checkpoint(ctx context.Context, epoch string) (uint64, error)
	// InboxCounts reports rows currently 'received' and 'dead' — the backlog/terminal gauges.
	InboxCounts(ctx context.Context) (received int64, dead int64, err error)
}

type inboxStore struct {
	q *gen.Queries
	// pool starts the outbox transaction; q rides a connection taken from it.
	pool *pgxpool.Pool
	m    *metrics.Metrics
}

// NewInboxStore returns an InboxStore backed by db's pool. m must not be nil: every statement is
// metered.
func NewInboxStore(db *DB, m *metrics.Metrics) InboxStore {
	return &inboxStore{q: gen.New(db.pool), pool: db.pool, m: m}
}

//nolint:gocritic // InboxReceipt is the pinned seam type; value semantics match EventStore.
func (s *inboxStore) ReceiveEvent(ctx context.Context, r InboxReceipt) (bool, error) {
	start := time.Now()
	_, err := s.q.InsertEventReceipt(ctx, gen.InsertEventReceiptParams{
		EventID:   r.EventID,
		Epoch:     r.Epoch,
		EventSeq:  r.Seq,
		EventTime: r.EventTime,
		Raw:       r.Raw,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		s.observe(queryInsertEventReceipt, start, resultConflict)
		return false, nil
	case err != nil:
		s.observe(queryInsertEventReceipt, start, resultError)
		return false, fmt.Errorf("store: receive event: %w", err)
	}
	s.observe(queryInsertEventReceipt, start, resultOK)
	return true, nil
}

//nolint:gocritic // InboxBaseline is the pinned seam type; value semantics match EventStore.
func (s *inboxStore) ReceiveBaseline(ctx context.Context, b InboxBaseline) (bool, error) {
	start := time.Now()
	_, err := s.q.InsertBaselineReceipt(ctx, gen.InsertBaselineReceiptParams{
		EventID:   b.EventID,
		Epoch:     b.Epoch,
		EventSeq:  b.Record.EventSeq,
		EventTime: b.EventTime,
		Type:      pgText(b.Record.Type),
		Severity:  pgText(b.Record.Severity),
		Scope:     pgText(b.Record.Scope),
		Summary:   pgText(b.Record.Summary),
		Details:   b.Record.Details,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		s.observe(queryInsertBaselineReceipt, start, resultConflict)
		return false, nil
	case err != nil:
		s.observe(queryInsertBaselineReceipt, start, resultError)
		return false, fmt.Errorf("store: receive baseline: %w", err)
	}
	s.observe(queryInsertBaselineReceipt, start, resultOK)
	return true, nil
}

// pgText maps a non-empty string to a valid pgtype.Text; inbox projection columns are nullable
// only for unprojected 'event' rows, which never take this path.
func pgText(v string) pgtype.Text {
	return pgtype.Text{String: v, Valid: v != ""}
}

// ProjectPending drives one out batch. See InboxStore and ProjectionFunc for the contract.
func (s *inboxStore) ProjectPending(ctx context.Context, batch int, proj ProjectionFunc) ([]Projected, []DeadEvent, error) {
	if batch <= 0 {
		batch = defaultProjectBatch
	}
	start := time.Now()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		s.observe(queryClaimDueEvents, start, resultError)
		return nil, nil, fmt.Errorf("store: project pending: begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	q := s.q.WithTx(tx)

	rows, err := q.ClaimDueEvents(ctx, int32(batch)) //nolint:gosec // batch is a small internal bound
	if err != nil {
		s.observe(queryClaimDueEvents, start, resultError)
		return nil, nil, fmt.Errorf("store: project pending: claim: %w", err)
	}
	if len(rows) == 0 {
		s.observe(queryClaimDueEvents, start, resultOK)
		return nil, nil, nil
	}

	out := make([]Projected, 0, len(rows))
	dead := make([]DeadEvent, 0)
	for i := range rows {
		r := &rows[i]
		prefilled := EventRecord{
			EventSeq:  r.EventSeq,
			EventTime: r.EventTime,
			Type:      r.Type.String,
			Severity:  r.Severity.String,
			Scope:     r.Scope.String,
			Summary:   r.Summary.String,
			Details:   r.Details,
		}
		rec, perr := proj(r.Kind, r.Raw, prefilled)
		if perr != nil {
			if errors.Is(perr, ErrPermanentProjection) {
				reason := truncateInboxError(perr.Error())
				if derr := q.MarkInboxDead(ctx, gen.MarkInboxDeadParams{
					ID:        r.ID,
					LastError: reason,
				}); derr != nil {
					s.observe(queryMarkInboxDead, start, resultError)
					return nil, dead, fmt.Errorf("store: project pending: mark dead: %w", derr)
				}
				s.observe(queryMarkInboxDead, start, resultOK)
				dead = append(dead, DeadEvent{EventID: r.EventID, Seq: r.EventSeq, Reason: reason})
				continue
			}
			// Transient: roll the whole batch back and re-enter from this same head row, so later
			// events can never be projected ahead of an earlier one.
			s.observe(queryClaimDueEvents, start, resultError)
			return nil, dead, fmt.Errorf("store: project pending: project event %s seq %d: %w",
				r.EventID, r.EventSeq, perr)
		}

		insertStart := time.Now()
		inserted, ierr := q.InsertTopologyEvent(ctx, gen.InsertTopologyEventParams{
			EventSeq:  rec.EventSeq,
			EventTime: rec.EventTime,
			Type:      rec.Type,
			Severity:  rec.Severity,
			Scope:     rec.Scope,
			Summary:   rec.Summary,
			Details:   rec.Details,
		})
		if ierr != nil {
			s.observe(queryInsertInboxHistory, insertStart, resultError)
			return nil, dead, fmt.Errorf("store: project pending: history insert event %s: %w", r.EventID, ierr)
		}
		s.observe(queryInsertInboxHistory, insertStart, boolResult(inserted > 0))

		if err := q.MarkInboxProjected(ctx, r.ID); err != nil {
			s.observe(queryMarkInboxProjected, start, resultError)
			return nil, dead, fmt.Errorf("store: project pending: mark projected event %s: %w", r.EventID, err)
		}
		s.observe(queryMarkInboxProjected, start, resultOK)
		out = append(out, Projected{
			EventID:         r.EventID,
			Epoch:           r.Epoch,
			Seq:             r.EventSeq,
			Kind:            r.Kind,
			Record:          rec,
			HistoryInserted: inserted > 0,
		})
	}

	if err := tx.Commit(ctx); err != nil {
		s.observe(queryClaimDueEvents, start, resultError)
		return nil, dead, fmt.Errorf("store: project pending: commit: %w", err)
	}
	committed = true
	s.observe(queryClaimDueEvents, start, resultOK)
	return out, dead, nil
}

// defaultProjectBatch bounds one claim: enough to drain a burst in a few passes, small enough that
// one transaction never holds SKIP LOCKED rows against a slow writer for long.
const defaultProjectBatch = 64

func (s *inboxStore) Checkpoint(ctx context.Context, epoch string) (uint64, error) {
	start := time.Now()
	seq, err := s.q.InboxCheckpoint(ctx, epoch)
	if err != nil {
		s.observe(queryInboxCheckpoint, start, resultError)
		return 0, fmt.Errorf("store: inbox checkpoint: %w", err)
	}
	s.observe(queryInboxCheckpoint, start, resultOK)
	return uint64(seq), nil //nolint:gosec // COALESCE(MAX(bigint),0) is never negative
}

func (s *inboxStore) InboxCounts(ctx context.Context) (received, dead int64, err error) {
	received, err = s.q.CountInboxByStatus(ctx, InboxReceived)
	if err != nil {
		return 0, 0, fmt.Errorf("store: inbox counts: received: %w", err)
	}
	dead, err = s.q.CountInboxByStatus(ctx, InboxDead)
	if err != nil {
		return 0, 0, fmt.Errorf("store: inbox counts: dead: %w", err)
	}
	return received, dead, nil
}

// observe mirrors eventStore.observe: duration by query, call count by query × result.
func (s *inboxStore) observe(query string, start time.Time, result string) {
	s.m.StoreQueryDuration.WithLabelValues(query).Observe(time.Since(start).Seconds())
	s.m.StoreQueries.WithLabelValues(query, result).Inc()
}

func boolResult(inserted bool) string {
	if inserted {
		return resultOK
	}
	return resultConflict
}

func truncateInboxError(msg string) string {
	if len(msg) <= maxInboxErrorLen {
		return msg
	}
	return msg[:maxInboxErrorLen]
}
