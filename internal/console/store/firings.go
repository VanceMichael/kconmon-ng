package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/EsDmitrii/kconmon-ng/internal/console/store/gen"
)

// ErrClaimNotHeld is returned by CreateRunForFiring when the firing is no longer 'claimed' at the
// moment the run-creation transaction links it -- its lease expired and a recovery tick released
// it. The run INSERT is rolled back in that case, so the caller's due point has either one linked
// run or none, and retrying (a new claim) is the only path.
var ErrClaimNotHeld = errors.New("store: firing claim is no longer held")

// Firing status values -- the closed set migration 00020's status comment names.
const (
	FiringClaimed  = "claimed"
	FiringRunning  = "running"
	FiringSettled  = "settled"
	FiringReleased = "released"
)

// Firing is one persisted schedule_firings row: the durable record of a single due point's
// claim-run-advance process.
type Firing struct {
	ID         string
	ScheduleID string
	DueAt      time.Time
	// RunID is "" until the run+link transaction commits; once set, the due point has fired.
	RunID          string
	Status         string
	ClaimedBy      string
	ClaimedAt      time.Time
	LeaseExpiresAt time.Time
	NextFireAt     *time.Time
	LastError      string
	UpdatedAt      time.Time
}

// FiringInput is CreateFiring's write payload. The id and the 'claimed' status are assigned by the
// store.
type FiringInput struct {
	ScheduleID string
	DueAt      time.Time
	ClaimedBy  string
	// LeaseExpiresAt is the absolute instant the claim is owned until; the caller (the scheduler
	// tick) computes it from its clock plus the claim lease. It must cover the slowest legitimate
	// run-start and bounds how long a dead claimer blocks takeover.
	LeaseExpiresAt time.Time
	// NextFireAt is the cadence advance planned at claim (interval: due point + interval;
	// once: nil), stored so recovery is byte-stable.
	NextFireAt *time.Time
}

func firingFromRow(f *gen.ScheduleFiring) Firing {
	return Firing{
		ID:             formatUUID(f.ID),
		ScheduleID:     formatUUID(f.ScheduleID),
		DueAt:          f.DueAt,
		RunID:          optionalUUIDString(f.RunID),
		Status:         f.Status,
		ClaimedBy:      f.ClaimedBy,
		ClaimedAt:      f.ClaimedAt,
		LeaseExpiresAt: f.LeaseExpiresAt,
		NextFireAt:     nullTime(f.NextFireAt),
		LastError:      f.LastError,
		UpdatedAt:      f.UpdatedAt,
	}
}

// CreateFiring persists a new 'claimed' firing. A due point already claimed, running or settled
// makes this return ErrAlreadyExists (the partial unique index schedule_firings_due_point_uniq).
func (db *DB) CreateFiring(ctx context.Context, in FiringInput) (f Firing, err error) { //nolint:gocritic // hugeParam: FiringInput mirrors the other write-payload structs (TargetInput etc.)
	sid, err := parseUUID(in.ScheduleID)
	if err != nil {
		return Firing{}, fmt.Errorf("store: create firing: %w", err)
	}
	id, err := parseUUID(uuid.NewString())
	if err != nil {
		return Firing{}, fmt.Errorf("store: create firing: %w", err)
	}
	start := time.Now()
	row, err := gen.New(db.pool).CreateFiring(ctx, gen.CreateFiringParams{
		ID:             id,
		ScheduleID:     sid,
		DueAt:          in.DueAt,
		ClaimedBy:      in.ClaimedBy,
		LeaseExpiresAt: in.LeaseExpiresAt,
		NextFireAt:     timestamptzFromPtr(in.NextFireAt),
	})
	db.observe(queryCreateFiring, start, queryResult(err))
	if err != nil {
		return Firing{}, fmt.Errorf("store: create firing: %w", wrapUniqueViolation(wrapForeignKeyViolation(err, ErrNotFound)))
	}
	return firingFromRow(&row), nil
}

// GetFiringForDuePoint returns the current non-released firing for (scheduleID, dueAt);
// ErrNotFound when there is none.
func (db *DB) GetFiringForDuePoint(ctx context.Context, scheduleID string, dueAt time.Time) (Firing, error) {
	sid, err := parseUUID(scheduleID)
	if err != nil {
		return Firing{}, fmt.Errorf("store: get firing: %w", err)
	}
	start := time.Now()
	row, err := gen.New(db.pool).GetFiringForDuePoint(ctx, gen.GetFiringForDuePointParams{
		ScheduleID: sid, DueAt: dueAt,
	})
	db.observe(queryGetFiringForDuePoint, start, queryResult(err))
	if err != nil {
		return Firing{}, fmt.Errorf("store: get firing: %w", wrapNoRows(err))
	}
	return firingFromRow(&row), nil
}

// ReleaseFiring marks a claim that produced no run as released; idempotent -- a row that is not
// 'claimed' (already released, or linked/settled by another path) is the desired end state of a
// release and reports no error.
func (db *DB) ReleaseFiring(ctx context.Context, id string) error {
	fid, err := parseUUID(id)
	if err != nil {
		return fmt.Errorf("store: release firing: %w", err)
	}
	start := time.Now()
	_, err = gen.New(db.pool).ReleaseFiring(ctx, fid)
	db.observe(queryReleaseFiring, start, queryResult(err))
	if err != nil {
		return fmt.Errorf("store: release firing: %w", err)
	}
	return nil
}

// SettleFiring marks a firing settled once its schedule row has been advanced; idempotent in the
// same way ReleaseFiring is -- a non-released row that already settled reports no error. A released
// row (which must never be settled) is an ErrWrongState-equivalent signal.
func (db *DB) SettleFiring(ctx context.Context, id string) error {
	fid, err := parseUUID(id)
	if err != nil {
		return fmt.Errorf("store: settle firing: %w", err)
	}
	start := time.Now()
	rows, err := gen.New(db.pool).SettleFiring(ctx, fid)
	db.observe(querySettleFiring, start, queryResult(err))
	if err != nil {
		return fmt.Errorf("store: settle firing: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("store: settle firing: %w: firing %s is released", ErrWrongState, id)
	}
	return nil
}

// ListStaleClaims returns claims past their lease; limit 0 uses the default of 100, any other
// value is clamped into [1,500].
func (db *DB) ListStaleClaims(ctx context.Context, now time.Time, limit int) ([]Firing, error) {
	start := time.Now()
	rows, err := gen.New(db.pool).ListStaleClaims(ctx, gen.ListStaleClaimsParams{
		LeaseExpiresAt: now,
		Limit:          int32(clampLimit(limit)), //nolint:gosec // clampLimit bounds this to [1,500]
	})
	db.observe(queryListStaleClaims, start, queryResult(err))
	if err != nil {
		return nil, fmt.Errorf("store: list stale claims: %w", err)
	}
	firings := make([]Firing, len(rows))
	for i := range rows {
		firings[i] = firingFromRow(&rows[i])
	}
	return firings, nil
}

// ListRunningFirings returns firings whose run is linked but whose schedule row may not have been
// advanced; limit handling matches ListStaleClaims.
func (db *DB) ListRunningFirings(ctx context.Context, limit int) ([]Firing, error) {
	start := time.Now()
	rows, err := gen.New(db.pool).ListRunningFirings(ctx, int32(clampLimit(limit))) //nolint:gosec // clampLimit bounds this to [1,500]
	db.observe(queryListRunningFirings, start, queryResult(err))
	if err != nil {
		return nil, fmt.Errorf("store: list running firings: %w", err)
	}
	firings := make([]Firing, len(rows))
	for i := range rows {
		firings[i] = firingFromRow(&rows[i])
	}
	return firings, nil
}

// CompleteOnceSchedule performs the kind='once' success retirement: enabled=false, next_fire_at
// NULL, last_fired_at stamped. ErrNotFound when id names no schedule.
func (db *DB) CompleteOnceSchedule(ctx context.Context, id string, firedAt time.Time) error {
	sid, err := parseUUID(id)
	if err != nil {
		return fmt.Errorf("store: complete once schedule: %w", err)
	}
	start := time.Now()
	rows, err := gen.New(db.pool).CompleteOnceSchedule(ctx, gen.CompleteOnceScheduleParams{
		ID:          sid,
		LastFiredAt: pgtype.Timestamptz{Time: firedAt, Valid: true},
	})
	db.observe(queryCompleteOnceSchedule, start, queryResult(err))
	if err != nil {
		return fmt.Errorf("store: complete once schedule: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("store: complete once schedule: %w", ErrNotFound)
	}
	return nil
}

// CreateRunForFiring is the scheduler's run-creation primitive. It inserts check_runs and links the
// firing in ONE TRANSACTION: a due point carries a run_id if and only if the run row exists, so a
// process death or a statement failure can never leave a run without the convergence fact nor a
// convergence fact without a run. The signatures mirror CreateRun's arguments plus firingID.
func (db *DB) CreateRunForFiring(
	ctx context.Context, firingID, id, checkType, plane string, spec json.RawMessage,
	initiatorKind, initiatorID string, pairTotal int32, deadlineAt time.Time,
) (_ Run, err error) {
	fid, err := parseUUID(firingID)
	if err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: %w", err)
	}
	rid, err := parseUUID(id)
	if err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: %w", err)
	}

	start := time.Now()
	defer func() { db.observe(queryCreateRun, start, queryResult(err)) }()

	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // harmless ErrTxClosed after Commit (pgx pattern)

	q := gen.New(tx)
	row, err := q.CreateRun(ctx, gen.CreateRunParams{
		ID:            rid,
		CheckType:     checkType,
		Plane:         plane,
		Spec:          spec,
		InitiatorKind: initiatorKind,
		InitiatorID:   initiatorID,
		PairTotal:     pairTotal,
		DeadlineAt:    pgtype.Timestamptz{Time: deadlineAt, Valid: !deadlineAt.IsZero()},
	})
	if err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: %w", wrapUniqueViolation(err))
	}

	linked, err := q.LinkFiringRun(ctx, gen.LinkFiringRunParams{ID: fid, RunID: rid})
	if err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: link: %w", err)
	}
	if linked == 0 {
		// The claim was released by a recovery tick after its lease expired. Roll the run back:
		// leaving it would recreate the exact orphan this ledger exists to prevent.
		return Run{}, fmt.Errorf("store: create run for firing: %w", ErrClaimNotHeld)
	}

	if err := tx.Commit(ctx); err != nil {
		return Run{}, fmt.Errorf("store: create run for firing: commit: %w", err)
	}
	return runFromRow(&row), nil
}
