// Package scheduler is the Console's cadence loop. The primitive, the per-attempt lock and the
// "not acquired is a silent skip" contract are deliberately identical; the durable claim ledger
// (schedule_firings) is what makes one due point's claim -> run -> advance process recoverable.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/EsDmitrii/kconmon-ng/internal/console/authz"
	"github.com/EsDmitrii/kconmon-ng/internal/console/checks"
	"github.com/EsDmitrii/kconmon-ng/internal/console/controllerclient"
	"github.com/EsDmitrii/kconmon-ng/internal/console/metrics"
	"github.com/EsDmitrii/kconmon-ng/internal/console/store"
)

// LockKey is the pg_try_advisory_lock key this loop serializes itself on across replicas; a
// collision would not corrupt anything, but it would make one of these silently starve.
const LockKey int64 = 2111970501

// dueLimit bounds how many due schedules one tick will look at; a tick holds the fleet-wide lock
// for as long as it runs.
const dueLimit = 100

// recoverLimit bounds how many ledger rows one recovery pass looks at per kind.
const recoverLimit = 100

/*
claimLease is how long one claim owns its due point without a heartbeat.

It must cover the slowest legitimate run-start (a topology round trip plus planning plus the run
transaction) and it is the outer bound on takeover: a claim whose owner died is reclaimed no later
than one lease after its death. The lease protects CORRECTNESS rather than liveness even when too
short -- StartForFiring links inside the run transaction against status='claimed', so a start that
finds the claim released rolls its run INSERT back and reports store.ErrClaimNotHeld instead of
leaving an unlinked run -- which is why the value can be generous without hesitation.
*/
const claimLease = 2 * time.Minute

/*
onceSkipRetry is how far forward a SKIPPED kind='once' schedule pushes its cadence.

A once-schedule has no next occurrence -- plannedNextFireAt returns nil for it -- so the skip path
used to leave its row alone entirely, on the reasoning that writing nil would clear next_fire_at and
retire the occurrence permanently. Leaving it alone retires the whole LOOP instead: ListDueSchedules
is `WHERE enabled AND next_fire_at <= now ORDER BY next_fire_at LIMIT 100`, and a row whose
next_fire_at is stuck in the past sorts ahead of every schedule that is merely due now. A skip reason
can outlive the tick that saw it -- a disabled definition stays disabled until someone re-enables it
-- so those rows accumulate at the head of the ordering, and at 100 of them nothing else in the fleet
ever fires again while the schedules themselves still read as enabled.

Pushing the cadence forward keeps the occurrence pending (next_fire_at stays non-NULL, so it comes
back when the block clears) and takes the row out of the due window in between, which is what the
ordering needs. The interval is minutes, not the tick's seconds: a skip that resolves does so on
human time -- someone re-enables a definition, or a long run finishes.
*/
const onceSkipRetry = time.Minute

// The schedule kinds this loop knows; 'cron' is a later milestone and would arrive here as an
// unrecognized kind -- see processClaim.
const (
	kindOnce       = "once"
	kindInterval   = "interval"
	kindContinuous = "continuous"
)

// SchedulerTicks result labels -- the closed set metrics.go documents.
const (
	tickOK        = "ok"
	tickNotLeader = "not-leader"
	tickError     = "error"
)

// SchedulerSkipped reason labels -- the closed set metrics.go documents.
const (
	skipOverrun  = "overrun"
	skipDisabled = "disabled"
)

// SchedulerFirings action labels -- the closed set metrics.go documents.
const (
	firingClaimed   = "claimed"   // a new claim row was inserted
	firingReclaimed = "reclaimed" // an expired claim with no run was released for another attempt
	firingConverged = "converged" // a linked run's unadvanced schedule row was finished post-crash
	firingConflict  = "conflict"  // lost the claim race to another replica
)

// initiatorKind is what a scheduled run records in check_runs.initiator_kind; it is deliberately
// not one of authz's own SubjectKind values.
const initiatorKind = "scheduler"

// Locker is the cross-replica mutual-exclusion seam, satisfied by *store.DB.
// See store.DB.WithAdvisoryLock for the (false, nil) = "someone else has it"
// contract this package depends on.
type Locker interface {
	WithAdvisoryLock(ctx context.Context, key int64, fn func(context.Context) error) (bool, error)
}

// Store is everything the loop reads and writes in PostgreSQL. A narrow local interface over
// *store.DB for the same reason httpapi declares its own: the unit tests drive the whole tick
// against a fake with no database at all.
type Store interface {
	ListDueSchedules(ctx context.Context, due time.Time, limit int) ([]store.Schedule, error)
	GetSchedule(ctx context.Context, id string) (store.Schedule, error)
	MarkScheduleFired(ctx context.Context, id string, firedAt time.Time, nextFireAt *time.Time, lastError string) error
	// MarkScheduleSkipped advances the cadence WITHOUT stamping last_fired_at, for an occurrence
	// that produced no run.
	MarkScheduleSkipped(ctx context.Context, id string, nextFireAt *time.Time) error
	GetDefinition(ctx context.Context, id string) (store.Definition, error)
	GetTarget(ctx context.Context, id string) (store.Target, error)

	// The durable firing ledger (migration 00020).
	CreateFiring(ctx context.Context, in store.FiringInput) (store.Firing, error)
	GetFiringForDuePoint(ctx context.Context, scheduleID string, dueAt time.Time) (store.Firing, error)
	ReleaseFiring(ctx context.Context, id string) error
	SettleFiring(ctx context.Context, id string) error
	ListStaleClaims(ctx context.Context, now time.Time, limit int) ([]store.Firing, error)
	ListRunningFirings(ctx context.Context, limit int) ([]store.Firing, error)
	// CompleteOnceSchedule disables a kind='once' schedule and clears next_fire_at; it is called
	// only after the run+link transaction confirmed the run was created.
	CompleteOnceSchedule(ctx context.Context, id string, firedAt time.Time) error
}

// Runner is the diagnostics runner seam.
type Runner interface {
	Start(ctx context.Context, spec checks.Spec, initiator authz.Subject) (string, error) //nolint:gocritic // Spec is a value type by design (checks.Runner.Start)
	// StartForFiring is Start plus the firing link in the SAME transaction as the run INSERT -- the
	// scheduler's only run-creation path.
	StartForFiring(ctx context.Context, spec checks.Spec, initiator authz.Subject, firingID string) (string, error) //nolint:gocritic // mirrors Start
	// ActiveForInitiator is the overrun guard's replica-independent question; see previousRunActive.
	ActiveForInitiator(ctx context.Context, initiatorKind, initiatorID string) (bool, error)
	Get(ctx context.Context, runID string) (checks.Run, error)
	ReapStuckRuns(ctx context.Context, limit int32) (int64, error)
}

// TopologySource is the live node/agent snapshot a definition's source_selection is resolved
// against.
type TopologySource interface {
	Topology(ctx context.Context) (*controllerclient.Topology, error)
}

var (
	_ Locker         = (*store.DB)(nil)
	_ Store          = (*store.DB)(nil)
	_ Runner         = (*checks.Runner)(nil)
	_ TopologySource = (*controllerclient.Client)(nil)
)

// Deps is everything New needs; every field is REQUIRED except Topology: without a topology source
// the loop can still fire every definition whose selection is "all" or "per-zone" (checks.Plan
// expands an empty source list against the current topology on its own), and refuses only the
// "one-per-zone" ones it cannot narrow honestly.
type Deps struct {
	Lock     Locker
	Store    Store
	Runner   Runner
	Topology TopologySource
	Metrics  *metrics.Metrics
	// Interval is the tick cadence (console.scheduler.tickInterval).
	Interval time.Duration
}

// Scheduler is the loop itself. One per process; Run owns it.
type Scheduler struct {
	lock     Locker
	store    Store
	runner   Runner
	topology TopologySource
	m        *metrics.Metrics
	interval time.Duration

	// now is time.Now indirected for the tests that assert next-fire
	// arithmetic, which cannot be done against a real clock without either
	// sleeping or asserting a range.
	now func() time.Time

	// ownerID is this process's identity on a claim's claimed_by; a new value per process lets the
	// lease and the logs name the replica a point belongs to.
	ownerID string

	// lastRun maps a schedule id to the id of the run this replica most recently started for it;
	// owned by the single goroutine inside Run, so it needs no mutex.
	lastRun map[string]string
}

// New returns a Scheduler. Run is what starts it.
func New(d Deps) *Scheduler { //nolint:gocritic // hugeParam: Deps mirrors httpapi.Deps' value semantics -- a named-field composition root, built once at boot
	return &Scheduler{
		lock:     d.Lock,
		store:    d.Store,
		runner:   d.Runner,
		topology: d.Topology,
		m:        d.Metrics,
		interval: d.Interval,
		now:      func() time.Time { return time.Now().UTC() },
		ownerID:  newOwnerID(),
		lastRun:  make(map[string]string),
	}
}

// newOwnerID builds this process's claim identity: the hostname when one is available plus a random
// suffix so two pods on one host never share a name.
func newOwnerID() string {
	hn, err := os.Hostname()
	if err != nil || hn == "" {
		hn = "console"
	}
	return hn + "-" + uuid.NewString()[:8]
}

// Run ticks every Interval until ctx is cancelled, and returns promptly when it is; the cadence
// here is SECONDS: a replica that just started is at most one tick behind.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Tick(ctx)
		}
	}
}

// Tick runs exactly one attempt, wrapped in the advisory lock; exported so the tests drive single,
// deterministic ticks instead of racing a ticker.
func (s *Scheduler) Tick(ctx context.Context) {
	locked, err := s.lock.WithAdvisoryLock(ctx, LockKey, s.leaderTick)
	switch {
	case err != nil:
		// Both "could not take the lock" and "the work itself failed" land
		// here. Neither stops Run's loop: the next tick is seconds away and
		// re-elects a leader from scratch.
		s.m.SchedulerTicks.WithLabelValues(tickError).Inc()
		slog.Warn("scheduler: tick failed", "error", err)
	case !locked:
		s.m.SchedulerTicks.WithLabelValues(tickNotLeader).Inc()
	default:
		s.m.SchedulerTicks.WithLabelValues(tickOK).Inc()
	}
}

// leaderTick is the work one tick does while holding LockKey. Recovery runs FIRST so orphaned
// claims and half-advanced firings converge before any new fire; all three parts always run -- a
// failure in one must not cost the fleet its other passes.
func (s *Scheduler) leaderTick(ctx context.Context) error {
	return errors.Join(s.recoverFirings(ctx), s.fireDue(ctx), s.reap(ctx))
}

// recoverFirings finishes the ledger's interrupted work. It only READS run state and writes
// schedule rows/ledger rows; it never starts a run and never publishes a completion frame.
func (s *Scheduler) recoverFirings(ctx context.Context) error {
	now := s.now()
	var errs []error

	// (1) Claims past their lease with no run: the owner is dead or stuck before the run
	// transaction. Release; the due pass in THIS tick re-claims the still-due schedule.
	stale, err := s.store.ListStaleClaims(ctx, now, recoverLimit)
	if err != nil {
		return fmt.Errorf("scheduler: list stale claims: %w", err)
	}
	for i := range stale {
		f := &stale[i]
		if rerr := s.store.ReleaseFiring(ctx, f.ID); rerr != nil {
			errs = append(errs, fmt.Errorf("scheduler: release stale claim %s: %w", f.ID, rerr))
			continue
		}
		s.m.SchedulerFirings.WithLabelValues(firingReclaimed).Inc()
		slog.Info("scheduler: released an expired claim with no run",
			"firing", f.ID, "schedule", f.ScheduleID, "previous_owner", f.ClaimedBy)
	}

	// (2) Firings with a linked run whose schedule row may never have been advanced (the mark
	// failed, or the process died right after the run transaction).
	running, err := s.store.ListRunningFirings(ctx, recoverLimit)
	if err != nil {
		errs = append(errs, fmt.Errorf("scheduler: list running firings: %w", err))
		return errors.Join(errs...)
	}
	for i := range running {
		if err := s.converge(ctx, &running[i]); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// converge advances the schedule row of a firing whose run transaction already committed, and
// settles it. The linked run distinguishes the three recovery shapes without any ambiguity: it
// exists and is active (still executing), exists and is terminal (completed) -- both converge
// identically since the schedule row is independent of run completion -- or is absent (retention),
// in which case the row was certainly advanced long ago and only the settle remains. A run that
// never existed can never reach here: a firing is 'running' only with a run_id.
func (s *Scheduler) converge(ctx context.Context, f *store.Firing) error {
	run, err := s.runner.Get(ctx, f.RunID)
	switch {
	case errors.Is(err, store.ErrNotFound):
		if settleErr := s.store.SettleFiring(ctx, f.ID); settleErr != nil {
			return fmt.Errorf("scheduler: settle firing %s after its run was pruned: %w", f.ID, settleErr)
		}
		s.m.SchedulerFirings.WithLabelValues(firingConverged).Inc()
		return nil
	case err != nil:
		return fmt.Errorf("scheduler: get run %s for firing %s: %w", f.RunID, f.ID, err)
	}

	sched, err := s.store.GetSchedule(ctx, f.ScheduleID)
	if err != nil {
		return fmt.Errorf("scheduler: get schedule %s for firing %s: %w", f.ScheduleID, f.ID, err)
	}

	if markErr := s.markFiredFrom(ctx, &sched, run.CreatedAt, f.NextFireAt); markErr != nil {
		return fmt.Errorf("scheduler: converge firing %s: %w", f.ID, markErr)
	}
	if err := s.store.SettleFiring(ctx, f.ID); err != nil {
		return fmt.Errorf("scheduler: settle converged firing %s: %w", f.ID, err)
	}
	s.m.SchedulerFirings.WithLabelValues(firingConverged).Inc()
	slog.Info("scheduler: converged a firing left before its schedule row advanced",
		"firing", f.ID, "schedule", sched.ID, "run", f.RunID, "run_status", run.Status)
	return nil
}

// markFiredFrom applies the schedule-row write a successful fire requires. kind='once' disables
// the schedule; anything else takes the firing's pre-planned next fire (nil retires an
// unrecognized kind).
func (s *Scheduler) markFiredFrom(ctx context.Context, sched *store.Schedule, firedAt time.Time, next *time.Time) error {
	if sched.Kind == kindOnce {
		return s.store.CompleteOnceSchedule(ctx, sched.ID, firedAt)
	}
	return s.store.MarkScheduleFired(ctx, sched.ID, firedAt, next, "")
}

// fireDue selects the schedules whose next_fire_at has passed and processes each one; the selection
// is store.ListDueSchedules verbatim.
func (s *Scheduler) fireDue(ctx context.Context) error {
	now := s.now()

	due, err := s.store.ListDueSchedules(ctx, now, dueLimit)
	if err != nil {
		return fmt.Errorf("scheduler: list due schedules: %w", err)
	}
	if len(due) == 0 {
		return nil
	}

	// One topology read per tick at most, shared by every schedule that
	// actually needs it (only "one-per-zone" definitions do), and skipped
	// entirely when none does.
	topo := newTopologyCache(s.topology)

	var errs []error
	for i := range due {
		if err := s.fireOne(ctx, &due[i], now, topo); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// fireOne binds one due schedule's process: find or make the claim for the due point, then
// processClaim drives it. kind='continuous' returns immediately (in practice one never reaches
// here at all -- httpapi seeds a continuous schedule with next_fire_at = NULL -- but "the scheduler
// silently ignores a whole schedule kind" is exactly the line that reads as a bug to the next
// person, so it is stated rather than left to be inferred from an index predicate two packages
// away.)
func (s *Scheduler) fireOne(ctx context.Context, sched *store.Schedule, now time.Time, topo *topologyCache) error {
	if sched.Kind == kindContinuous || sched.NextFireAt == nil {
		return nil
	}
	dueAt := *sched.NextFireAt

	// An existing non-released firing decides the point.
	if f, err := s.store.GetFiringForDuePoint(ctx, sched.ID, dueAt); err == nil {
		switch f.Status {
		case store.FiringSettled:
			return nil
		case store.FiringRunning:
			// A schedule that is still due despite a linked run is precisely the post-crash case:
			// converge finishes the advance, no second run.
			return s.converge(ctx, &f)
		case store.FiringClaimed:
			switch {
			case f.ClaimedBy == s.ownerID:
				// Our own unexpired claim (a resumed tick): continue it.
				return s.processClaim(ctx, &f, sched, now, topo)
			case now.Before(f.LeaseExpiresAt):
				// Another replica is on it within its lease -- takeover would be premature.
				return nil
			default:
				// Lease expired: stale claim. Defensive -- recoverFirings normally releases these
				// first; the branch matters when that listing was truncated.
				if rerr := s.store.ReleaseFiring(ctx, f.ID); rerr != nil {
					return fmt.Errorf("scheduler: release expired claim %s: %w", f.ID, rerr)
				}
				s.m.SchedulerFirings.WithLabelValues(firingReclaimed).Inc()
			}
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("scheduler: schedule %s: look up firing for %s: %w", sched.ID, dueAt, err)
	}

	// Claim the due point: schedule_firings_due_point_uniq makes this an atomic election.
	f, err := s.store.CreateFiring(ctx, store.FiringInput{
		ScheduleID:     sched.ID,
		DueAt:          dueAt,
		ClaimedBy:      s.ownerID,
		LeaseExpiresAt: now.Add(claimLease),
		NextFireAt:     plannedNextFireAt(sched, dueAt),
	})
	if err != nil {
		if errors.Is(err, store.ErrAlreadyExists) {
			// Another replica won the race -- the closed-set counterpart of the lock.
			s.m.SchedulerFirings.WithLabelValues(firingConflict).Inc()
			return nil
		}
		return fmt.Errorf("scheduler: schedule %s: claim %s: %w", sched.ID, dueAt, err)
	}
	s.m.SchedulerFirings.WithLabelValues(firingClaimed).Inc()

	return s.processClaim(ctx, &f, sched, now, topo)
}

// processClaim drives one claimed point to its outcome:
//
//   - a no-run skip (disabled definition / overrun): release the claim, advance via the skip write;
//   - a failure no run could be created from: a permanent refusal (cannotRun) settles the occurrence
//     with the reason, an interval failure consumes it fired-style with the error, a transient kind='once'
//     failure releases and re-queues the occurrence;
//   - a successful start: the run+link transaction already committed inside StartForFiring, then the
//     schedule row advances and the claim settles -- if that advance fails the firing stays 'running',
//     which is the exact fact converge needs, and the claim is NEVER released.
func (s *Scheduler) processClaim(
	ctx context.Context, f *store.Firing, sched *store.Schedule, now time.Time, topo *topologyCache,
) error {
	def, err := s.store.GetDefinition(ctx, sched.DefinitionID)
	if err != nil {
		return s.abortClaim(ctx, f, sched, now,
			fmt.Errorf("get definition %s: %w", sched.DefinitionID, err))
	}

	// An enabled schedule on a disabled definition is a paused check, not a broken one.
	if !def.Enabled {
		return s.skipClaim(ctx, f, sched, now, skipDisabled)
	}

	if s.previousRunActive(ctx, sched.ID) {
		return s.skipClaim(ctx, f, sched, now, skipOverrun)
	}

	spec, err := s.specFor(ctx, &def, topo)
	if err != nil {
		// A property of the definition as stored retires with the reason; anything transient is
		// handled the abort way (once: re-queue; interval: consumed with the error).
		if cannotRun(err) {
			return s.settleWithError(ctx, f, sched, now, err)
		}
		return s.abortClaim(ctx, f, sched, now, err)
	}

	runID, err := s.runner.StartForFiring(ctx, spec, authz.Subject{Kind: initiatorKind, ID: sched.ID}, f.ID)
	if err != nil {
		// The run transaction rolled back because recovery released our expired claim mid-start.
		// Nothing was created and the claim is already released -- the point is re-claimed by a
		// later tick; do not write a mark here.
		if errors.Is(err, store.ErrClaimNotHeld) {
			slog.Warn("scheduler: run start lost its expired claim, the point will be re-claimed",
				"schedule", sched.ID, "firing", f.ID)
			return err
		}
		if cannotRun(err) {
			return s.settleWithError(ctx, f, sched, now, err)
		}
		return s.abortClaim(ctx, f, sched, now, err)
	}

	// The due point HAS fired: run row and link are one committed fact. Advance the schedule row
	// and settle.
	s.lastRun[sched.ID] = runID
	s.m.SchedulerFired.WithLabelValues(sched.Kind).Inc()
	slog.Info("scheduler: started scheduled run",
		"schedule", sched.ID, "definition", def.ID, "kind", sched.Kind, "run", runID)

	if markErr := s.markFiredFrom(ctx, sched, now, f.NextFireAt); markErr != nil {
		// Firing stays 'running': converge re-applies this exact write. Never release.
		return fmt.Errorf("scheduler: schedule %s: advance after run %s: %w", sched.ID, runID, markErr)
	}
	if err := s.store.SettleFiring(ctx, f.ID); err != nil {
		// The schedule row is advanced already; converge's rewrite is idempotent and settles.
		return fmt.Errorf("scheduler: schedule %s: settle firing %s: %w", sched.ID, f.ID, err)
	}
	return nil
}

// skipClaim handles an occurrence that produced no run because of a deliberate skip: release the
// claim first (so the point is claimable again regardless of what the later write does), then
// advance the cadence WITHOUT stamping last_fired_at. A kind='once' schedule retries on
// onceSkipRetry from now -- writing its planned nil would retire the occurrence permanently; an
// interval advances from the claimed point.
func (s *Scheduler) skipClaim(
	ctx context.Context, f *store.Firing, sched *store.Schedule, now time.Time, reason string,
) error {
	if err := s.store.ReleaseFiring(ctx, f.ID); err != nil {
		return fmt.Errorf("scheduler: schedule %s: release claim for a skip: %w", sched.ID, err)
	}

	next := plannedNextFireAt(sched, f.DueAt)
	if sched.Kind == kindOnce {
		retry := now.Add(onceSkipRetry)
		next = &retry
	}
	if err := s.store.MarkScheduleSkipped(ctx, sched.ID, next); err != nil {
		return fmt.Errorf("scheduler: schedule %s: mark skipped: %w", sched.ID, err)
	}

	s.m.SchedulerSkipped.WithLabelValues(reason).Inc()
	slog.Info("scheduler: skipped a due schedule", "schedule", sched.ID, "reason", reason)
	return nil
}

// abortClaim handles a failure BEFORE any run existed. A kind='once' occurrence is still owed:
// release the claim and re-queue it on the skip-retry cadence, with last_fired_at untouched. An
// interval occurrence is consumed the way a successful-but-errored fire is -- fired-mark with the
// reason and the next occurrence kept -- which is the historical behavior and prevents a per-tick
// retry storm. cause is reported back so the tick outcome counts the failure.
func (s *Scheduler) abortClaim(
	ctx context.Context, f *store.Firing, sched *store.Schedule, now time.Time, cause error,
) error {
	if sched.Kind != kindOnce {
		return s.settleWithError(ctx, f, sched, now, cause)
	}

	if err := s.store.ReleaseFiring(ctx, f.ID); err != nil {
		return errors.Join(cause, fmt.Errorf("scheduler: schedule %s: release claim for retry: %w", sched.ID, err))
	}
	retry := now.Add(onceSkipRetry)
	if err := s.store.MarkScheduleSkipped(ctx, sched.ID, &retry); err != nil {
		return errors.Join(cause, fmt.Errorf("scheduler: schedule %s: re-queue for retry: %w", sched.ID, err))
	}
	return cause
}

// settleWithError consumes the occurrence WITHOUT a run, writing the fired-style mark that carries
// the reason (interval: next occurrence from the claimed point; once: nil next, retired), then
// settles the claim. cause is returned so the tick reports the failure as an error.
func (s *Scheduler) settleWithError(
	ctx context.Context, f *store.Firing, sched *store.Schedule, now time.Time, cause error,
) error {
	lastError := scheduleErrorText(sched.ID, cause)
	if markErr := s.store.MarkScheduleFired(ctx, sched.ID, now, f.NextFireAt, lastError); markErr != nil {
		return errors.Join(cause, fmt.Errorf("scheduler: schedule %s: record the failed fire: %w", sched.ID, markErr))
	}
	if err := s.store.SettleFiring(ctx, f.ID); err != nil {
		return errors.Join(cause, fmt.Errorf("scheduler: schedule %s: settle failed firing: %w", sched.ID, err))
	}
	return cause
}

// unrunnableError is specFor's refusal of a definition that describes a run the agents refuse.
type unrunnableError struct{ msg string }

func (e *unrunnableError) Error() string { return e.msg }

// cannotRun reports whether a start failure is a property of the definition as stored, which only
// an edit clears: a check type the agents refuse toward its destination, or a plane the runner
// refuses.
func cannotRun(err error) bool {
	_, unrunnable := errors.AsType[*unrunnableError](err)
	return unrunnable || errors.Is(err, checks.ErrInvalidPlane)
}

// scheduleErrorText renders a startFor error for check_schedules.last_error; the "scheduler:
// schedule <uuid>: " prefix every error arriving here carries is stripped.
func scheduleErrorText(scheduleID string, err error) string {
	return strings.TrimPrefix(err.Error(), "scheduler: schedule "+scheduleID+": ")
}

// previousRunActive reports whether a run for scheduleID is still mid-flight; failing OPEN here is
// the deliberate direction.
//
// It ASKS THE STORE rather than this replica's memory. The advisory lock rotates tick by tick, so
// with the chart's default of two console replicas the replica that fired a run was routinely not
// the one holding the next tick — and the other had no memory of it, so it fired the same schedule
// again. Two overlapping runs of the same check, doubling the fan-out at exactly the moment the
// first one was already slow enough to still be going.
//
// The in-memory map stays as the fast path and as the fallback when the store cannot answer.
func (s *Scheduler) previousRunActive(ctx context.Context, scheduleID string) bool {
	if active, err := s.runner.ActiveForInitiator(ctx, initiatorKind, scheduleID); err == nil {
		if !active {
			delete(s.lastRun, scheduleID)
		}
		return active
	}

	runID, ok := s.lastRun[scheduleID]
	if !ok {
		return false
	}
	run, err := s.runner.Get(ctx, runID)
	if err != nil {
		delete(s.lastRun, scheduleID)
		return false
	}
	if run.Status == "pending" || run.Status == "running" {
		return true
	}
	delete(s.lastRun, scheduleID)
	return false
}

// reap drives checks.Runner.ReapStuckRuns from the schedule pass.
//
// It is no longer the ONLY caller: cmd/console spawns checks.Runner.ReapLoop unconditionally,
// because reaping an orphaned run must not depend on the schedule loop being enabled (it is off by
// default). This stays because a tick that has just fired runs is the cheapest moment to notice the
// ones that never finished, and the statement is idempotent either way.
func (s *Scheduler) reap(ctx context.Context) error {
	n, err := s.runner.ReapStuckRuns(ctx, 0)
	if n > 0 {
		s.m.RunsReaped.WithLabelValues().Add(float64(n))
		slog.Warn("scheduler: force-finished stuck runs", "runs", n)
	}
	if err != nil {
		return fmt.Errorf("scheduler: reap stuck runs: %w", err)
	}
	return nil
}

// plannedNextFireAt computes the cadence advance a claim stores BEFORE the run is created, so the
// run path, every retry and recovery all advance to one identical value. - once: nil; - interval:
// the CLAIMED DUE POINT plus the interval (never "now" plus it -- a tick running behind or a
// recovered claim would otherwise drift the cadence); an unrecognized kind retires.
func plannedNextFireAt(sched *store.Schedule, dueAt time.Time) *time.Time {
	switch sched.Kind {
	case kindOnce:
		return nil
	case kindInterval:
		if sched.IntervalNs <= 0 {
			// store.ScheduleInput.Validate rejects this, so it can only come from a hand-edited
			// row; retiring it beats dividing by it.
			slog.Warn("scheduler: interval schedule has no interval, retiring it from the due index",
				"schedule", sched.ID)
			return nil
		}
		next := dueAt.Add(time.Duration(sched.IntervalNs))
		return &next
	default:
		slog.Warn("scheduler: unrecognized schedule kind, retiring it from the due index",
			"schedule", sched.ID, "kind", sched.Kind)
		return nil
	}
}

// specFor projects a stored definition onto the checks.Spec that checks.Runner.Start accepts.
func (s *Scheduler) specFor(ctx context.Context, def *store.Definition, topo *topologyCache) (checks.Spec, error) {
	// A row the API would now refuse: written before it did, or its definition edited since.
	if err := checks.RefuseExternalRun(def.CheckType, def.DestinationKind); err != nil {
		return checks.Spec{}, &unrunnableError{err.Error() + ", so no run was started"}
	}

	sources, err := s.sourcesFor(ctx, def.SourceSelection, topo)
	if err != nil {
		return checks.Spec{}, err
	}

	spec := checks.Spec{
		Sources: sources,
		Type:    def.CheckType,
		Plane:   def.Plane,
	}

	switch def.DestinationKind {
	case "node":
		// Left empty on purpose: checks.Plan expands an empty destination side
		// into every node in the current topology, which is exactly what a
		// node-destination definition means (probe the mesh).
	case "target":
		target, terr := s.store.GetTarget(ctx, def.DestinationTargetID)
		if terr != nil {
			return checks.Spec{}, fmt.Errorf("get destination target %s: %w", def.DestinationTargetID, terr)
		}
		spec.TypedDestinations = []checks.Destination{{
			Kind: checks.DestKindTarget, Name: target.Name, Address: target.Address,
		}}
	case "adhoc":
		// Name is the DEFINITION's name, never the raw address.
		spec.TypedDestinations = []checks.Destination{{
			Kind: checks.DestKindAdhoc, Name: def.Name, Address: def.DestinationAddress,
		}}
	default:
		return checks.Spec{}, fmt.Errorf("unknown destination kind %q on definition %s", def.DestinationKind, def.ID)
	}

	return spec, nil
}

// sourcesFor resolves a definition's source_selection to the node names a run should dispatch FROM;
// the semantics are the same ones checks.AssignAgents and httpapi's projectedAgents implement for
// the agent-assignment and projection paths.
func (s *Scheduler) sourcesFor(ctx context.Context, selection string, topo *topologyCache) ([]string, error) {
	if selection != "one-per-zone" {
		return nil, nil
	}
	snapshot, err := topo.get(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve one-per-zone sources: %w", err)
	}

	byZone := make(map[string]string, len(snapshot.Agents))
	for i := range snapshot.Agents {
		agent := &snapshot.Agents[i]
		if cur, ok := byZone[agent.Zone]; !ok || agent.NodeName < cur {
			byZone[agent.Zone] = agent.NodeName
		}
	}
	if len(byZone) == 0 {
		return nil, errors.New("resolve one-per-zone sources: topology reports no agents")
	}

	sources := make([]string, 0, len(byZone))
	for _, node := range byZone {
		sources = append(sources, node)
	}
	// Sorted so two ticks over an unchanged topology produce byte-identical
	// specs -- the stored spec snapshot is what an operator diffs runs by.
	sort.Strings(sources)
	return sources, nil
}

// topologyCache memoizes one tick's topology read. Built per tick, never
// reused across ticks: a snapshot older than one tick would resolve
// one-per-zone against nodes that may already be gone.
type topologyCache struct {
	src  TopologySource
	snap *controllerclient.Topology
	err  error
	done bool
}

func newTopologyCache(src TopologySource) *topologyCache {
	return &topologyCache{src: src}
}

// get returns the tick's snapshot, fetching it at most once -- including the
// failure, which is cached too so ten due schedules cannot turn one controller
// outage into ten round trips.
func (t *topologyCache) get(ctx context.Context) (*controllerclient.Topology, error) {
	if t.done {
		return t.snap, t.err
	}
	t.done = true
	if t.src == nil {
		t.err = errors.New("no topology source is configured (controller.url is unset)")
		return nil, t.err
	}
	t.snap, t.err = t.src.Topology(ctx)
	if t.err != nil {
		t.err = fmt.Errorf("read topology: %w", t.err)
		return nil, t.err
	}
	return t.snap, nil
}
