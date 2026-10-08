package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/EsDmitrii/kconmon-ng/internal/console/store"
)

// restartScheduler builds a fresh Scheduler over the same fakes -- the in-process model of "the
// process exited and restarted", or "another replica took over": no memory, a new owner id, same
// durable store.
func (h *harness) restartScheduler() *Scheduler {
	s := New(Deps{
		Lock: h.lock, Store: h.store, Runner: h.runner, Topology: h.topo,
		Metrics: h.m, Interval: time.Second,
	})
	s.now = h.s.now
	return s
}

// activeFiringForSched returns the current non-released firing of schedID (test convenience).
func (h *harness) activeFiringForSched(t *testing.T) *store.Firing {
	t.Helper()
	sched := h.store.schedules[schedID]
	if sched.NextFireAt == nil {
		t.Fatal("scheduled due point is nil during the assertion")
	}
	f, ok := h.store.activeFiringFor(schedID, *sched.NextFireAt)
	if !ok {
		return nil
	}
	return &f
}

// TestIntervalCrashAfterRunBeforeAdvanceConvergesWithoutRefire is the core failure window: the
// run+link committed, MarkScheduleFired hit the database blip. The firing stays 'running', and a
// restarted scheduler converges the schedule row without a second run -- no duplicate samples.
func TestIntervalCrashAfterRunBeforeAdvanceConvergesWithoutRefire(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	h.seedSchedule(kindInterval, int64(oneMinute))
	h.store.markErr = errors.New("database is unavailable")

	h.s.Tick(context.Background())

	if len(h.runner.started) != 1 {
		t.Fatalf("started %d runs, want 1", len(h.runner.started))
	}
	// The schedule row was NOT advanced: the point stays due and unfired.
	sched := h.store.schedules[schedID]
	if sched.LastFiredAt != nil {
		t.Errorf("lastFiredAt = %v, want nil: the mark failed", sched.LastFiredAt)
	}
	if sched.NextFireAt == nil || !sched.NextFireAt.Equal(fixedNow.Add(-time.Second)) {
		t.Errorf("nextFireAt = %v, want the old due point untouched", sched.NextFireAt)
	}
	firing := h.activeFiringForSched(t)
	if firing == nil || firing.Status != store.FiringRunning || firing.RunID == "" {
		t.Fatalf("firing = %+v, want running with the run id -- the convergence fact", firing)
	}
	if got := h.counter(h.m.SchedulerTicks, tickError); got != 1 {
		t.Errorf("tick error = %v, want 1", got)
	}

	// The database recovers; a FRESH scheduler (process restart) ticks.
	h.store.markErr = nil
	s2 := h.restartScheduler()
	s2.Tick(context.Background())

	if len(h.runner.started) != 1 {
		t.Fatalf("started %d runs total, want exactly 1: convergence must not refire",
			len(h.runner.started))
	}
	after := h.store.schedules[schedID]
	if after.LastFiredAt == nil || !after.LastFiredAt.Equal(fixedNow) {
		t.Errorf("lastFiredAt = %v, want %v stamped from the linked run", after.LastFiredAt, fixedNow)
	}
	wantNext := fixedNow.Add(-time.Second + oneMinute)
	if after.NextFireAt == nil || !after.NextFireAt.Equal(wantNext) {
		t.Errorf("nextFireAt = %v, want %v", after.NextFireAt, wantNext)
	}
	if firing = h.activeFiringForSched(t); firing != nil {
		t.Errorf("firing = %+v, want settled (and thus invisible to active lookups)", firing)
	}
	if got := h.counter(h.m.SchedulerFirings, firingConverged); got != 1 {
		t.Errorf("converged = %v, want 1", got)
	}
}

// TestOnceCrashAfterRunLeavesTheScheduleEnabledUntilConvergence pins the gate: a kind='once'
// schedule is disabled only after its run is confirmed created AND the disable write succeeds. When
// CompleteOnceSchedule failed post-run, the restart converges -- disables it -- with no second run,
// and sends nothing that would duplicate a completion notification.
func TestOnceCrashAfterRunLeavesTheScheduleEnabledUntilConvergence(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	runAt := fixedNow.Add(-time.Minute)
	h.store.schedules[schedID] = store.Schedule{
		ID: schedID, DefinitionID: defID, Kind: kindOnce,
		RunAt: &runAt, Enabled: true, NextFireAt: &runAt,
	}
	h.store.completeErr = errors.New("database is unavailable")

	h.s.Tick(context.Background())

	if len(h.runner.started) != 1 {
		t.Fatalf("started %d runs, want 1", len(h.runner.started))
	}
	sched := h.store.schedules[schedID]
	if !sched.Enabled {
		t.Error("enabled = false despite the failed disable write")
	}
	firing := h.activeFiringForSched(t)
	if firing == nil || firing.Status != store.FiringRunning {
		t.Fatalf("firing = %+v, want running: the restart must know the run exists", firing)
	}

	// The run has since COMPLETED before the restart. Recovery must read that as terminal, not
	// start again and not notify completion a second time.
	for id, run := range h.runner.runs {
		run.Status = "succeeded"
		h.runner.runs[id] = run
	}

	h.store.completeErr = nil
	s2 := h.restartScheduler()
	s2.Tick(context.Background())

	if n := len(h.runner.started); n != 1 {
		t.Fatalf("started %d runs total, want exactly 1 despite the terminal run", n)
	}
	after := h.store.schedules[schedID]
	if after.Enabled {
		t.Error("enabled = true after convergence, want false")
	}
	if after.NextFireAt != nil {
		t.Errorf("nextFireAt = %v, want nil", after.NextFireAt)
	}
	// The complete write ran exactly once (the failed attempt was counted too -- the fake counts
	// calls regardless of outcome).
	if h.store.completes != 2 {
		t.Errorf("CompleteOnceSchedule call count = %d, want 2 (one failed, one after restart)",
			h.store.completes)
	}
	// The firing settled -- settle can't be keyed off the schedule's due point anymore: the
	// completed once row carries next_fire_at=NULL. The predecessor is released, this one is
	// settled; nothing claimed or running may remain.
	var openFiring *store.Firing
	for _, x := range h.store.firings {
		if x.ScheduleID == schedID && x.Status != store.FiringReleased && x.Status != store.FiringSettled {
			x := x
			openFiring = &x
		}
	}
	if openFiring != nil {
		t.Errorf("firing = %+v, want settled (no claimed/running firing left)", openFiring)
	}
}

// TestStaleClaimIsReleasedAndReclaimed covers the "claim with no run" side: a claim whose owner
// died before the run transaction is released once its lease expires, and the same tick re-claims
// and runs the point.
func TestStaleClaimIsReleasedAndReclaimed(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	h.seedSchedule(kindInterval, int64(oneMinute))

	// Another owner's claim, its lease already in the past.
	f, err := h.store.CreateFiring(context.Background(), store.FiringInput{
		ScheduleID: schedID, DueAt: fixedNow.Add(-time.Second), ClaimedBy: "dead-owner-aaaa1111",
		LeaseExpiresAt: fixedNow.Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("seed stale claim: %v", err)
	}

	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 1 {
		t.Fatalf("started %d runs, want 1", n)
	}
	// The old firing was released (it stays as the audit trail); a new firing drove the run and
	// settled.
	old := h.store.firings[f.ID]
	if old.Status != store.FiringReleased {
		t.Errorf("old firing status = %q, want released", old.Status)
	}
	var settled int
	for _, x := range h.store.firings {
		if x.ScheduleID == schedID && x.Status == store.FiringSettled {
			settled++
		}
	}
	if settled != 1 {
		t.Errorf("settled firings for the schedule = %d, want 1", settled)
	}
	if got := h.counter(h.m.SchedulerFirings, firingReclaimed); got != 1 {
		t.Errorf("reclaimed = %v, want 1", got)
	}
	if got := h.counter(h.m.SchedulerFirings, firingClaimed); got != 1 {
		t.Errorf("claimed = %v, want 1", got)
	}
}

// TestUnexpiredForeignClaimDefersTakeover: while another replica holds a live (unexpired) claim,
// a tick neither starts a run nor inserts a competing claim; takeover happens only after the lease.
func TestUnexpiredForeignClaimDefersTakeover(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	h.seedSchedule(kindInterval, int64(oneMinute))
	if _, err := h.store.CreateFiring(context.Background(), store.FiringInput{
		ScheduleID: schedID, DueAt: fixedNow.Add(-time.Second), ClaimedBy: "other-owner-bbbb2222",
		LeaseExpiresAt: fixedNow.Add(time.Minute),
	}); err != nil {
		t.Fatalf("seed live claim: %v", err)
	}

	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 0 {
		t.Fatalf("started %d runs against a live foreign claim, want 0", n)
	}
	if got := h.counter(h.m.SchedulerFirings, firingClaimed); got != 0 {
		t.Errorf("claimed = %v, want 0: no competing claim may be inserted", got)
	}
	if got := h.counter(h.m.SchedulerFirings, firingReclaimed); got != 0 {
		t.Errorf("reclaimed = %v, want 0: the lease has not expired", got)
	}

	// Clock moves beyond the lease: takeover proceeds.
	h.s.now = func() time.Time { return fixedNow.Add(2 * time.Minute) }
	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 1 {
		t.Fatalf("started %d runs after the lease expired, want 1", n)
	}
}

// TestStartThatLostItsClaimIsRetried: when StartForFiring finds its claim released (recovery
// acted on an expired lease while this replica was stalled), the run transaction rolls back and
// reports ErrClaimNotHeld. Nothing is marked, the point is still due, and the next attempt runs.
func TestStartThatLostItsClaimIsRetried(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	h.seedSchedule(kindInterval, int64(oneMinute))

	// Predecessor attempt already released (the exact shape after recovery gave it up).
	old, err := h.store.CreateFiring(context.Background(), store.FiringInput{
		ScheduleID: schedID, DueAt: fixedNow.Add(-time.Second), ClaimedBy: "stalled-owner-cccc3333",
		LeaseExpiresAt: fixedNow.Add(-time.Second),
	})
	if err != nil {
		t.Fatalf("seed predecessor claim: %v", err)
	}
	if err := h.store.ReleaseFiring(context.Background(), old.ID); err != nil {
		t.Fatalf("release predecessor: %v", err)
	}

	// This attempt's start loses its fresh claim mid-flight.
	h.runner.claimLost = true
	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 0 {
		t.Fatalf("started %d runs although claim was lost, want 0", n)
	}
	if len(h.store.marks) != 0 {
		t.Errorf("marks = %+v, want none: the schedule row must not advance without a run",
			h.store.marks)
	}
	sched := h.store.schedules[schedID]
	if !sched.Enabled || sched.NextFireAt == nil {
		t.Errorf("schedule = %+v, want still enabled and due", sched)
	}

	// A later tick (same process) resumes its own claim and runs the point.
	h.runner.claimLost = false
	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 1 {
		t.Fatalf("started %d runs on retry, want exactly 1", n)
	}
}

// TestOldFiredOnceDataStaysRetired is backward compatibility: a row fired before the firing
// ledger exists (enabled=true, next_fire_at=NULL) must be ignored -- not due, not rewritten, no
// claim created.
func TestOldFiredOnceDataStaysRetired(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	runAt := fixedNow.Add(-time.Hour)
	h.store.schedules[schedID] = store.Schedule{
		ID: schedID, DefinitionID: defID, Kind: kindOnce,
		RunAt: &runAt, Enabled: true, NextFireAt: nil,
	}

	h.s.Tick(context.Background())
	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 0 {
		t.Errorf("started %d runs for an old retired occurrence, want 0", n)
	}
	if len(h.store.firings) != 0 {
		t.Errorf("created %d firings for old data, want none", len(h.store.firings))
	}
	got := h.store.schedules[schedID]
	if !got.Enabled || got.NextFireAt != nil {
		t.Errorf("old row changed to %+v, want enabled=true and next_fire_at=NULL left alone", got)
	}
}

// TestDisabledScheduleNeverClaims covers the disabled-schedule compatibility: a schedule with
// enabled=false (with or without a past next_fire_at) is never listed due and never claims.
func TestDisabledScheduleNeverClaims(t *testing.T) {
	h := newHarness(t, true)
	h.seedDefinition()
	past := fixedNow.Add(-time.Hour)
	h.store.schedules[schedID] = store.Schedule{
		ID: schedID, DefinitionID: defID, Kind: kindInterval,
		IntervalNs: int64(oneMinute), Enabled: false, NextFireAt: &past,
	}

	h.s.Tick(context.Background())

	if n := len(h.runner.started); n != 0 {
		t.Errorf("started %d runs for a disabled schedule, want 0", n)
	}
	if len(h.store.firings) != 0 {
		t.Errorf("created %d firings for a disabled schedule, want none", len(h.store.firings))
	}
}
