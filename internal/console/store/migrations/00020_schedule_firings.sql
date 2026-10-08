-- +goose Up
-- schedule_firings is the durable, recoverable ledger one due point (one occurrence of a kind='once'
-- or kind='interval' schedule) moves through. The scheduler claims a due point BEFORE it creates the
-- run, links the run in the SAME transaction that inserts it, and settles the claim only once the
-- schedule row (last_fired_at / next_fire_at / last_error) has been advanced. A process death or a
-- database blip anywhere after the run transaction therefore leaves a fact a restart, or another
-- replica's tick, can converge without starting the due point a second time.
CREATE TABLE schedule_firings (
    id               UUID        PRIMARY KEY,
    schedule_id      UUID        NOT NULL REFERENCES check_schedules(id) ON DELETE CASCADE,
    -- due_at is the claimed due point: check_schedules.next_fire_at snapshotted at claim time.
    due_at           TIMESTAMPTZ NOT NULL,
    -- run_id is set in the same transaction that inserts check_runs (LinkFiringRun); a non-NULL
    -- run_id means the due point HAS fired and must never start again.
    run_id           UUID        REFERENCES check_runs(id) ON DELETE SET NULL,
    -- claimed  : a replica intends to start and owns the point until lease_expires_at;
    -- running  : the run+link transaction committed, the point has fired;
    -- settled  : the schedule row was advanced, convergence finished;
    -- released : no run was created and the claim was given up -- another claim for the same point
    --            may be inserted.
    status           TEXT        NOT NULL,
    claimed_by       TEXT        NOT NULL,
    claimed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    -- next_fire_at is the cadence advance planned at claim time (interval: due_at + interval;
    -- once: NULL), so every retry and recovery writes byte-identical state.
    next_fire_at     TIMESTAMPTZ,
    last_error       TEXT        NOT NULL DEFAULT '',
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One non-released row per (schedule, due point): a due point is claimed at most once. Released
-- attempts are excluded so the same point can be claimed again, and more than one may be kept as
-- the audit trail of unsuccessful attempts.
CREATE UNIQUE INDEX schedule_firings_due_point_uniq
    ON schedule_firings (schedule_id, due_at)
    WHERE status <> 'released';

-- Recovery's stale-claim scan (lease expiry, claimed rows only).
CREATE INDEX schedule_firings_claimed_lease_idx
    ON schedule_firings (lease_expires_at)
    WHERE status = 'claimed';

-- Recovery's convergence scan (running rows that still need the schedule row advanced).
CREATE INDEX schedule_firings_running_idx
    ON schedule_firings (updated_at)
    WHERE status = 'running';

-- +goose Down
DROP TABLE schedule_firings;
