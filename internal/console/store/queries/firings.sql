-- name: CreateFiring :one
-- The row starts in status 'claimed'; schedule_firings_due_point_uniq (a partial unique index on
-- (schedule_id, due_at) WHERE status <> 'released') is what makes two replicas racing for the same
-- due point impossible to both win -- the loser gets a unique violation and silently leaves.
INSERT INTO schedule_firings (
    id, schedule_id, due_at, status, claimed_by, lease_expires_at, next_fire_at
)
VALUES ($1, $2, $3, 'claimed', $4, $5, $6)
RETURNING id, schedule_id, due_at, run_id, status, claimed_by, claimed_at,
          lease_expires_at, next_fire_at, last_error, updated_at;

-- name: GetFiringForDuePoint :one
-- The current (non-released) firing for a due point, if any -- the first thing a tick asks before
-- it claims, so a point that is claimed/running/settled is never claimed again.
SELECT id, schedule_id, due_at, run_id, status, claimed_by, claimed_at,
       lease_expires_at, next_fire_at, last_error, updated_at
FROM schedule_firings
WHERE schedule_id = $1 AND due_at = $2 AND status <> 'released';

-- name: ReleaseFiring :execrows
-- A claim that produced NO run is given up; only a 'claimed' row matches, after which the partial
-- unique index lets the point be claimed again.
UPDATE schedule_firings
SET status = 'released', updated_at = now()
WHERE id = $1 AND status = 'claimed';

-- name: LinkFiringRun :execrows
-- Runs INSIDE the run-creation transaction: together with the check_runs INSERT these two
-- statements commit (or roll back) as one, so a due point has a linked run if and only if the run
-- row exists. The status='claimed' guard refuses to link a point another transaction released.
UPDATE schedule_firings
SET status = 'running', run_id = $2, updated_at = now()
WHERE id = $1 AND status = 'claimed';

-- name: SettleFiring :execrows
-- The final transition: the schedule row was advanced. Both 'claimed' (a permanent refusal, whose
-- fired-mark wrote the schedule row without a run) and 'running' settle; a released row does not.
UPDATE schedule_firings
SET status = 'settled', updated_at = now()
WHERE id = $1 AND status <> 'released';

-- name: ListStaleClaims :many
-- Recovery: claims past their lease with no run created; the tick releases these, and the same
-- tick's due pass re-claims the still-due schedule.
SELECT id, schedule_id, due_at, run_id, status, claimed_by, claimed_at,
       lease_expires_at, next_fire_at, last_error, updated_at
FROM schedule_firings
WHERE status = 'claimed' AND lease_expires_at <= $1
ORDER BY lease_expires_at
LIMIT $2;

-- name: ListRunningFirings :many
-- Recovery: firings whose run transaction committed but whose schedule row may never have been
-- advanced (the mark failed, or the process died); the tick converges and settles them.
SELECT id, schedule_id, due_at, run_id, status, claimed_by, claimed_at,
       lease_expires_at, next_fire_at, last_error, updated_at
FROM schedule_firings
WHERE status = 'running'
ORDER BY updated_at
LIMIT $1;

-- name: CompleteOnceSchedule :execrows
-- The kind='once' success bookkeeping: the occurrence is retired by DISABLING the schedule and
-- clearing next_fire_at, and this is called only after the run+link transaction committed -- a
-- once schedule is never disabled before its run is confirmed created. Idempotent: a re-run after
-- a crash writes the same terminal row.
UPDATE check_schedules
SET enabled       = false,
    last_fired_at = $2,
    next_fire_at  = NULL,
    last_error    = '',
    last_error_at = NULL,
    updated_at    = now()
WHERE id = $1;
