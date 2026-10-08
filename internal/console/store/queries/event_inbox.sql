-- name: InsertEventReceipt :one
-- Durable receipt of one controller event, BEFORE any projection runs. ON CONFLICT DO NOTHING is
-- the single dedup gate: reconnect replay, a duplicate stream and a competing console replica
-- all converge on the existing row. Returns no rows (pgx.ErrNoRows) when another delivery won.
INSERT INTO event_inbox (event_id, epoch, event_seq, event_time, kind, raw)
VALUES ($1, $2, $3, $4, 'event', $5)
ON CONFLICT (event_id) DO NOTHING
RETURNING id;

-- name: InsertBaselineReceipt :one
-- A topology_baseline is synthesized by the console, so it carries no raw pb.Event: its projected
-- columns are filled at receipt time. Same dedup gate as InsertEventReceipt.
INSERT INTO event_inbox
    (event_id, epoch, event_seq, event_time, kind, type, severity, scope, summary, details)
VALUES ($1, $2, $3, $4, 'baseline', $5, $6, $7, $8, $9)
ON CONFLICT (event_id) DO NOTHING
RETURNING id;

-- name: ClaimDueEvents :many
-- One outbox batch, oldest-first across controller incarnations. FOR UPDATE SKIP LOCKED lets
-- several console replicas drain disjoint batches without blocking each other. Runs inside the
-- caller's transaction: a rollback releases every row, so a transient failure on the head row is
-- retried from that same row next pass instead of letting later events overtake it.
SELECT id, event_id, epoch, event_seq, event_time, kind, raw, type, severity, scope, summary, details
FROM event_inbox
WHERE status = 'received'
ORDER BY epoch, event_seq, id
LIMIT $1
FOR UPDATE SKIP LOCKED;

-- name: MarkInboxProjected :exec
UPDATE event_inbox
SET status = 'projected', attempts = attempts + 1, last_error = '', updated_at = now()
WHERE id = $1;

-- name: MarkInboxDead :exec
-- The terminal state for an event that can never project (unknown payload, corrupt bytes). It is
-- diagnosed, not retried, and leaves the queue: the claim query above never reads it again.
UPDATE event_inbox
SET status = 'dead', attempts = attempts + 1, last_error = $2, updated_at = now()
WHERE id = $1;

-- name: InboxCheckpoint :one
-- Highest seq DURABLY held for an incarnation, in any state: a 'received' row is already durable
-- (the outbox still owes its projection), and a 'dead' row still occupies its seq. It becomes
-- WatchEvents' last_seq on the next reconnect, so the controller never resends anything this
-- console has already receipted; durable identity — not projection state — defines the cursor.
SELECT COALESCE(MAX(event_seq), 0)::bigint AS seq
FROM event_inbox
WHERE epoch = $1;

-- name: CountInboxByStatus :one
SELECT count(*) AS n FROM event_inbox WHERE status = $1;

-- name: DeleteEventInboxBefore :execrows
DELETE FROM event_inbox
WHERE id IN (SELECT ei.id FROM event_inbox ei WHERE ei.event_time < $1 ORDER BY ei.event_time LIMIT $2);
