-- +goose Up
-- event_inbox is the durable RECEIPT LOG of the controller event stream: every received event is
-- inserted here BEFORE it drives anything else (realtime dispatch, topology_events, the fold).
-- event_id is the canonical receipt identity — a controller-assigned UUID, or the legacy
-- "<seq>-<unixNano>" derivation for old controllers — so ON CONFLICT here is the ONE dedup gate
-- shared by reconnect replay and every console replica. Rows in status 'received' are claimed in
-- (epoch, event_seq) order by the transactional outbox drainer; 'projected' rows have a
-- topology_events twin in the same transaction; 'dead' rows are permanently unprojectable events
-- kept for diagnosis and never blocking the queue head.
CREATE TABLE event_inbox (
    id          BIGSERIAL    PRIMARY KEY,
    event_id    TEXT         NOT NULL UNIQUE,
    epoch       TEXT         NOT NULL DEFAULT '',
    event_seq   BIGINT       NOT NULL,
    event_time  TIMESTAMPTZ  NOT NULL,
    kind        TEXT         NOT NULL DEFAULT 'event',
    raw         BYTEA,
    type        TEXT,
    severity    TEXT,
    scope       TEXT,
    summary     TEXT,
    details     JSONB,
    status      TEXT         NOT NULL DEFAULT 'received',
    attempts    INTEGER      NOT NULL DEFAULT 0,
    last_error  TEXT         NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT event_inbox_kind_check   CHECK (kind IN ('event', 'baseline')),
    CONSTRAINT event_inbox_status_check CHECK (status IN ('received', 'projected', 'dead'))
);

-- The drainer's queue: oldest-first claim per controller incarnation. Partial so the hot path
-- never scans already-projected history.
CREATE INDEX event_inbox_due_idx
    ON event_inbox (epoch, event_seq, id)
    WHERE status = 'received';

-- Retention sweeps and the checkpoint/oldest queries ride event_time.
CREATE INDEX event_inbox_time_idx ON event_inbox (event_time);

-- +goose Down
DROP TABLE event_inbox;
