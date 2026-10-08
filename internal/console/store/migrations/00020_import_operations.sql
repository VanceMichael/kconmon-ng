-- +goose Up
-- +goose StatementBegin

-- import_operations is the durable state of ONE logical configuration import, keyed by the
-- operationId the CALLER chose on POST /api/v1/import. A network drop, a duplicate POST or a
-- process restart therefore cannot leave the same logical import half-finished: every request
-- carrying the same id advances THIS row, a request whose executor's heartbeat has gone stale
-- (no live process) claims the row and resumes from `checkpoint`, and a finished import answers
-- every later duplicate from `result` without writing a row twice.
CREATE TABLE import_operations (
    id                TEXT PRIMARY KEY,
    -- fingerprint pins the operation to ONE bundle: reusing an id with a different bundle is a
    -- 409, never an ambiguous merge.
    fingerprint       TEXT        NOT NULL,
    -- running: some executor is (or may be) advancing it; succeeded: terminal, result is final.
    status            TEXT        NOT NULL DEFAULT 'running'
                          CHECK (status IN ('running', 'succeeded')),
    checkpoint        JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- result is the final import response once status = succeeded; NULL while running.
    result            JSONB,
    -- executor_id identifies the console process that last owned progress; used together with
    -- last_heartbeat_at to decide when an operation has no live executor and may be claimed.
    executor_id       TEXT        NOT NULL,
    started_by        TEXT        NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The takeover scan is "a running operation whose heartbeat is older than the timeout": this is
-- the index that makes it a bounded read regardless of how many operations accumulated.
CREATE INDEX import_operations_running_heartbeat_idx
    ON import_operations (last_heartbeat_at)
    WHERE status = 'running';

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS import_operations;
-- +goose StatementEnd
