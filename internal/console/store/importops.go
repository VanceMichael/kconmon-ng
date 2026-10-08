package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Import operation statuses (import_operations.status, migration 00020).
const (
	ImportStatusRunning   = "running"
	ImportStatusSucceeded = "succeeded"
)

// ErrImportNotOwned is returned when an executor writes a checkpoint or a completion for an
// operation it no longer owns: another executor fenced it (a successful takeover after a stale
// heartbeat). It tells the caller to STOP applying that bundle -- the new owner advances it.
var ErrImportNotOwned = errors.New("store: import operation is not owned by this executor")

// ImportOperation is one durable row of import_operations.
type ImportOperation struct {
	ID          string
	Fingerprint string
	Status      string
	Checkpoint  json.RawMessage
	// Result is nil while the operation is running and the final import response once succeeded.
	Result json.RawMessage

	ExecutorID      string
	StartedBy       string
	LastHeartbeatAt time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// Store query labels for the import-operation queries.
const (
	queryCreateImportOperation = "create_import_operation"
	queryGetImportOperation    = "get_import_operation"
	queryClaimImportOperation  = "claim_import_operation"
	querySaveImportCheckpoint  = "save_import_checkpoint"
	queryCompleteImport        = "complete_import_operation"
)

// importOperationColumns is the full column list every query here scans, in scan order.
const importOperationColumns = `
	id, fingerprint, status, checkpoint, result,
	executor_id, started_by, last_heartbeat_at, created_at, updated_at`

func scanImportOperation(row pgx.Row) (ImportOperation, error) {
	var op ImportOperation
	err := row.Scan(
		&op.ID, &op.Fingerprint, &op.Status, &op.Checkpoint, &op.Result,
		&op.ExecutorID, &op.StartedBy, &op.LastHeartbeatAt, &op.CreatedAt, &op.UpdatedAt,
	)
	if err != nil {
		return ImportOperation{}, err
	}
	return op, nil
}

// CreateImportOperation inserts the row for a brand-new logical import. A concurrent first request
// answers ErrAlreadyExists, which the caller treats as "get the row and join it".
func (db *DB) CreateImportOperation(
	ctx context.Context, id, fingerprint, startedBy, executorID string, checkpoint json.RawMessage,
) (ImportOperation, error) {
	start := time.Now()
	row := db.pool.QueryRow(ctx, `
		INSERT INTO import_operations
			(id, fingerprint, checkpoint, executor_id, started_by, last_heartbeat_at)
		VALUES ($1, $2, $3, $4, $5, now())
		RETURNING `+importOperationColumns,
		id, fingerprint, orEmptyJSON(checkpoint), executorID, startedBy,
	)
	op, err := scanImportOperation(row)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolationCode {
			err = ErrAlreadyExists
		}
	}
	db.observe(queryCreateImportOperation, start, queryResult(err))
	if err != nil {
		return ImportOperation{}, fmt.Errorf("store: create import operation: %w", err)
	}
	return op, nil
}

// GetImportOperation reads one operation by id; a missing row answers ErrNotFound.
func (db *DB) GetImportOperation(ctx context.Context, id string) (ImportOperation, error) {
	start := time.Now()
	op, err := scanImportOperation(db.pool.QueryRow(ctx,
		`SELECT `+importOperationColumns+` FROM import_operations WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	db.observe(queryGetImportOperation, start, queryResult(err))
	if err != nil {
		return ImportOperation{}, fmt.Errorf("store: get import operation: %w", err)
	}
	return op, nil
}

// ClaimImportOperation takes ownership of a RUNNING operation whose last heartbeat predates
// staleBefore -- the old executor therefore has no demonstrably live process. false (with no
// error) means the race was lost: a fresh heartbeat arrived, another claim won, or the operation
// already finished; the caller then re-reads and joins.
func (db *DB) ClaimImportOperation(
	ctx context.Context, id, executorID string, staleBefore time.Time,
) (ImportOperation, bool, error) {
	start := time.Now()
	op, err := scanImportOperation(db.pool.QueryRow(ctx, `
		UPDATE import_operations
		   SET executor_id = $2, last_heartbeat_at = now(), updated_at = now()
		 WHERE id = $1 AND status = $3 AND last_heartbeat_at < $4
		RETURNING `+importOperationColumns,
		id, executorID, ImportStatusRunning, staleBefore,
	))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		db.observe(queryClaimImportOperation, start, resultOK)
		return ImportOperation{}, false, nil
	case err != nil:
		db.observe(queryClaimImportOperation, start, resultError)
		return ImportOperation{}, false, fmt.Errorf("store: claim import operation: %w", err)
	}
	db.observe(queryClaimImportOperation, start, resultOK)
	return op, true, nil
}

// SaveImportCheckpoint persists one checkpoint and renews this executor's heartbeat in the same
// write: a checkpoint and a heartbeat are one statement. Ownership-fenced -- an executor another
// process has since replaced gets ErrImportNotOwned and must stop.
func (db *DB) SaveImportCheckpoint(
	ctx context.Context, id, executorID string, checkpoint json.RawMessage,
) error {
	start := time.Now()
	tag, err := db.pool.Exec(ctx, `
		UPDATE import_operations
		   SET checkpoint = $3, last_heartbeat_at = now(), updated_at = now()
		 WHERE id = $1 AND executor_id = $2 AND status = $4`,
		id, executorID, orEmptyJSON(checkpoint), ImportStatusRunning,
	)
	switch {
	case err != nil:
		db.observe(querySaveImportCheckpoint, start, resultError)
		return fmt.Errorf("store: save import checkpoint: %w", err)
	case tag.RowsAffected() == 0:
		db.observe(querySaveImportCheckpoint, start, resultConflict)
		return ErrImportNotOwned
	}
	db.observe(querySaveImportCheckpoint, start, resultOK)
	return nil
}

// CompleteImportOperation flips the operation to succeeded and stores the final result; the write
// is terminal and ownership-fenced like SaveImportCheckpoint.
func (db *DB) CompleteImportOperation(
	ctx context.Context, id, executorID string, result json.RawMessage,
) error {
	start := time.Now()
	tag, err := db.pool.Exec(ctx, `
		UPDATE import_operations
		   SET status = $3, result = $4, last_heartbeat_at = now(), updated_at = now()
		 WHERE id = $1 AND executor_id = $2 AND status = $5`,
		id, executorID, ImportStatusSucceeded, result, ImportStatusRunning,
	)
	switch {
	case err != nil:
		db.observe(queryCompleteImport, start, resultError)
		return fmt.Errorf("store: complete import operation: %w", err)
	case tag.RowsAffected() == 0:
		db.observe(queryCompleteImport, start, resultConflict)
		return ErrImportNotOwned
	}
	db.observe(queryCompleteImport, start, resultOK)
	return nil
}
