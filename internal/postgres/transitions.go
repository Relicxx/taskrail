package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/Relicxx/taskrail/internal/task"
)

func (s *Store) Cancel(ctx context.Context, client, id string) (task.Task, error) {
	parsed, parseErr := uuid.Parse(id)
	if parseErr != nil {
		return task.Task{}, task.ErrNotFound
	}

	tx, e := s.begin(ctx)
	if e != nil {
		return task.Task{}, e
	}

	defer rollback(tx)
	id = parsed.String()
	v, e := scan(tx.QueryRow(ctx, `
SELECT `+columns+`
FROM tasks
WHERE client_id = $1 AND id = $2
FOR UPDATE
`, client, id))
	if e != nil {
		return v, e
	}

	if v.State == task.Queued || v.State == task.Running {
		v, e = scan(tx.QueryRow(ctx, `
UPDATE tasks
SET cancel_requested = true,
    state = CASE
        WHEN state = 'queued' THEN 'canceled'
        ELSE state
    END,
    updated_at = clock_timestamp()
WHERE id = $1
RETURNING
`+columns, id))
		if e != nil {
			return v, e
		}
	}

	e = tx.Commit(ctx)
	return v, e
}

func (s *Store) Complete(ctx context.Context, id, worker string, gen int64, result json.RawMessage) error {
	return s.finish(ctx, id, worker, gen, result, "", false, 0, true)
}

func (s *Store) Fail(ctx context.Context, id, worker string, gen int64, message string, permanent bool, delay time.Duration) error {
	return s.finish(ctx, id, worker, gen, nil, message, permanent, delay, false)
}

func (s *Store) finish(
	ctx context.Context,
	id, worker string,
	gen int64,
	result json.RawMessage,
	message string,
	permanent bool,
	delay time.Duration,
	success bool,
) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}

	defer rollback(tx)
	v, e := scan(tx.QueryRow(ctx, `
SELECT `+columns+`
FROM tasks
WHERE id = $1
  AND worker_id = $2
  AND generation = $3
  AND state = 'running'
  AND lease_until > clock_timestamp()
FOR UPDATE
`, id, worker, gen))
	if errors.Is(e, task.ErrNotFound) {
		return task.ErrLeaseLost
	}

	if e != nil {
		return e
	}

	state, outcome := task.Succeeded, "succeeded"
	if v.CancelRequested {
		state = task.Canceled
		outcome = "canceled"
		result = nil
	} else if !success {
		state = task.Queued
		outcome = "retry"
		if permanent || v.AttemptCount >= v.MaxAttempts {
			state = task.Failed
			outcome = "failed"
		}
	}

	// Recheck lease after taking the row lock; a waited transaction cannot finalize expired ownership.
	tag, e := tx.Exec(ctx, `
UPDATE tasks
SET state = $4,
    result = $5,
    worker_id = NULL,
    lease_until = NULL,
    run_at = CASE
        WHEN $4 = 'queued' THEN clock_timestamp() + $6::bigint * interval '1 millisecond'
        ELSE run_at
    END,
    updated_at = clock_timestamp()
WHERE id = $1
  AND worker_id = $2
  AND generation = $3
  AND lease_until > clock_timestamp()
`, id, worker, gen, state, []byte(result), delay.Milliseconds())
	if e != nil {
		return e
	}

	if tag.RowsAffected() != 1 {
		return task.ErrLeaseLost
	}

	_, e = tx.Exec(ctx, `
UPDATE task_attempts
SET finished_at = clock_timestamp(),
    outcome = $3,
    error = $4
WHERE task_id = $1 AND generation = $2
`, id, gen, outcome, message)
	if e != nil {
		return e
	}

	return tx.Commit(ctx)
}

func (s *Store) Recover(ctx context.Context, limit int) (int, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return 0, e
	}

	defer rollback(tx)
	rows, e := tx.Query(ctx, `
SELECT `+columns+`
FROM tasks
WHERE state = 'running'
  AND lease_until <= clock_timestamp()
ORDER BY lease_until
FOR UPDATE SKIP LOCKED
LIMIT $1
`, limit)
	if e != nil {
		return 0, e
	}

	var expired []task.Task

	for rows.Next() {
		v, e := scan(rows)
		if e != nil {
			rows.Close()
			return 0, e
		}

		expired = append(expired, v)
	}

	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}

	for _, v := range expired {
		state := task.Queued
		if v.CancelRequested {
			state = task.Canceled
		} else if v.AttemptCount >= v.MaxAttempts {
			state = task.Failed
		}

		if _, e = tx.Exec(ctx, `
UPDATE tasks
SET state = $2,
    worker_id = NULL,
    lease_until = NULL,
    run_at = clock_timestamp(),
    updated_at = clock_timestamp()
WHERE id = $1
`, v.ID, state); e != nil {
			return 0, e
		}

		if _, e = tx.Exec(ctx, `
UPDATE task_attempts
SET finished_at = clock_timestamp(),
    outcome = 'lease_expired',
    error = 'worker lease expired'
WHERE task_id = $1
  AND generation = $2
`, v.ID, v.Generation); e != nil {
			return 0, e
		}
	}

	if e = tx.Commit(ctx); e != nil {
		return 0, e
	}

	return len(expired), nil
}
