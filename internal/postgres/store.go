package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Relicxx/taskrail/internal/task"
)

type Store struct{ pool *pgxpool.Pool }

func New(p *pgxpool.Pool) *Store {
	return &Store{pool: p}
}

const columns = `
id::text, client_id, type, payload, result, state, run_at,
attempt_count, max_attempts, timeout_ms, generation,
COALESCE(worker_id, ''), cancel_requested, parent_task_id::text,
created_at, updated_at`

func scan(row pgx.Row) (t task.Task, e error) {
	e = row.Scan(
		&t.ID, &t.ClientID, &t.Type, &t.Payload, &t.Result, &t.State,
		&t.RunAt, &t.AttemptCount, &t.MaxAttempts, &t.TimeoutMS,
		&t.Generation, &t.WorkerID, &t.CancelRequested, &t.ParentTaskID,
		&t.CreatedAt, &t.UpdatedAt,
	)
	if errors.Is(e, pgx.ErrNoRows) {
		e = task.ErrNotFound
	}
	return
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	return s.pool.Begin(ctx)
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3e9)
	defer cancel()
	_ = tx.Rollback(ctx)
}
