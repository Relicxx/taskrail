package postgres

import (
	"context"
	"encoding/json"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) Create(ctx context.Context, client, key string, in task.CreateInput) (task.Task, bool, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return task.Task{}, false, e
	}
	defer rollback(tx)
	v, replay, e := s.create(ctx, tx, client, key, in, nil)
	if e != nil {
		return v, false, e
	}
	e = tx.Commit(ctx)
	return v, replay, e
}
func (s *Store) create(ctx context.Context, tx pgx.Tx, client, key string, in task.CreateInput, parent *string) (task.Task, bool, error) {
	hash, e := task.Hash(in)
	if e != nil {
		return task.Task{}, false, e
	}
	if parent != nil {
		hash = hash + ":" + *parent
	}
	id := uuid.NewString()
	tag, e := tx.Exec(ctx, `INSERT INTO tasks(id,client_id,type,payload,run_at,max_attempts,timeout_ms,idempotency_key,request_hash,parent_task_id) VALUES($1,$2,$3,$4,COALESCE($5,clock_timestamp()),$6,$7,$8,$9,$10) ON CONFLICT(client_id,idempotency_key) DO NOTHING`, id, client, in.Type, []byte(in.Payload), in.RunAt, in.MaxAttempts, in.TimeoutMS, key, hash, parent)
	if e != nil {
		return task.Task{}, false, e
	}
	var existingHash string
	if e = tx.QueryRow(ctx, `SELECT request_hash FROM tasks WHERE client_id=$1 AND idempotency_key=$2`, client, key).Scan(&existingHash); e != nil {
		return task.Task{}, false, e
	}
	if existingHash != hash {
		return task.Task{}, false, task.ErrConflict
	}
	v, e := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM tasks WHERE client_id=$1 AND idempotency_key=$2`, client, key))
	return v, tag.RowsAffected() == 0, e
}
func (s *Store) Retry(ctx context.Context, client, id, key string) (task.Task, bool, error) {
	parsed, parseErr := uuid.Parse(id)
	if parseErr != nil {
		return task.Task{}, false, task.ErrNotFound
	}
	tx, e := s.begin(ctx)
	if e != nil {
		return task.Task{}, false, e
	}
	defer rollback(tx)
	id = parsed.String()
	old, e := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM tasks WHERE client_id=$1 AND id=$2 FOR UPDATE`, client, id))
	if e != nil {
		return old, false, e
	}
	if old.State != task.Failed {
		return old, false, task.ErrConflict
	}
	in := task.CreateInput{Type: old.Type, Payload: json.RawMessage(old.Payload), MaxAttempts: old.MaxAttempts, TimeoutMS: old.TimeoutMS}
	v, replay, e := s.create(ctx, tx, client, key, in, &id)
	if e != nil {
		return v, false, e
	}
	e = tx.Commit(ctx)
	return v, replay, e
}
