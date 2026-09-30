package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/google/uuid"
	"time"
)

func (s *Store) Get(ctx context.Context, client, id string) (task.Task, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return task.Task{}, task.ErrNotFound
	}
	id = parsed.String()
	return scan(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM tasks WHERE client_id=$1 AND id=$2`, client, id))
}

type cursor struct {
	Time time.Time `json:"t"`
	ID   string    `json:"i"`
}

func (s *Store) List(ctx context.Context, client, raw string, limit int) ([]task.Task, string, error) {
	if limit < 1 || limit > 100 {
		return nil, "", task.ErrInvalid
	}
	var c cursor
	if raw != "" {
		b, e := base64.RawURLEncoding.DecodeString(raw)
		if e != nil || json.Unmarshal(b, &c) != nil || c.Time.IsZero() {
			return nil, "", task.ErrInvalid
		}
		_, parseErr := uuid.Parse(c.ID)
		if parseErr != nil {
			return nil, "", task.ErrInvalid
		}
	}
	c.ID = parsedCursorID(c.ID)
	var after any
	var id any
	if raw != "" {
		after = c.Time
		id = c.ID
	}
	rows, e := s.pool.Query(ctx, `SELECT `+columns+` FROM tasks WHERE client_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id)>($2,$3::uuid)) ORDER BY created_at,id LIMIT $4`, client, after, id, limit+1)
	if e != nil {
		return nil, "", e
	}
	defer rows.Close()
	out := []task.Task{}
	for rows.Next() {
		v, e := scan(rows)
		if e != nil {
			return nil, "", e
		}
		out = append(out, v)
	}
	if e = rows.Err(); e != nil {
		return nil, "", e
	}
	next := ""
	if len(out) > limit {
		out = out[:limit]
		v := out[len(out)-1]
		b, _ := json.Marshal(cursor{v.CreatedAt, v.ID})
		next = base64.RawURLEncoding.EncodeToString(b)
	}
	return out, next, nil
}
func (s *Store) Attempts(ctx context.Context, client, id string) ([]task.Attempt, error) {
	if _, e := s.Get(ctx, client, id); e != nil {
		return nil, e
	}
	rows, e := s.pool.Query(ctx, `SELECT generation,worker_id,started_at,finished_at,outcome,error FROM task_attempts WHERE task_id=$1 ORDER BY generation`, id)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []task.Attempt{}
	for rows.Next() {
		var a task.Attempt
		if e = rows.Scan(&a.Generation, &a.WorkerID, &a.StartedAt, &a.FinishedAt, &a.Outcome, &a.Error); e != nil {
			return nil, e
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func parsedCursorID(raw string) string {
	v, e := uuid.Parse(raw)
	if e != nil {
		return raw
	}
	return v.String()
}
