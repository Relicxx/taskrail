package postgres

import (
	"context"
	"github.com/Relicxx/taskrail/internal/task"
	"time"
)

func (s *Store) Claim(ctx context.Context, worker string, lease time.Duration) (task.Task, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return task.Task{}, e
	}
	defer rollback(tx)
	v, e := scan(tx.QueryRow(ctx, `UPDATE tasks SET state='running',generation=generation+1,attempt_count=attempt_count+1,worker_id=$1,lease_until=clock_timestamp()+$2::bigint*interval '1 millisecond',updated_at=clock_timestamp() WHERE id=(SELECT id FROM tasks WHERE state='queued' AND run_at<=clock_timestamp() AND attempt_count<max_attempts ORDER BY run_at,id FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING `+columns, worker, lease.Milliseconds()))
	if e != nil {
		return v, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO task_attempts(task_id,generation,worker_id) VALUES($1,$2,$3)`, v.ID, v.Generation, worker); e != nil {
		return v, e
	}
	e = tx.Commit(ctx)
	return v, e
}
func (s *Store) Heartbeat(ctx context.Context, id, worker string, gen int64, lease time.Duration) (bool, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return false, e
	}
	defer rollback(tx)
	var locked string
	e = tx.QueryRow(ctx, `SELECT id::text FROM tasks WHERE id=$1 AND worker_id=$2 AND generation=$3 AND state='running' FOR UPDATE`, id, worker, gen).Scan(&locked)
	if _, missing := scanError(e); missing {
		return false, task.ErrLeaseLost
	}
	if e != nil {
		return false, e
	}
	var cancel bool
	e = tx.QueryRow(ctx, `UPDATE tasks SET lease_until=clock_timestamp()+$4::bigint*interval '1 millisecond',updated_at=clock_timestamp() WHERE id=$1 AND worker_id=$2 AND generation=$3 AND state='running' AND lease_until>clock_timestamp() RETURNING cancel_requested`, id, worker, gen, lease.Milliseconds()).Scan(&cancel)
	if e != nil {
		if _, getErr := scanError(e); getErr {
			return false, task.ErrLeaseLost
		}
	}
	if e != nil {
		return false, e
	}
	return cancel, tx.Commit(ctx)
}
