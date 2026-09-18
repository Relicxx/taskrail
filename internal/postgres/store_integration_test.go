package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL unset")
	}
	ctx := context.Background()
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	schema := "t_" + uuid.NewString()[:8]
	if _, e = p.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	sql, e := os.ReadFile("../../migrations/00001_tasks.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, string(sql)); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { pool.Close(); _, _ = p.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); p.Close() })
	return New(pool)
}
func input() task.CreateInput {
	return task.CreateInput{Type: "report.aggregate", Payload: json.RawMessage(`{"numbers":[1,2,3]}`), MaxAttempts: 5, TimeoutMS: 60000}
}
func TestCreateRace(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _, e := s.Create(ctx, "alice", "same", input())
			if e != nil {
				t.Error(e)
				return
			}
			ids <- v.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate")
		}
	}
	in := input()
	in.Payload = json.RawMessage(`{"numbers":[9]}`)
	if _, _, e := s.Create(ctx, "alice", "same", in); !errors.Is(e, task.ErrConflict) {
		t.Fatal(e)
	}
}
func TestLeaseRecovery(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, _, e := s.Create(ctx, "alice", "k", input())
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Claim(ctx, "a", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Claim(ctx, "b", time.Minute); !errors.Is(e, task.ErrNotFound) {
		t.Fatal(e)
	}
	_, e = s.pool.Exec(ctx, "UPDATE tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", v.ID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, a.ID, "a", a.Generation, json.RawMessage(`{}`)); !errors.Is(e, task.ErrLeaseLost) {
		t.Fatal(e)
	}
	if n, e := s.Recover(ctx, 10); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	b, e := s.Claim(ctx, "b", time.Minute)
	if e != nil || b.Generation != 2 {
		t.Fatal(b, e)
	}
	if e = s.Complete(ctx, a.ID, "a", a.Generation, json.RawMessage(`{}`)); !errors.Is(e, task.ErrLeaseLost) {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, b.ID, "b", b.Generation, json.RawMessage(`{"ok":true}`)); e != nil {
		t.Fatal(e)
	}
	v, e = s.Get(ctx, "alice", v.ID)
	if e != nil || v.State != task.Succeeded {
		t.Fatal(v, e)
	}
	if _, e = s.Get(ctx, "bob", v.ID); !errors.Is(e, task.ErrNotFound) {
		t.Fatal(e)
	}
}
func TestCancelAndRetry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, _, _ := s.Create(ctx, "alice", "k", input())
	a, e := s.Claim(ctx, "a", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Cancel(ctx, "alice", v.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, a.ID, "a", a.Generation, json.RawMessage(`{}`)); e != nil {
		t.Fatal(e)
	}
	v, _ = s.Get(ctx, "alice", v.ID)
	if v.State != task.Canceled {
		t.Fatal(v)
	}
	if _, _, e = s.Retry(ctx, "alice", v.ID, "retry"); !errors.Is(e, task.ErrConflict) {
		t.Fatal(e)
	}
}
func TestParallelClaim(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		if _, _, e := s.Create(ctx, "alice", uuid.NewString(), input()); e != nil {
			t.Fatal(e)
		}
	}
	var wg sync.WaitGroup
	seen := sync.Map{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := s.Claim(ctx, uuid.NewString(), time.Minute)
			if e != nil {
				t.Error(e)
				return
			}
			if _, loaded := seen.LoadOrStore(v.ID, true); loaded {
				t.Error("double claim")
			}
		}()
	}
	wg.Wait()
}
func TestExhaustionAndRetry(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	in := input()
	in.MaxAttempts = 1
	v, _, e := s.Create(ctx, "alice", "original", in)
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Claim(ctx, "w", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Fail(ctx, a.ID, "w", a.Generation, "timeout", false, 0); e != nil {
		t.Fatal(e)
	}
	v, _ = s.Get(ctx, "alice", v.ID)
	if v.State != task.Failed {
		t.Fatal(v)
	}
	child, replay, e := s.Retry(ctx, "alice", v.ID, "retry")
	if e != nil || replay || child.ParentTaskID == nil || *child.ParentTaskID != v.ID || child.AttemptCount != 0 {
		t.Fatal(child, replay, e)
	}
	same, replay, e := s.Retry(ctx, "alice", v.ID, "retry")
	if e != nil || !replay || same.ID != child.ID {
		t.Fatal(same, replay, e)
	}
	if _, _, e = s.Retry(ctx, "bob", v.ID, "retry"); !errors.Is(e, task.ErrNotFound) {
		t.Fatal(e)
	}
}
func TestPaginationAndCancelQueued(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, _, e := s.Create(ctx, "alice", uuid.NewString(), input()); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := s.pool.Exec(ctx, "UPDATE tasks SET created_at='2026-01-01T00:00:00Z'"); e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		items, next, e := s.List(ctx, "alice", cursor, 2)
		if e != nil {
			t.Fatal(e)
		}
		for _, v := range items {
			if seen[v.ID] {
				t.Fatal("duplicate page")
			}
			seen[v.ID] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if len(seen) != 5 {
		t.Fatal(seen)
	}
	if _, _, e := s.List(ctx, "alice", "broken", 2); !errors.Is(e, task.ErrInvalid) {
		t.Fatal(e)
	}
	for id := range seen {
		v, e := s.Cancel(ctx, "alice", id)
		if e != nil || v.State != task.Canceled {
			t.Fatal(v, e)
		}
	}
	if _, e := s.Claim(ctx, "w", time.Minute); !errors.Is(e, task.ErrNotFound) {
		t.Fatal(e)
	}
}
func TestCompleteBeforeCancel(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, _, _ = s.Create(ctx, "alice", "k", input())
	a, e := s.Claim(ctx, "w", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, a.ID, "w", a.Generation, json.RawMessage(`{}`)); e != nil {
		t.Fatal(e)
	}
	v, e := s.Cancel(ctx, "alice", a.ID)
	if e != nil || v.State != task.Succeeded {
		t.Fatal(v, e)
	}
	attempts, e := s.Attempts(ctx, "alice", a.ID)
	if e != nil || len(attempts) != 1 || attempts[0].FinishedAt == nil || attempts[0].Outcome != "succeeded" {
		t.Fatal(attempts, e)
	}
}
func TestExpiredFinalAttempt(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	in := input()
	in.MaxAttempts = 1
	_, _, _ = s.Create(ctx, "alice", "k", in)
	a, e := s.Claim(ctx, "w", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.pool.Exec(ctx, "UPDATE tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", a.ID)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.Recover(ctx, 10); e != nil {
		t.Fatal(e)
	}
	v, e := s.Get(ctx, "alice", a.ID)
	if e != nil || v.State != task.Failed {
		t.Fatal(v, e)
	}
}
func TestCanceledLease(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, _, _ = s.Create(ctx, "alice", "k", input())
	a, e := s.Claim(ctx, "w", time.Minute)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = s.Cancel(ctx, "alice", a.ID)
	_, e = s.pool.Exec(ctx, "UPDATE tasks SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", a.ID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.Recover(ctx, 10)
	if e != nil {
		t.Fatal(e)
	}
	v, _ := s.Get(ctx, "alice", a.ID)
	if v.State != task.Canceled {
		t.Fatal(v)
	}
}
