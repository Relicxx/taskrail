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
