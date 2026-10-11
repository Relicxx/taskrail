package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/httpapi"
	"github.com/Relicxx/taskrail/internal/task"
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
	cfg.ConnConfig.RuntimeParams["application_name"] = schema
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

	t.Cleanup(func() {
		pool.Close()
		_, _ = p.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		p.Close()
	})
	return New(pool)
}

func input() task.CreateInput {
	return task.CreateInput{
		Type:        "report.aggregate",
		Payload:     json.RawMessage(`{"numbers":[1,2,3]}`),
		MaxAttempts: 5,
		TimeoutMS:   60000,
	}
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

func TestHeartbeatDoesNotRenewAfterLockWait(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	_, _, _ = s.Create(ctx, "alice", "k", input())
	a, e := s.Claim(ctx, "w", time.Second)
	if e != nil {
		t.Fatal(e)
	}

	tx, e := s.pool.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}

	defer rollback(tx)
	if _, e = tx.Exec(ctx, "SELECT id FROM tasks WHERE id=$1 FOR UPDATE", a.ID); e != nil {
		t.Fatal(e)
	}

	done := make(chan error, 1)

	go func() {
		_, e := s.Heartbeat(ctx, a.ID, "w", a.Generation, time.Minute)
		done <- e
	}()

	deadline := time.Now().Add(time.Second)

	for {
		var blocked int
		if e = s.pool.QueryRow(ctx,
			"SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND application_name=$1",
			s.pool.Config().ConnConfig.RuntimeParams["application_name"],
		).Scan(&blocked); e != nil {
			t.Fatal(e)
		}

		if blocked > 0 {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("heartbeat did not block")
		}

		time.Sleep(time.Millisecond)
	}

	if _, e = tx.Exec(ctx, "SELECT pg_sleep(1.1)"); e != nil {
		t.Fatal(e)
	}

	if e = tx.Commit(ctx); e != nil {
		t.Fatal(e)
	}

	select {
	case e := <-done:
		if !errors.Is(e, task.ErrLeaseLost) {
			t.Fatal("renewed expired lease", e)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat blocked")
	}
}

func TestUUIDRepresentations(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, _, e := s.Create(ctx, "alice", "uuid", input())
	if e != nil {
		t.Fatal(e)
	}

	for _, id := range []string{"urn:uuid:" + v.ID, "{" + v.ID + "}"} {
		got, e := s.Get(ctx, "alice", id)
		if e != nil || got.ID != v.ID {
			t.Fatal(got, e)
		}
	}

	b, _ := json.Marshal(cursor{Time: v.CreatedAt, ID: "urn:uuid:" + v.ID})
	if _, _, e = s.List(ctx, "alice", base64.RawURLEncoding.EncodeToString(b), 2); e != nil {
		t.Fatal(e)
	}
}

func TestHTTPPostgresReplayAndOwnership(t *testing.T) {
	s := testStore(t)
	h := httpapi.New(s, config.Config{Keys: map[string]string{"alice": "a", "bob": "b"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body := `{"type":"report.aggregate","payload":{"numbers":[1,2,3]}}`
	post := func(key string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v1/tasks", strings.NewReader(body))
		r.Header.Set("X-API-Key", "a")
		r.Header.Set("Idempotency-Key", key)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	first := post("lost-reply")
	if first.Code != 201 {
		t.Fatal(first.Code, first.Body.String())
	}

	var v task.Task
	_ = json.Unmarshal(first.Body.Bytes(), &v)
	repeat := post("lost-reply")
	if repeat.Code != 200 {
		t.Fatal(repeat.Code, repeat.Body.String())
	}

	var replay task.Task
	_ = json.Unmarshal(repeat.Body.Bytes(), &replay)
	if replay.ID != v.ID {
		t.Fatal("duplicated task after reply loss")
	}

	for _, suffix := range []string{"/cancel", "/retry"} {
		r := httptest.NewRequest("POST", "/v1/tasks/"+v.ID+suffix, nil)
		r.Header.Set("X-API-Key", "b")
		r.Header.Set("Idempotency-Key", "foreign")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 404 {
			t.Fatal(suffix, w.Code)
		}
	}
}

func TestClaimRollback(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	v, _, e := s.Create(ctx, "alice", "rollback", input())
	if e != nil {
		t.Fatal(e)
	}

	_, e = s.pool.Exec(ctx, `ALTER TABLE task_attempts ADD CONSTRAINT reject_attempt CHECK (generation < 0)`)
	if e != nil {
		t.Fatal(e)
	}

	if _, e = s.Claim(ctx, "w", time.Minute); e == nil {
		t.Fatal("expected attempt insert failure")
	}

	v, e = s.Get(ctx, "alice", v.ID)
	if e != nil || v.State != task.Queued || v.AttemptCount != 0 {
		t.Fatal("partial transaction", v, e)
	}
}
