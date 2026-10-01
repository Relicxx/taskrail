package worker

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

type runningStore struct {
	Store
	claims         atomic.Int64
	completed      atomic.Int64
	heartbeatCalls atomic.Int64
}

func (s *runningStore) Claim(context.Context, string, time.Duration) (task.Task, error) {
	n := s.claims.Add(1)
	return task.Task{ID: uuid.NewString(), Type: "block", Generation: n, TimeoutMS: 10000}, nil
}
func (s *runningStore) Heartbeat(context.Context, string, string, int64, time.Duration) (bool, error) {
	s.heartbeatCalls.Add(1)
	return false, nil
}
func (s *runningStore) Recover(context.Context, int) (int, error) { return 0, nil }
func (s *runningStore) Complete(context.Context, string, string, int64, json.RawMessage) error {
	s.completed.Add(1)
	return nil
}
func (s *runningStore) Fail(context.Context, string, string, int64, string, bool, time.Duration) error {
	return nil
}

type blockHandler struct {
	started chan struct{}
	release chan struct{}
	active  atomic.Int64
	maximum atomic.Int64
}

func (h *blockHandler) Execute(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	n := h.active.Add(1)
	defer h.active.Add(-1)
	for {
		old := h.maximum.Load()
		if old >= n || h.maximum.CompareAndSwap(old, n) {
			break
		}
	}
	h.started <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-h.release:
		return json.RawMessage(`{}`), nil
	}
}
func TestBoundedConcurrencyAndShutdown(t *testing.T) {
	s := &runningStore{}
	h := &blockHandler{started: make(chan struct{}, 10), release: make(chan struct{})}
	w := New(s, map[string]task.Handler{"block": h}, config.Config{WorkerID: "w", Concurrency: 2, Heartbeat: time.Millisecond * 20, Lease: time.Second, Poll: time.Millisecond, Grace: time.Millisecond * 30}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-h.started:
		case <-time.After(time.Second):
			t.Fatal("handlers did not start")
		}
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown exceeded grace")
	}
	if n := h.maximum.Load(); n != 2 {
		t.Fatal(n)
	}
	if n := s.claims.Load(); n != 2 {
		t.Fatal("claimed after shutdown", n)
	}
}

type timeoutHandler struct{}

func (timeoutHandler) Execute(ctx context.Context, _ json.RawMessage) (json.RawMessage, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type outcomeStore struct {
	Store
	failed chan bool
	cancel bool
	lost   bool
}

func (s *outcomeStore) Heartbeat(context.Context, string, string, int64, time.Duration) (bool, error) {
	if s.lost {
		return false, task.ErrLeaseLost
	}
	return s.cancel, nil
}
func (s *outcomeStore) Fail(_ context.Context, _ string, _ string, _ int64, message string, permanent bool, _ time.Duration) error {
	s.failed <- permanent
	return nil
}
func TestTimeoutAndRequestedCancellation(t *testing.T) {
	for _, requested := range []bool{false, true} {
		s := &outcomeStore{failed: make(chan bool, 1), cancel: requested}
		w := New(s, map[string]task.Handler{"wait": timeoutHandler{}}, config.Config{WorkerID: "w", Heartbeat: time.Millisecond, Lease: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		w.execute(context.Background(), task.Task{Type: "wait", TimeoutMS: 10})
		select {
		case permanent := <-s.failed:
			if permanent {
				t.Fatal("timeout marked permanent")
			}
		default:
			t.Fatal("timeout not finalized")
		}
	}
}
func TestLostLeaseDoesNotFinalize(t *testing.T) {
	s := &outcomeStore{failed: make(chan bool, 1), lost: true}
	w := New(s, map[string]task.Handler{"wait": timeoutHandler{}}, config.Config{WorkerID: "w", Heartbeat: time.Millisecond, Lease: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	w.execute(context.Background(), task.Task{Type: "wait", TimeoutMS: 10000})
	select {
	case <-s.failed:
		t.Fatal("finalized lost lease")
	default:
	}
}
func TestPermanentClassification(t *testing.T) {
	if !task.IsPermanent(task.Permanent(errors.New("invalid"))) {
		t.Fatal("lost error classification")
	}
}
func TestShutdownKeepsHeartbeatDuringGrace(t *testing.T) {
	s := &runningStore{}
	h := &blockHandler{started: make(chan struct{}, 10), release: make(chan struct{})}
	w := New(s, map[string]task.Handler{"block": h}, config.Config{WorkerID: "w", Concurrency: 1, Heartbeat: 10 * time.Millisecond, Lease: time.Second, Poll: time.Millisecond, Grace: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-h.started:
	case <-time.After(time.Second):
		t.Fatal("not started")
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for s.heartbeatCalls.Load() < 3 {
		select {
		case <-done:
			t.Fatal("stopped before grace")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("heartbeats stopped during grace")
		}
		time.Sleep(time.Millisecond)
	}
	close(h.release)
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("not finished")
	}
	if s.completed.Load() != 1 {
		t.Fatal("active task not completed")
	}
}
func TestCanceledRunDoesNotClaim(t *testing.T) {
	for i := 0; i < 30; i++ {
		s := &runningStore{}
		w := New(s, map[string]task.Handler{}, config.Config{WorkerID: "w", Concurrency: 1, Heartbeat: time.Millisecond, Lease: time.Second, Poll: time.Millisecond, Grace: time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if e := w.Run(ctx); e != nil {
			t.Fatal(e)
		}
		if s.claims.Load() != 0 {
			t.Fatal("claimed with canceled root context")
		}
	}
}
