package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
)

type brokenStore struct{ Store }

func (brokenStore) Heartbeat(context.Context, string, string, int64, time.Duration) (bool, error) {
	return false, errors.New("database unavailable")
}

func (brokenStore) Complete(context.Context, string, string, int64, json.RawMessage) error {
	panic("must not complete without heartbeat")
}

func (brokenStore) Fail(context.Context, string, string, int64, string, bool, time.Duration) error {
	return nil
}

func TestHeartbeatFailureCancelsHandler(t *testing.T) {
	w := New(brokenStore{}, map[string]task.Handler{"demo.wait": task.Wait{}}, config.Config{WorkerID: "w", Heartbeat: time.Millisecond, Lease: time.Second}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	start := time.Now()
	w.execute(context.Background(), task.Task{
		ID:        "x",
		Type:      "demo.wait",
		Payload:   json.RawMessage(`{"duration_ms":5000}`),
		TimeoutMS: 10000,
	})
	if time.Since(start) > time.Second {
		t.Fatal("did not cancel on database failure")
	}
}

type panicker struct{}

func (panicker) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	panic("sensitive payload")
}

func TestPanicIsSafe(t *testing.T) {
	_, e := safeExecute(context.Background(), panicker{}, nil)
	if e == nil || e.Error() != "handler panic" {
		t.Fatal(e)
	}
}

func TestHandlerCannotCompleteAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := safeExecute(ctx, ignoresContext{}, nil)
	if !errors.Is(e, context.Canceled) {
		t.Fatal("canceled handler result was accepted", e)
	}
}

type ignoresContext struct{}

func (ignoresContext) Execute(context.Context, json.RawMessage) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}
