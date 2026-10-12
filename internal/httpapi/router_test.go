package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
)

type fakeStore struct{ Store }

func (fakeStore) Create(ctx context.Context, client, key string, in task.CreateInput) (task.Task, bool, error) {
	if client != "alice" {
		panic("wrong owner")
	}

	if key == "conflict" {
		return task.Task{}, false, task.ErrConflict
	}

	return task.Task{ID: "id", State: task.Queued}, key == "repeat", nil
}

func (fakeStore) Get(context.Context, string, string) (task.Task, error) {
	return task.Task{}, task.ErrNotFound
}

func (fakeStore) Ping(context.Context) error {
	return nil
}

func TestRoutes(t *testing.T) {
	h := New(fakeStore{}, config.Config{Keys: map[string]string{"alice": "secret"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, tt := range []struct {
		name, key, body, content, auth, path string
		want                                 int
	}{
		{
			"create",
			"new",
			`{"type":"report.aggregate","payload":{"numbers":[1]}}`,
			"application/json",
			"secret",
			"/v1/tasks",
			201,
		},
		{
			"repeat",
			"repeat",
			`{"type":"report.aggregate","payload":{"numbers":[1]}}`,
			"application/json",
			"secret",
			"/v1/tasks",
			200,
		},
		{
			"conflict",
			"conflict",
			`{"type":"report.aggregate","payload":{"numbers":[1]}}`,
			"application/json",
			"secret",
			"/v1/tasks",
			409,
		},
		{
			"unauthorized",
			"new",
			`{}`,
			"application/json",
			"wrong",
			"/v1/tasks",
			401,
		},
		{
			"missingkey",
			"",
			`{}`,
			"application/json",
			"secret",
			"/v1/tasks",
			400,
		},
		{
			"duplicate",
			"new",
			`{"type":"report.aggregate","type":"demo.wait","payload":{}}`,
			"application/json",
			"secret",
			"/v1/tasks",
			400,
		},
		{"media", "new", `{}`, "text/plain", "secret", "/v1/tasks", 415},
		{
			"large",
			"new",
			strings.Repeat("x", 1048577),
			"application/json",
			"secret",
			"/v1/tasks",
			413,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			r.Header.Set("Content-Type", tt.content)
			r.Header.Set("X-API-Key", tt.auth)
			r.Header.Set("Idempotency-Key", tt.key)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tt.want {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}

			var response map[string]json.RawMessage
			if json.Unmarshal(w.Body.Bytes(), &response) != nil {
				t.Fatal("not JSON")
			}
		})
	}
}

func TestRateLimit(t *testing.T) {
	h := New(fakeStore{}, config.Config{Keys: map[string]string{"alice": "secret"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	limited := false

	for i := 0; i < 110; i++ {
		r := httptest.NewRequest("GET", "/v1/tasks/x", nil)
		r.Header.Set("X-API-Key", "secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code == 429 {
			limited = true
			break
		}

		if w.Code != 404 {
			t.Fatal(w.Code)
		}
	}

	if !limited {
		t.Fatal("unbounded requests")
	}
}

func TestExplicitZeroLimits(t *testing.T) {
	h := New(fakeStore{}, config.Config{Keys: map[string]string{"alice": "secret"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	for _, field := range []string{"max_attempts", "timeout_ms"} {
		r := httptest.NewRequest("POST", "/v1/tasks", strings.NewReader(`{"type":"report.aggregate","payload":{"numbers":[1]},"`+field+`":0}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-API-Key", "secret")
		r.Header.Set("Idempotency-Key", "zero")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(field, w.Code)
		}
	}
}
