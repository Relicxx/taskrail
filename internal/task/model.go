package task

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type State string

const (
	Queued    State = "queued"
	Running   State = "running"
	Succeeded State = "succeeded"
	Failed    State = "failed"
	Canceled  State = "canceled"
)

var (
	ErrInvalid   = errors.New("invalid input")
	ErrNotFound  = errors.New("not found")
	ErrConflict  = errors.New("conflict")
	ErrLeaseLost = errors.New("lease lost")
)

type CreateInput struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	RunAt       *time.Time      `json:"run_at,omitempty"`
	MaxAttempts int             `json:"max_attempts,omitempty"`
	TimeoutMS   int             `json:"timeout_ms,omitempty"`
}
type Task struct {
	ID              string          `json:"id"`
	ClientID        string          `json:"-"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Result          json.RawMessage `json:"result,omitempty"`
	State           State           `json:"state"`
	RunAt           time.Time       `json:"run_at"`
	AttemptCount    int             `json:"attempt_count"`
	MaxAttempts     int             `json:"max_attempts"`
	TimeoutMS       int             `json:"timeout_ms"`
	Generation      int64           `json:"-"`
	WorkerID        string          `json:"-"`
	CancelRequested bool            `json:"cancel_requested"`
	ParentTaskID    *string         `json:"parent_task_id,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
}
type Attempt struct {
	Generation int64      `json:"generation"`
	WorkerID   string     `json:"worker_id"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Outcome    string     `json:"outcome"`
	Error      string     `json:"error,omitempty"`
}
type Handler interface {
	Execute(context.Context, json.RawMessage) (json.RawMessage, error)
}
type permanent struct{ error }

func Permanent(e error) error  { return permanent{e} }
func IsPermanent(e error) bool { var p permanent; return errors.As(e, &p) }
func Backoff(attempt int, random float64) time.Duration {
	if random < 0 {
		random = 0
	}
	if random > 1 {
		random = 1
	}
	seconds := 1.
	for i := 1; i < attempt && seconds < 60; i++ {
		seconds *= 2
	}
	if seconds > 60 {
		seconds = 60
	}
	return time.Duration(seconds * random * float64(time.Second))
}
