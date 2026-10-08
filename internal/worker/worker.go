package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
)

type Store interface {
	Claim(context.Context, string, time.Duration) (task.Task, error)
	Heartbeat(context.Context, string, string, int64, time.Duration) (bool, error)
	Complete(context.Context, string, string, int64, json.RawMessage) error
	Fail(context.Context, string, string, int64, string, bool, time.Duration) error
	Recover(context.Context, int) (int, error)
}
type Worker struct {
	store    Store
	handlers map[string]task.Handler
	cfg      config.Config
	logger   *slog.Logger
}

func New(s Store, h map[string]task.Handler, c config.Config, l *slog.Logger) *Worker {
	return &Worker{s, h, c, l}
}

func (w *Worker) Run(ctx context.Context) error {
	activeCtx, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	var wg sync.WaitGroup
	recoveryCtx, cancelRecovery := context.WithCancel(ctx)
	defer cancelRecovery()
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(w.cfg.Poll)
		defer tick.Stop()
		for {
			select {
			case <-recoveryCtx.Done():
				return
			case <-tick.C:
				c, cancel := context.WithTimeout(recoveryCtx, 2*time.Second)
				n, e := w.store.Recover(c, 100)
				cancel()
				if e != nil && recoveryCtx.Err() == nil {
					w.logger.Warn("recovery unavailable")
				} else if n > 0 {
					w.logger.Info("expired leases recovered", "count", n)
				}
			}
		}
	}()
	slots := make(chan struct{}, w.cfg.Concurrency)
loop:
	for {
		if ctx.Err() != nil {
			break loop
		}
		select {
		case <-ctx.Done():
			break loop
		case slots <- struct{}{}:
		}
		if ctx.Err() != nil {
			<-slots
			break loop
		}
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		v, e := w.store.Claim(c, w.cfg.WorkerID, w.cfg.Lease)
		cancel()
		if e != nil {
			<-slots
			if !errors.Is(e, task.ErrNotFound) && ctx.Err() == nil {
				w.logger.Warn("claim unavailable")
			}
			timer := time.NewTimer(time.Duration(float64(w.cfg.Poll) * (.8 + rand.Float64()*.4)))
			select {
			case <-ctx.Done():
				timer.Stop()
				break loop
			case <-timer.C:
			}
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				<-slots
			}()
			w.execute(activeCtx, v)
		}()
	}
	cancelRecovery()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	timer := time.NewTimer(w.cfg.Grace)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		cancelActive()
		return nil
	}
}

func (w *Worker) execute(parent context.Context, v task.Task) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(v.TimeoutMS)*time.Millisecond)
	defer cancel()
	var outcome string
	// Heartbeats live independently of execution timeout until the final DB write finishes.
	hbCtx, stopHB := context.WithCancel(parent)
	defer stopHB()
	lost := make(chan struct{}, 1)
	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		tick := time.NewTicker(w.cfg.Heartbeat)
		defer tick.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-tick.C:
				budget := w.cfg.Heartbeat / 2
				if budget > 2*time.Second {
					budget = 2 * time.Second
				}
				c, stop := context.WithTimeout(hbCtx, budget)
				requested, e := w.store.Heartbeat(c, v.ID, w.cfg.WorkerID, v.Generation, w.cfg.Lease)
				stop()
				if e != nil {
					select {
					case lost <- struct{}{}:
					default:
					}
					cancel()
					return
				}
				if requested {
					cancel()
				}
			}
		}
	}()
	handler, ok := w.handlers[v.Type]
	var result json.RawMessage
	var e error
	if !ok {
		e = task.Permanent(task.ErrInvalid)
	} else {
		result, e = safeExecute(ctx, handler, v.Payload)
	}
	// Stop renewal before finalization. The final SQL transaction rechecks the lease.
	stopHB()
	hbWG.Wait()
	select {
	case <-lost:
		return
	default:
	}
	if parent.Err() != nil {
		return
	}
	c, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if e == nil {
		e = w.store.Complete(c, v.ID, w.cfg.WorkerID, v.Generation, result)
		if e == nil {
			outcome = "succeeded"
		}
	} else {
		message := "handler error"
		if errors.Is(e, context.DeadlineExceeded) {
			message = "execution timeout"
		} else if errors.Is(e, context.Canceled) {
			message = "execution canceled"
		} else if task.IsPermanent(e) {
			message = "permanent handler error"
		}
		permanent := task.IsPermanent(e)
		e = w.store.Fail(c, v.ID, w.cfg.WorkerID, v.Generation, message, permanent, task.Backoff(v.AttemptCount, rand.Float64()))
		if e == nil {
			outcome = "error"
		}
	}
	if e != nil {
		w.logger.Warn("attempt finalization failed", "task_id", v.ID, "generation", v.Generation)
	} else {
		w.logger.Info("attempt finished",
			"task_id", v.ID,
			"generation", v.Generation,
			"outcome", outcome,
		)
	}
}

func safeExecute(ctx context.Context, h task.Handler, p json.RawMessage) (result json.RawMessage, e error) {
	defer func() {
		if recover() != nil {
			result = nil
			e = errors.New("handler panic")
		}
	}()
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	result, e = h.Execute(ctx, p)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, e
}
