package app

import (
	"context"
	"errors"
	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/httpapi"
	"github.com/Relicxx/taskrail/internal/postgres"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/Relicxx/taskrail/internal/worker"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func Run(mode string) error {
	cfg, e := config.Load()
	if e != nil {
		return e
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = uuid.NewString()
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pc, e := pgxpool.ParseConfig(cfg.DatabaseURL)
	if e != nil {
		return errors.New("invalid database config")
	}
	pc.ConnConfig.ConnectTimeout = 3 * time.Second
	pc.MaxConns = 16
	pool, e := pgxpool.NewWithConfig(ctx, pc)
	if e != nil {
		return errors.New("database pool initialization failed")
	}
	defer pool.Close()
	ready, cancel := context.WithTimeout(ctx, 3*time.Second)
	e = pool.Ping(ready)
	cancel()
	if e != nil {
		return errors.New("database unavailable")
	}
	store := postgres.New(pool)
	errCh := make(chan error, 1)
	if mode == "worker" {
		handlers := map[string]task.Handler{"report.aggregate": task.Aggregate{}}
		if cfg.Demo {
			handlers["demo.wait"] = task.Wait{}
		}
		return worker.New(store, handlers, cfg, logger).Run(ctx)
	}
	api := server(cfg.HTTPAddr, httpapi.New(store, cfg, logger))
	go func() {
		if e := api.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errCh <- e
			stop()
		}
	}()
	logger.Info("api started", "address", cfg.HTTPAddr)
	<-ctx.Done()
	shutdownServer(api)
	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}
func server(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
}
func shutdownServer(s *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}
