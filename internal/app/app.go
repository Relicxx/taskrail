package app

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/httpapi"
	"github.com/Relicxx/taskrail/internal/postgres"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/Relicxx/taskrail/internal/worker"
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

	pool, e := openPool(ctx, cfg.DatabaseURL)
	if e != nil {
		return e
	}

	defer pool.Close()

	store := postgres.New(pool)
	errCh := make(chan error, 1)
	if mode == "worker" {
		return runWorker(ctx, cfg, store, logger, errCh)
	}

	return runAPI(ctx, cfg, store, logger, errCh, stop)
}

func openPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pc, e := pgxpool.ParseConfig(databaseURL)
	if e != nil {
		return nil, errors.New("invalid database config")
	}

	pc.ConnConfig.ConnectTimeout = 3 * time.Second
	pc.MaxConns = 16

	pool, e := pgxpool.NewWithConfig(ctx, pc)
	if e != nil {
		return nil, errors.New("database pool initialization failed")
	}

	ready, cancel := context.WithTimeout(ctx, 3*time.Second)
	e = pool.Ping(ready)
	cancel()
	if e != nil {
		pool.Close()
		return nil, errors.New("database unavailable")
	}

	return pool, nil
}

func serve(s *http.Server, errCh chan<- error, stop context.CancelFunc) {
	go func() {
		if e := s.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			errCh <- e
			stop()
		}
	}()
}

func runWorker(ctx context.Context, cfg config.Config, store *postgres.Store, logger *slog.Logger, errCh <-chan error) error {
	handlers := map[string]task.Handler{
		"report.aggregate": task.Aggregate{},
	}

	if cfg.Demo {
		handlers["demo.wait"] = task.Wait{}
	}

	if e := worker.New(store, handlers, cfg, logger).Run(ctx); e != nil {
		return e
	}

	select {
	case e := <-errCh:
		return e
	default:
		return nil
	}
}

func runAPI(ctx context.Context, cfg config.Config, store *postgres.Store, logger *slog.Logger, errCh chan error, stop context.CancelFunc) error {
	api := server(cfg.HTTPAddr, httpapi.New(store, cfg, logger))
	serve(api, errCh, stop)
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
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}

func shutdownServer(s *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = s.Shutdown(ctx)
}
