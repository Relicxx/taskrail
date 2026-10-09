package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
)

type Store interface {
	Create(context.Context, string, string, task.CreateInput) (task.Task, bool, error)
	Get(context.Context, string, string) (task.Task, error)
	List(context.Context, string, string, int) ([]task.Task, string, error)
	Attempts(context.Context, string, string) ([]task.Attempt, error)
	Cancel(context.Context, string, string) (task.Task, error)
	Retry(context.Context, string, string, string) (task.Task, bool, error)
	Ping(context.Context) error
}

func New(store Store, cfg config.Config, logger *slog.Logger) http.Handler {
	handlers := &taskHandlers{store: store, cfg: cfg}
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	_ = r.SetTrustedProxies(nil)
	r.Use(requestID())
	r.Use(requestObserver(logger))
	r.NoRoute(handlers.notFound)
	r.NoMethod(handlers.methodNotAllowed)
	r.HandleMethodNotAllowed = true
	r.GET("/healthz", handlers.health)
	r.GET("/readyz", handlers.ready)

	g := r.Group("/v1")
	g.Use(clientAccess(cfg))
	g.POST("/tasks", handlers.create)
	g.GET("/tasks", handlers.list)
	g.GET("/tasks/:id", handlers.get)
	g.GET("/tasks/:id/attempts", handlers.attempts)
	g.POST("/tasks/:id/cancel", handlers.cancel)
	g.POST("/tasks/:id/retry", handlers.retry)
	return r
}
