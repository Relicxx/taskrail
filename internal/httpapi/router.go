package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"sync"
	"time"
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
type window struct {
	start time.Time
	used  int
}

func New(store Store, cfg config.Config, logger *slog.Logger) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	_ = r.SetTrustedProxies(nil)
	r.Use(func(c *gin.Context) {
		id := uuid.NewString()
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	})
	r.Use(func(c *gin.Context) {
		defer func() {
			if recover() != nil {
				respond(c, 500, "internal_error")
				logger.Error("http handler panic", "request_id", c.GetString("request_id"))
			}
		}()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		logger.Info("http request", "method", c.Request.Method, "route", route, "status", c.Writer.Status(), "request_id", c.GetString("request_id"))
	})
	r.NoRoute(func(c *gin.Context) { respond(c, 404, "not_found") })
	r.NoMethod(func(c *gin.Context) { respond(c, 405, "method_not_allowed") })
	r.HandleMethodNotAllowed = true
	r.GET("/healthz", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		if store.Ping(ctx) != nil {
			respond(c, 503, "not_ready")
			return
		}
		c.JSON(200, gin.H{"status": "ready"})
	})
	var mu sync.Mutex
	windows := map[string]window{}
	slots := make(chan struct{}, 64)
	g := r.Group("/v1")
	g.Use(func(c *gin.Context) {
		given := sha256.Sum256([]byte(c.GetHeader("X-API-Key")))
		client := ""
		for id, key := range cfg.Keys {
			expected := sha256.Sum256([]byte(key))
			if subtle.ConstantTimeCompare(given[:], expected[:]) == 1 {
				client = id
			}
		}
		if client == "" {
			respond(c, 401, "unauthorized")
			return
		}
		c.Set("client", client)
		mu.Lock()
		v := windows[client]
		now := time.Now()
		if now.Sub(v.start) >= time.Second {
			v = window{start: now}
		}
		v.used++
		windows[client] = v
		mu.Unlock()
		if v.used > 100 {
			c.Header("Retry-After", "1")
			respond(c, 429, "rate_limited")
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			respond(c, 429, "busy")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	})
	g.POST("/tasks", func(c *gin.Context) {
		key, ok := idempotency(c)
		if !ok {
			return
		}
		media, _, e := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if e != nil || media != "application/json" {
			respond(c, 415, "unsupported_media_type")
			return
		}
		body, e := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
		if e != nil {
			var max *http.MaxBytesError
			if errors.As(e, &max) {
				respond(c, 413, "body_too_large")
			} else {
				respond(c, 400, "invalid_body")
			}
			return
		}
		var in task.CreateInput
		if task.StrictJSON(body, &in) != nil {
			respond(c, 400, "invalid_json")
			return
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(body, &fields)
		for _, name := range []string{"max_attempts", "timeout_ms"} {
			if raw, present := fields[name]; present {
				var n *int
				if json.Unmarshal(raw, &n) != nil || n == nil || *n <= 0 {
					respond(c, 400, "invalid_limits")
					return
				}
			}
		}
		in, e = task.Normalize(in, cfg.Demo)
		if e != nil {
			handleError(c, e)
			return
		}
		v, replay, e := store.Create(c.Request.Context(), c.GetString("client"), key, in)
		if e != nil {
			handleError(c, e)
			return
		}
		code := 201
		if replay {
			code = 200
		}
		c.Header("Location", "/v1/tasks/"+v.ID)
		c.JSON(code, v)
	})
	g.GET("/tasks", func(c *gin.Context) {
		limit := 20
		if raw := c.Query("limit"); raw != "" {
			var e error
			limit, e = strconv.Atoi(raw)
			if e != nil || limit < 1 || limit > 100 {
				respond(c, 400, "invalid_limit")
				return
			}
		}
		if len(c.Query("cursor")) > 1024 {
			respond(c, 400, "invalid_cursor")
			return
		}
		items, next, e := store.List(c.Request.Context(), c.GetString("client"), c.Query("cursor"), limit)
		if e != nil {
			handleError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": items, "next_cursor": next})
	})
	g.GET("/tasks/:id", func(c *gin.Context) {
		v, e := store.Get(c.Request.Context(), c.GetString("client"), c.Param("id"))
		if e != nil {
			handleError(c, e)
			return
		}
		c.JSON(200, v)
	})
	g.GET("/tasks/:id/attempts", func(c *gin.Context) {
		v, e := store.Attempts(c.Request.Context(), c.GetString("client"), c.Param("id"))
		if e != nil {
			handleError(c, e)
			return
		}
		c.JSON(200, gin.H{"items": v})
	})
	g.POST("/tasks/:id/cancel", func(c *gin.Context) {
		v, e := store.Cancel(c.Request.Context(), c.GetString("client"), c.Param("id"))
		if e != nil {
			handleError(c, e)
			return
		}
		c.JSON(200, v)
	})
	g.POST("/tasks/:id/retry", func(c *gin.Context) {
		key, ok := idempotency(c)
		if !ok {
			return
		}
		v, replay, e := store.Retry(c.Request.Context(), c.GetString("client"), c.Param("id"), key)
		if e != nil {
			handleError(c, e)
			return
		}
		code := 201
		if replay {
			code = 200
		}
		c.Header("Location", "/v1/tasks/"+v.ID)
		c.JSON(code, v)
	})
	return r
}
func idempotency(c *gin.Context) (string, bool) {
	k := c.GetHeader("Idempotency-Key")
	if len(k) < 1 || len(k) > 128 {
		respond(c, 400, "invalid_idempotency_key")
		return "", false
	}
	for _, b := range []byte(k) {
		if b < 33 || b > 126 {
			respond(c, 400, "invalid_idempotency_key")
			return "", false
		}
	}
	return k, true
}
func respond(c *gin.Context, status int, code string) {
	c.AbortWithStatusJSON(status, gin.H{"code": code, "message": http.StatusText(status), "request_id": c.GetString("request_id")})
}
func handleError(c *gin.Context, e error) {
	switch {
	case errors.Is(e, task.ErrInvalid):
		respond(c, 400, "invalid_input")
	case errors.Is(e, task.ErrNotFound):
		respond(c, 404, "not_found")
	case errors.Is(e, task.ErrConflict):
		respond(c, 409, "conflict")
	case errors.Is(e, context.DeadlineExceeded):
		respond(c, 503, "unavailable")
	default:
		respond(c, 500, "internal_error")
	}
}
