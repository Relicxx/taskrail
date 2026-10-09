package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"log/slog"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/Relicxx/taskrail/internal/config"
)

type window struct {
	start time.Time
	used  int
}

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := uuid.NewString()
		c.Set("request_id", id)
		c.Header("X-Request-ID", id)
		c.Next()
	}
}

func requestObserver(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
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
		logger.Info("http request",
			"method", c.Request.Method,
			"route", route,
			"status", c.Writer.Status(),
			"request_id", c.GetString("request_id"),
		)
	}
}

func clientAccess(cfg config.Config) gin.HandlerFunc {
	var mu sync.Mutex
	windows := map[string]window{}
	slots := make(chan struct{}, 64)
	return func(c *gin.Context) {
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
			defer func() {
				<-slots
			}()
		default:
			respond(c, 429, "busy")
			return
		}
		ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
		defer cancel()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}
