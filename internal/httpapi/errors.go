package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Relicxx/taskrail/internal/task"
)

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
	c.AbortWithStatusJSON(status, gin.H{
		"code":       code,
		"message":    http.StatusText(status),
		"request_id": c.GetString("request_id"),
	})
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
