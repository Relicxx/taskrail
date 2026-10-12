package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Relicxx/taskrail/internal/config"
	"github.com/Relicxx/taskrail/internal/task"
)

type taskHandlers struct {
	store Store
	cfg   config.Config
}

func (h *taskHandlers) notFound(c *gin.Context) {
	respond(c, 404, "not_found")
}

func (h *taskHandlers) methodNotAllowed(c *gin.Context) {
	respond(c, 405, "method_not_allowed")
}

func (h *taskHandlers) health(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
}

func (h *taskHandlers) ready(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()
	if h.store.Ping(ctx) != nil {
		respond(c, 503, "not_ready")
		return
	}

	c.JSON(200, gin.H{"status": "ready"})
}

func (h *taskHandlers) create(c *gin.Context) {
	key, ok := idempotency(c)
	if !ok {
		return
	}

	in, ok := readCreateInput(c, h.cfg.Demo)
	if !ok {
		return
	}

	v, replay, e := h.store.Create(c.Request.Context(), c.GetString("client"), key, in)
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
}

func readCreateInput(c *gin.Context, demo bool) (task.CreateInput, bool) {
	media, _, e := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if e != nil || media != "application/json" {
		respond(c, 415, "unsupported_media_type")
		return task.CreateInput{}, false
	}

	body, e := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	if e != nil {
		var max *http.MaxBytesError
		if errors.As(e, &max) {
			respond(c, 413, "body_too_large")
		} else {
			respond(c, 400, "invalid_body")
		}

		return task.CreateInput{}, false
	}

	var in task.CreateInput
	if task.StrictJSON(body, &in) != nil {
		respond(c, 400, "invalid_json")
		return task.CreateInput{}, false
	}

	var fields map[string]json.RawMessage
	_ = json.Unmarshal(body, &fields)

	for _, name := range []string{"max_attempts", "timeout_ms"} {
		if raw, present := fields[name]; present {
			var n *int
			if json.Unmarshal(raw, &n) != nil || n == nil || *n <= 0 {
				respond(c, 400, "invalid_limits")
				return task.CreateInput{}, false
			}
		}
	}

	in, e = task.Normalize(in, demo)
	if e != nil {
		handleError(c, e)
		return task.CreateInput{}, false
	}

	return in, true
}

func (h *taskHandlers) list(c *gin.Context) {
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

	items, next, e := h.store.List(c.Request.Context(), c.GetString("client"), c.Query("cursor"), limit)
	if e != nil {
		handleError(c, e)
		return
	}

	c.JSON(200, gin.H{"items": items, "next_cursor": next})
}

func (h *taskHandlers) get(c *gin.Context) {
	v, e := h.store.Get(c.Request.Context(), c.GetString("client"), c.Param("id"))
	if e != nil {
		handleError(c, e)
		return
	}

	c.JSON(200, v)
}

func (h *taskHandlers) attempts(c *gin.Context) {
	v, e := h.store.Attempts(c.Request.Context(), c.GetString("client"), c.Param("id"))
	if e != nil {
		handleError(c, e)
		return
	}

	c.JSON(200, gin.H{"items": v})
}

func (h *taskHandlers) cancel(c *gin.Context) {
	v, e := h.store.Cancel(c.Request.Context(), c.GetString("client"), c.Param("id"))
	if e != nil {
		handleError(c, e)
		return
	}

	c.JSON(200, v)
}

func (h *taskHandlers) retry(c *gin.Context) {
	key, ok := idempotency(c)
	if !ok {
		return
	}

	v, replay, e := h.store.Retry(c.Request.Context(), c.GetString("client"), c.Param("id"), key)
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
}
