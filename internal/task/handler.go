package task

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"time"
)

type Aggregate struct{}

func (Aggregate) Execute(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	n, err := decodeNumbers(raw)
	if err != nil {
		return nil, Permanent(ErrInvalid)
	}
	min, max, sum := n.Numbers[0], n.Numbers[0], 0.
	for _, v := range n.Numbers {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		sum += v
		if math.IsInf(sum, 0) || math.IsNaN(sum) {
			return nil, Permanent(errors.New("numeric overflow"))
		}
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return json.Marshal(struct {
		Count int     `json:"count"`
		Sum   float64 `json:"sum"`
		Min   float64 `json:"min"`
		Max   float64 `json:"max"`
		Mean  float64 `json:"mean"`
	}{len(n.Numbers), sum, min, max, sum / float64(len(n.Numbers))})
}

type Wait struct{}

func (Wait) Execute(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
	var d Duration
	if StrictJSON(raw, &d) != nil || d.DurationMS < 1 || d.DurationMS > 300000 {
		return nil, Permanent(ErrInvalid)
	}
	timer := time.NewTimer(time.Duration(d.DurationMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return json.RawMessage(`{"completed":true}`), nil
	}
}
