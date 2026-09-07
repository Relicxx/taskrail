package task

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	for _, raw := range []string{`{"numbers":[]}`, `{"numbers":[1],"x":2}`, `{"numbers":[1],"numbers":[2]}`, `{"numbers":[1]} {}`, `{"numbers":[1e999]}`} {
		if _, err := Normalize(CreateInput{Type: "report.aggregate", Payload: json.RawMessage(raw)}, false); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, n := range []int{-1, 11} {
		if _, err := Normalize(CreateInput{Type: "report.aggregate", Payload: json.RawMessage(`{"numbers":[1]}`), MaxAttempts: n}, false); err == nil {
			t.Errorf("accepted max %d", n)
		}
	}
	for _, n := range []int{-1, 301000} {
		if _, err := Normalize(CreateInput{Type: "report.aggregate", Payload: json.RawMessage(`{"numbers":[1]}`), TimeoutMS: n}, false); err == nil {
			t.Errorf("accepted timeout %d", n)
		}
	}
	if _, err := Normalize(CreateInput{Type: "demo.wait", Payload: json.RawMessage(`{"duration_ms":10}`)}, false); err == nil {
		t.Fatal("demo allowed")
	}
}
func TestAggregate(t *testing.T) {
	r, e := (Aggregate{}).Execute(context.Background(), json.RawMessage(`{"numbers":[1,2,3]}`))
	if e != nil {
		t.Fatal(e)
	}
	var got struct {
		Count               int
		Sum, Min, Max, Mean float64
	}
	if e = json.Unmarshal(r, &got); e != nil {
		t.Fatal(e)
	}
	if got.Count != 3 || got.Sum != 6 || got.Min != 1 || got.Max != 3 || got.Mean != 2 {
		t.Fatal(string(r))
	}
}
func TestOverflow(t *testing.T) {
	_, e := (Aggregate{}).Execute(context.Background(), json.RawMessage(`{"numbers":[1e308,1e308]}`))
	if e == nil || !IsPermanent(e) {
		t.Fatal("overflow must be permanent")
	}
}
func TestWaitCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, e := (Wait{}).Execute(ctx, json.RawMessage(`{"duration_ms":100}`))
	if e == nil {
		t.Fatal("ignored context")
	}
}
func TestBackoff(t *testing.T) {
	if Backoff(1, .5) != 500*time.Millisecond || Backoff(10000, 1) > 60*time.Second {
		t.Fatal("backoff bounds")
	}
}
func TestNormalizeDefaults(t *testing.T) {
	in, e := Normalize(CreateInput{Type: "report.aggregate", Payload: json.RawMessage(`{"numbers":[1]}`)}, false)
	if e != nil || in.MaxAttempts != 5 || in.TimeoutMS != 60000 {
		t.Fatal(in, e)
	}
	a, _ := Hash(in)
	time.Sleep(time.Millisecond)
	b, _ := Hash(in)
	if a != b {
		t.Fatal("unstable immediate hash")
	}
}
