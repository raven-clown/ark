package tap

import (
	"strings"
	"testing"
)

func TestWatchingOnlyWhileSubscribed(t *testing.T) {
	h := NewHub()
	if h.Watching("orders") {
		t.Fatal("watching with no subscribers")
	}
	s, stop := h.Subscribe("orders", 4)
	if !h.Watching("orders") || h.Watching("payments") {
		t.Fatal("watching should be per pipeline")
	}
	h.Publish(Record{Pipeline: "orders", Stage: StageIn})
	h.Publish(Record{Pipeline: "payments", Stage: StageIn})
	if got := (<-s.C).Stage; got != StageIn {
		t.Fatalf("stage = %q", got)
	}
	select {
	case r := <-s.C:
		t.Fatalf("got a record for another pipeline: %+v", r)
	default:
	}
	stop()
	stop()
	if h.Watching("orders") {
		t.Fatal("still watching after stop")
	}
}

func TestSlowWatcherDropsInsteadOfBlocking(t *testing.T) {
	h := NewHub()
	s, stop := h.Subscribe("orders", 2)
	defer stop()
	for range 5 {
		h.Publish(Record{Pipeline: "orders"})
	}
	if s.Dropped() != 3 {
		t.Fatalf("dropped = %d, want 3", s.Dropped())
	}
}

func TestSetValueTruncates(t *testing.T) {
	var r Record
	r.SetValue([]byte(strings.Repeat("x", MaxValueBytes+10)))
	if len(r.Value) != MaxValueBytes || !r.Truncated {
		t.Fatalf("len=%d truncated=%v", len(r.Value), r.Truncated)
	}
}
