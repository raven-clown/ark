package events

import (
	"strconv"
	"testing"
	"time"
)

func TestRecentNewestFirstAndFiltered(t *testing.T) {
	l := New(10)
	for i := 0; i < 15; i++ {
		p := "a"
		if i%2 == 1 {
			p = "b"
		}
		l.Add(Event{Pipeline: p, Kind: MessageRetrying, Message: strconv.Itoa(i)})
	}
	l.Add(Event{Pipeline: "a", Kind: Paused, Message: "p"})

	all := l.Recent("", time.Time{}, 100)
	if len(all) != 10 {
		t.Fatalf("ring of 10 should hold 10 events, got %d", len(all))
	}
	if all[0].Message != "p" {
		t.Errorf("newest first: got %q", all[0].Message)
	}
	onlyB := l.Recent("b", time.Time{}, 100)
	for _, e := range onlyB {
		if e.Pipeline != "b" {
			t.Fatalf("pipeline filter leaked %q", e.Pipeline)
		}
	}
	paused := l.Recent("", time.Time{}, 100, Paused)
	if len(paused) != 1 {
		t.Errorf("kind filter: want 1 pause, got %d", len(paused))
	}
	if got := l.Recent("", time.Time{}, 3); len(got) != 3 {
		t.Errorf("limit: want 3, got %d", len(got))
	}
}
