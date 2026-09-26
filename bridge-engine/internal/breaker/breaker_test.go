package breaker

import (
	"testing"
	"time"
)

func TestStartsClosedAndAllows(t *testing.T) {
	b := New(3, 50*time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected a fresh breaker to allow")
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("expected initial state closed, got %q", got)
	}
}

func TestOpensAfterThresholdFailures(t *testing.T) {
	b := New(3, time.Hour)

	for i := 0; i < 2; i++ {
		b.RecordResult(false)
		if got := b.State(); got != "closed" {
			t.Fatalf("expected closed after %d failures, got %q", i+1, got)
		}
	}

	b.RecordResult(false)
	if got := b.State(); got != "open" {
		t.Fatalf("expected open after reaching threshold, got %q", got)
	}
	if b.Allow() {
		t.Fatal("expected Allow to return false while open and before cooldown elapses")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	b := New(3, time.Hour)

	b.RecordResult(false)
	b.RecordResult(false)
	b.RecordResult(true)
	b.RecordResult(false)
	b.RecordResult(false)

	if got := b.State(); got != "closed" {
		t.Fatalf("expected still closed since success reset the failure streak, got %q", got)
	}
}

func TestHalfOpenAfterCooldownThenCloses(t *testing.T) {
	b := New(1, 20*time.Millisecond)

	b.RecordResult(false)
	if got := b.State(); got != "open" {
		t.Fatalf("expected open, got %q", got)
	}

	if b.Allow() {
		t.Fatal("expected Allow to be false immediately after opening")
	}

	time.Sleep(30 * time.Millisecond)

	if !b.Allow() {
		t.Fatal("expected Allow to be true after the cooldown elapses")
	}
	if got := b.State(); got != "half_open" {
		t.Fatalf("expected half_open after Allow triggers the trial, got %q", got)
	}

	b.RecordResult(true)
	if got := b.State(); got != "closed" {
		t.Fatalf("expected closed after a successful half-open trial, got %q", got)
	}
}

func TestHalfOpenReopensOnFailure(t *testing.T) {
	b := New(1, 20*time.Millisecond)

	b.RecordResult(false)
	time.Sleep(30 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected Allow to be true after cooldown")
	}

	b.RecordResult(false)
	if got := b.State(); got != "open" {
		t.Fatalf("expected a failed half-open trial to reopen the breaker, got %q", got)
	}
}

func TestRepeatFailuresDoNotOpenAClosedBreaker(t *testing.T) {
	b := New(3, time.Hour)

	b.RecordResult(false)
	for i := 0; i < 10; i++ {
		b.RecordRepeatFailure()
	}
	if got := b.State(); got != "closed" {
		t.Fatalf("expected closed, retries of one failed message must not open it, got %q", got)
	}

	b.RecordResult(false)
	b.RecordResult(false)
	if got := b.State(); got != "open" {
		t.Fatalf("expected open after three different messages failed, got %q", got)
	}
}

func TestRepeatFailureReopensHalfOpen(t *testing.T) {
	b := New(1, 10*time.Millisecond)

	b.RecordResult(false)
	time.Sleep(20 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("expected a trial call after the cooldown")
	}
	b.RecordRepeatFailure()
	if got := b.State(); got != "open" {
		t.Fatalf("expected a failed trial to reopen the breaker, got %q", got)
	}
}
