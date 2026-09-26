package breaker

import (
	"sync"
	"time"
)

type state int

const (
	closed state = iota
	open
	halfOpen
)

type Breaker struct {
	mu               sync.Mutex
	state            state
	failureThreshold int
	cooldown         time.Duration
	consecutiveFails int
	openedAt         time.Time
}

func New(failureThreshold int, cooldown time.Duration) *Breaker {
	return &Breaker{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
	}
}

func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case open:
		if time.Since(b.openedAt) >= b.cooldown {
			b.state = halfOpen
			return true
		}
		return false
	default:
		return true
	}
}

func (b *Breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case open:
		return "open"
	case halfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

func (b *Breaker) RecordResult(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if success {
		b.consecutiveFails = 0
		b.state = closed
		return
	}

	b.consecutiveFails++
	if b.state == halfOpen || b.consecutiveFails >= b.failureThreshold {
		b.state = open
		b.openedAt = time.Now()
	}
}

// RecordRepeatFailure records another failed attempt at a message whose
// earlier attempt already counted. It doesn't lengthen the failure streak,
// so a few messages the target always fails on can't open the breaker for
// every other message; it still reopens a half-open breaker.
func (b *Breaker) RecordRepeatFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == halfOpen {
		b.state = open
		b.openedAt = time.Now()
	}
}
