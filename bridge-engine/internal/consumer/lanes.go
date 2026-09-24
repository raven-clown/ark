package consumer

import "sync"

// laneSet serializes jobs that share a key while letting different keys
// run concurrently: each new job for a key waits on the previous job for
// that key to finish.
type laneSet struct {
	mu    sync.Mutex
	tails map[string]chan struct{}
}

func newLanes() *laneSet {
	return &laneSet{tails: make(map[string]chan struct{})}
}

// acquire registers a job for key. wait is nil when nothing is ahead of it;
// otherwise the job must not start until wait is closed. release must be
// called exactly once when the job finishes.
func (l *laneSet) acquire(key string) (wait <-chan struct{}, release func()) {
	if key == "" {
		return nil, func() {}
	}
	mine := make(chan struct{})

	l.mu.Lock()
	prev := l.tails[key]
	l.tails[key] = mine
	l.mu.Unlock()

	release = func() {
		close(mine)
		l.mu.Lock()
		if l.tails[key] == mine {
			delete(l.tails, key)
		}
		l.mu.Unlock()
	}
	if prev == nil {
		return nil, release
	}
	return prev, release
}
