package consumer

import "sync"

type laneSet struct {
	mu    sync.Mutex
	tails map[string]chan struct{}
}

func newLanes() *laneSet {
	return &laneSet{tails: make(map[string]chan struct{})}
}

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
