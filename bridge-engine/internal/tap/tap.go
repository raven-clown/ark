// Package tap lets the console watch messages move through a pipeline
// live. Publishing costs one atomic load while nobody is watching, and a
// watcher that can't keep up loses records instead of slowing the
// pipeline down.
package tap

import (
	"sync"
	"sync/atomic"
	"time"
)

// Stages a record can describe.
const (
	StageIn       = "in"       // consumed from the source topic
	StageCallback = "callback" // one callback attempt finished
	StageOut      = "out"      // left the pipeline (see Record.To)
)

// Where a message went when it left (Record.To).
const (
	ToDestination = "destination"
	ToReject      = "reject"
	ToDLQ         = "dlq"
	ToDropped     = "dropped"
	ToOverride    = "override" // post/fast path destination_override topic
	ToWebhook     = "webhook"
)

// MaxValueBytes caps how much of a payload a record carries.
const MaxValueBytes = 4096

type Record struct {
	Time          time.Time         `json:"time"`
	Pipeline      string            `json:"pipeline"`
	Stage         string            `json:"stage"`
	To            string            `json:"to,omitempty"`
	Topic         string            `json:"topic,omitempty"`
	Partition     *int              `json:"partition,omitempty"`
	Offset        *int64            `json:"offset,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Key           string            `json:"key,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Value         string            `json:"value,omitempty"`
	Truncated     bool              `json:"truncated,omitempty"`
	Target        string            `json:"target,omitempty"`
	Status        int               `json:"status,omitempty"`
	Attempt       int               `json:"attempt,omitempty"`
	DurationMs    float64           `json:"duration_ms,omitempty"`
	Rule          string            `json:"rule,omitempty"`
	Reason        string            `json:"reason,omitempty"`
}

// SetValue stores v, cut to MaxValueBytes.
func (r *Record) SetValue(v []byte) {
	if len(v) > MaxValueBytes {
		v = v[:MaxValueBytes]
		r.Truncated = true
	}
	r.Value = string(v)
}

type Sub struct {
	C       <-chan Record
	ch      chan Record
	dropped atomic.Int64
}

// Dropped is how many records this watcher missed because it fell behind.
func (s *Sub) Dropped() int64 { return s.dropped.Load() }

type Hub struct {
	watchers atomic.Int64
	mu       sync.RWMutex
	subs     map[string]map[*Sub]struct{}
}

func NewHub() *Hub { return &Hub{subs: make(map[string]map[*Sub]struct{})} }

// Default is the hub the pipelines on this node publish to.
var Default = NewHub()

// Watching reports whether anyone is tailing pipeline. Callers check it
// before building a Record so an unwatched pipeline pays nothing.
func (h *Hub) Watching(pipeline string) bool {
	if h.watchers.Load() == 0 {
		return false
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs[pipeline]) > 0
}

func (h *Hub) Publish(r Record) {
	if r.Time.IsZero() {
		r.Time = time.Now()
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs[r.Pipeline] {
		select {
		case s.ch <- r:
		default:
			s.dropped.Add(1)
		}
	}
}

// Subscribe starts watching pipeline. Call the returned func to stop.
func (h *Hub) Subscribe(pipeline string, buffer int) (*Sub, func()) {
	if buffer <= 0 {
		buffer = 256
	}
	ch := make(chan Record, buffer)
	s := &Sub{C: ch, ch: ch}
	h.mu.Lock()
	if h.subs[pipeline] == nil {
		h.subs[pipeline] = make(map[*Sub]struct{})
	}
	h.subs[pipeline][s] = struct{}{}
	h.mu.Unlock()
	h.watchers.Add(1)
	var once sync.Once
	return s, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[pipeline], s)
			if len(h.subs[pipeline]) == 0 {
				delete(h.subs, pipeline)
			}
			h.mu.Unlock()
			h.watchers.Add(-1)
		})
	}
}
