package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkatail"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

// StateTopic records which dead-letter/reject entries have already been
// retried or discarded. It's compacted and keyed by "<topic>/<entry id>",
// so every node (and every restart) agrees on what is still pending.
const StateTopic = "__ark_dlq_state"

const (
	StateRetried   = "retried"
	StateDiscarded = "discarded"
)

type StateStore struct {
	brokers []string
	writer  *producer.Producer
	log     *slog.Logger

	caughtUp atomic.Bool

	mu      sync.RWMutex
	handled map[string]string
}

func NewStateStore(ctx context.Context, brokers []string, replicationFactor int, log *slog.Logger) (*StateStore, error) {
	if err := kafkaadmin.EnsureCompactedTopic(ctx, brokers, StateTopic, 1, replicationFactor); err != nil {
		return nil, fmt.Errorf("ensuring %s exists: %w", StateTopic, err)
	}
	return &StateStore{
		brokers: brokers,
		writer:  producer.New(brokers, StateTopic),
		log:     log.With("component", "dlq-state"),
		handled: make(map[string]string),
	}, nil
}

func stateKey(topic, id string) string { return topic + "/" + id }

// Handled reports whether an entry was already retried or discarded.
func (s *StateStore) Handled(topic, id string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.handled[stateKey(topic, id)]
	return state, ok
}

// Mark durably records an entry's state before updating the local view, so
// a successful return means every node will see it too.
func (s *StateStore) Mark(ctx context.Context, topic, id, state string) error {
	key := stateKey(topic, id)
	if err := s.writer.Send(ctx, []byte(key), []byte(state), nil); err != nil {
		return err
	}
	s.mu.Lock()
	s.handled[key] = state
	s.mu.Unlock()
	return nil
}

// Prune tombstones the state of every entry on topic/partition below
// firstOffset: those records were already removed by retention, so their
// state can never matter again. This keeps the state topic and every
// node's memory from growing forever.
func (s *StateStore) Prune(ctx context.Context, topic string, partition int, firstOffset int64) (int, error) {
	prefix := topic + "/" + strconv.Itoa(partition) + ":"
	s.mu.RLock()
	var stale []string
	for key := range s.handled {
		rest, ok := strings.CutPrefix(key, prefix)
		if !ok {
			continue
		}
		if off, err := strconv.ParseInt(rest, 10, 64); err == nil && off < firstOffset {
			stale = append(stale, key)
		}
	}
	s.mu.RUnlock()
	if len(stale) == 0 {
		return 0, nil
	}

	msgs := make([]kafka.Message, len(stale))
	for i, key := range stale {
		msgs[i] = kafka.Message{Key: []byte(key)}
	}
	if err := s.writer.SendMany(ctx, msgs...); err != nil {
		return 0, err
	}
	s.mu.Lock()
	for _, key := range stale {
		delete(s.handled, key)
	}
	s.mu.Unlock()
	return len(stale), nil
}

// WaitCaughtUp blocks until the existing state has been read, or ctx ends.
func (s *StateStore) WaitCaughtUp(ctx context.Context) {
	for !s.caughtUp.Load() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (s *StateStore) Run(ctx context.Context) {
	defer s.writer.Close()
	kafkatail.Compacted(ctx, s.brokers, StateTopic, s.log,
		func(msg kafka.Message) {
			s.mu.Lock()
			if msg.Value == nil {
				delete(s.handled, string(msg.Key))
			} else {
				s.handled[string(msg.Key)] = string(msg.Value)
			}
			s.mu.Unlock()
		},
		func() { s.caughtUp.Store(true) })
}
