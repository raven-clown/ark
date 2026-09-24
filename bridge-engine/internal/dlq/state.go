package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
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

	var hw int64
	for {
		conn, err := kafka.DialLeader(ctx, "tcp", s.brokers[0], StateTopic, 0)
		if err == nil {
			hw, err = conn.ReadLastOffset()
			_ = conn.Close()
		}
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		s.log.Error("reading dlq state end offset failed", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	if hw == 0 {
		s.caughtUp.Store(true)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.brokers,
		Topic:       StateTopic,
		Partition:   0,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.log.Error("reading dlq state failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		s.mu.Lock()
		if msg.Value == nil {
			delete(s.handled, string(msg.Key))
		} else {
			s.handled[string(msg.Key)] = string(msg.Value)
		}
		s.mu.Unlock()
		if msg.Offset+1 >= hw {
			s.caughtUp.Store(true)
		}
	}
}
