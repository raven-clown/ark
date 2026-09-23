package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/producer"
)

type Entry struct {
	ID        string    `json:"id"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
}

type Browser struct {
	topic      string
	maxEntries int

	mu      sync.RWMutex
	entries []Entry

	reader  *kafka.Reader
	retryTo *producer.Producer
	log     *slog.Logger
}

func NewBrowser(brokers []string, topic, groupID string, maxEntries int, retryTo *producer.Producer, log *slog.Logger) *Browser {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		Topic:    topic,
		GroupID:  groupID,
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  time.Second,
	})

	return &Browser{
		topic:      topic,
		maxEntries: maxEntries,
		reader:     reader,
		retryTo:    retryTo,
		log:        log.With("dlq_topic", topic),
	}
}

func (b *Browser) Run(ctx context.Context) {
	for {
		msg, err := b.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.log.Error("dlq browser fetch failed", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		b.add(msg)
	}
}

func (b *Browser) Close() error {
	return b.reader.Close()
}

func (b *Browser) add(msg kafka.Message) {
	entry := Entry{
		ID:        entryID(msg.Partition, msg.Offset),
		Key:       string(msg.Key),
		Value:     string(msg.Value),
		Timestamp: msg.Time,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, entry)
	if len(b.entries) > b.maxEntries {
		b.entries = b.entries[len(b.entries)-b.maxEntries:]
	}
}

func (b *Browser) List() []Entry {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]Entry, len(b.entries))
	copy(out, b.entries)
	return out
}

func (b *Browser) Get(id string) (Entry, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, e := range b.entries {
		if e.ID == id {
			return e, true
		}
	}
	return Entry{}, false
}

func (b *Browser) Discard(id string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, e := range b.entries {
		if e.ID == id {
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			return true
		}
	}
	return false
}

func (b *Browser) Retry(ctx context.Context, id string) error {
	entry, ok := b.Get(id)
	if !ok {
		return fmt.Errorf("dlq entry %s not found", id)
	}
	if err := b.retryTo.Send(ctx, []byte(entry.Key), []byte(entry.Value), nil); err != nil {
		return fmt.Errorf("retrying %s: %w", id, err)
	}
	b.Discard(id)
	return nil
}

func entryID(partition int, offset int64) string {
	return strconv.Itoa(partition) + ":" + strconv.FormatInt(offset, 10)
}

func ParseEntryID(id string) (partition int, offset int64, err error) {
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid dlq entry id %q", id)
	}
	partition, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid dlq entry id %q: %w", id, err)
	}
	offset, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid dlq entry id %q: %w", id, err)
	}
	return partition, offset, nil
}
