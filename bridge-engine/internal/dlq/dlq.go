package dlq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

// RedriveCountHeader counts how many times a message has been resent from
// a dead-letter topic, so automatic redrive can stop after max_times.
const RedriveCountHeader = "X-Ark-Redrive-Count"

type Entry struct {
	ID       string `json:"id"`
	Redrives int    `json:"redrives"`
	// Reason says why the message ended up here, as recorded by ARK when
	// it routed it (callback status, last error, or the rule that matched).
	Reason        string    `json:"reason,omitempty"`
	FailedAt      string    `json:"failed_at,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Key           string    `json:"key,omitempty"`
	Value         string    `json:"value"`
	Timestamp     time.Time `json:"timestamp"`
	Partition     int       `json:"partition"`
	Offset        int64     `json:"offset"`
}

// Browser keeps the most recent entries of a dead-letter or reject topic
// for inspection, retry and discard. It reads every partition directly
// rather than through a consumer group, so every node in a cluster sees
// the same entries, and it hides entries the shared StateStore says were
// already retried or discarded, so they don't come back after a restart.
type Browser struct {
	brokers    []string
	topic      string
	pipeline   string
	maxEntries int
	state      *StateStore

	mu      sync.RWMutex
	entries []Entry

	retryTo *producer.Producer
	log     *slog.Logger
}

// NewBrowser shows the entries pipeline sent to topic. Several pipelines can
// share one topic, so entries another pipeline routed there are left out.
func NewBrowser(brokers []string, topic, pipeline string, state *StateStore, maxEntries int, retryTo *producer.Producer, log *slog.Logger) *Browser {
	return &Browser{
		brokers:    brokers,
		topic:      topic,
		pipeline:   pipeline,
		maxEntries: maxEntries,
		state:      state,
		retryTo:    retryTo,
		log:        log.With("dlq_topic", topic),
	}
}

func (b *Browser) Run(ctx context.Context) {
	partitions, err := b.partitions(ctx)
	for err != nil {
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, kafka.UnknownTopicOrPartition) {
			b.log.Debug("dlq topic not created yet, waiting for it", "error", err)
		} else {
			b.log.Error("listing dlq partitions failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		partitions, err = b.partitions(ctx)
	}

	var wg sync.WaitGroup
	for _, p := range partitions {
		wg.Add(2)
		go func(p kafka.Partition) {
			defer wg.Done()
			b.tail(ctx, p.ID)
		}(p)
		go func(p kafka.Partition) {
			defer wg.Done()
			b.pruneState(ctx, p.ID)
		}(p)
	}
	wg.Wait()
}

// pruneState periodically drops state for entries retention has removed.
func (b *Browser) pruneState(ctx context.Context, partition int) {
	if b.state == nil {
		return
	}
	ticker := time.NewTicker(tuning.DLQPrune())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		conn, err := kafkaadmin.DialLeaderAny(ctx, b.brokers, b.topic, partition)
		if err != nil {
			continue
		}
		first, err := conn.ReadFirstOffset()
		_ = conn.Close()
		if err != nil {
			continue
		}
		if n, err := b.state.Prune(ctx, b.topic, partition, first); err != nil {
			b.log.Warn("pruning dlq state failed", "error", err, "partition", partition)
		} else if n > 0 {
			b.log.Info("pruned dlq state for entries removed by retention", "entries", n, "partition", partition)
		}
	}
}

func (b *Browser) partitions(ctx context.Context) ([]kafka.Partition, error) {
	conn, err := kafkaadmin.DialAny(ctx, b.brokers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.ReadPartitions(b.topic)
}

// tail reads one partition starting maxEntries back from its end, since
// only the most recent entries are ever kept.
func (b *Browser) tail(ctx context.Context, partition int) {
	start := kafka.FirstOffset
	if conn, err := kafkaadmin.DialLeaderAny(ctx, b.brokers, b.topic, partition); err == nil {
		first, last, err := conn.ReadOffsets()
		_ = conn.Close()
		if err == nil {
			start = max(first, last-int64(b.maxEntries))
		}
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   b.brokers,
		Topic:     b.topic,
		Partition: partition,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   time.Second,
	})
	defer reader.Close()
	if err := reader.SetOffset(start); err != nil {
		b.log.Error("positioning dlq reader failed", "error", err, "partition", partition)
		return
	}

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.log.Error("dlq browser fetch failed", "error", err, "partition", partition)
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

// Close is kept for callers that manage the Browser's lifetime explicitly;
// readers are closed when Run's context ends.
func (b *Browser) Close() error { return nil }

func (b *Browser) add(msg kafka.Message) {
	redrives := 0
	var reason, failedAt, correlationID string
	for _, h := range msg.Headers {
		switch h.Key {
		case RedriveCountHeader:
			redrives, _ = strconv.Atoi(string(h.Value))
		case "X-Ark-Reason":
			reason = string(h.Value)
		case "X-Ark-Failed-At":
			failedAt = string(h.Value)
		case "X-Correlation-ID":
			correlationID = string(h.Value)
		case "X-Ark-Pipeline":
			if b.pipeline != "" && string(h.Value) != b.pipeline {
				return
			}
		}
	}
	entry := Entry{
		ID:            entryID(msg.Partition, msg.Offset),
		Redrives:      redrives,
		Reason:        reason,
		FailedAt:      failedAt,
		CorrelationID: correlationID,
		Key:           string(msg.Key),
		Value:         string(msg.Value),
		Timestamp:     msg.Time,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, entry)
	if len(b.entries) > b.maxEntries*2 {
		sort.Slice(b.entries, func(i, j int) bool { return b.entries[i].Timestamp.Before(b.entries[j].Timestamp) })
		b.entries = b.entries[len(b.entries)-b.maxEntries:]
	}
}

func (b *Browser) handled(id string) bool {
	if b.state == nil {
		return false
	}
	_, ok := b.state.Handled(b.topic, id)
	return ok
}

// List returns pending entries, oldest first, at most maxEntries.
func (b *Browser) List() []Entry {
	b.mu.RLock()
	out := make([]Entry, 0, len(b.entries))
	for _, e := range b.entries {
		if !b.handled(e.ID) {
			out = append(out, e)
		}
	}
	b.mu.RUnlock()

	sort.Slice(out, func(i, j int) bool { return out[i].Timestamp.Before(out[j].Timestamp) })
	if len(out) > b.maxEntries {
		out = out[len(out)-b.maxEntries:]
	}
	return out
}

func (b *Browser) Get(id string) (Entry, bool) {
	if b.handled(id) {
		return Entry{}, false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, e := range b.entries {
		if e.ID == id {
			return e, true
		}
	}
	return Entry{}, false
}

// Discard hides an entry on every node, durably. The message itself stays
// in the topic until retention removes it.
func (b *Browser) Discard(ctx context.Context, id string) (bool, error) {
	if _, ok := b.Get(id); !ok {
		return false, nil
	}
	if b.state == nil {
		return false, fmt.Errorf("dlq state store not configured")
	}
	if err := b.state.Mark(ctx, b.topic, id, StateDiscarded); err != nil {
		return false, fmt.Errorf("recording discard of %s: %w", id, err)
	}
	return true, nil
}

// Retry resends an entry to the pipeline's source topic and records that
// it was retried, so neither this node nor any other offers it again.
func (b *Browser) Retry(ctx context.Context, id string) error {
	entry, ok := b.Get(id)
	if !ok {
		return fmt.Errorf("dlq entry %s not found or already handled", id)
	}
	if b.state == nil {
		return fmt.Errorf("dlq state store not configured")
	}
	headers := map[string]string{RedriveCountHeader: strconv.Itoa(entry.Redrives + 1)}
	if err := b.retryTo.Send(ctx, []byte(entry.Key), []byte(entry.Value), headers); err != nil {
		return fmt.Errorf("retrying %s: %w", id, err)
	}
	if err := b.state.Mark(ctx, b.topic, id, StateRetried); err != nil {
		return fmt.Errorf("retried %s but recording it failed, it may be offered again: %w", id, err)
	}
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
