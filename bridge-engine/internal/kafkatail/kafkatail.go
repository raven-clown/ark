// Package kafkatail reads single-partition compacted topics that hold
// latest-value-per-key state (cluster placements, config, control, DLQ
// state) into memory.
package kafkatail

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

// idleCatchUp is how long a read may go quiet before the existing records
// are considered fully read. It covers a topic whose last records were
// removed by compaction, where there is nothing left to see "lag reach 0"
// on.
const idleCatchUp = 2 * time.Second

// Compacted tails partition 0 of topic from the beginning until ctx ends.
// It calls onRecord for every record and onCaughtUp exactly once, after the
// records that existed when it started have been read. Catch-up is
// detected from the reader's own lag reaching zero, or from the topic
// going quiet, never from comparing against an offset that compaction may
// have deleted.
func Compacted(ctx context.Context, brokers []string, topic string, log *slog.Logger, onRecord func(kafka.Message), onCaughtUp func()) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		Partition:   0,
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	caughtUp := false
	markCaughtUp := func() {
		if !caughtUp {
			caughtUp = true
			if onCaughtUp != nil {
				onCaughtUp()
			}
		}
	}

	for {
		fetchCtx, cancel := ctx, context.CancelFunc(func() {})
		if !caughtUp {
			fetchCtx, cancel = context.WithTimeout(ctx, idleCatchUp)
		}
		msg, err := reader.FetchMessage(fetchCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !caughtUp && errors.Is(err, context.DeadlineExceeded) {
				markCaughtUp()
				continue
			}
			log.Error("reading compacted topic failed", "topic", topic, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		onRecord(msg)
		if !caughtUp && reader.Lag() <= 0 {
			markCaughtUp()
		}
	}
}
