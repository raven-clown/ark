package kafkatail

import (
	"context"
	"errors"

	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

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
			fetchCtx, cancel = context.WithTimeout(ctx, tuning.CompactedIdle())
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
