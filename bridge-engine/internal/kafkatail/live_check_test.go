//go:build livekafka

package kafkatail

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
)

func TestLiveCatchUp(t *testing.T) {
	brokers := []string{"localhost:9092"}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tc := range []struct {
		topic   string
		records int
	}{{"tailtest.empty." + time.Now().Format("150405"), 0}, {"tailtest.full." + time.Now().Format("150405"), 50}} {
		if err := kafkaadmin.EnsureCompactedTopic(ctx, brokers, tc.topic, 1, 1); err != nil {
			t.Fatal(err)
		}
		if tc.records > 0 {
			w := &kafka.Writer{Addr: kafka.TCP(brokers...), Topic: tc.topic, BatchTimeout: 5 * time.Millisecond}
			for i := 0; i < tc.records; i++ {
				if err := w.WriteMessages(ctx, kafka.Message{Key: []byte{byte(i)}, Value: []byte("v")}); err != nil {
					t.Fatal(err)
				}
			}
			w.Close()
		}

		seen, caught := 0, make(chan int, 1)
		tctx, tcancel := context.WithCancel(ctx)
		start := time.Now()
		go Compacted(tctx, brokers, tc.topic, log, func(kafka.Message) { seen++ }, func() { caught <- seen })
		select {
		case n := <-caught:
			t.Logf("%s: caught up after %v having read %d of %d records", tc.topic, time.Since(start).Round(time.Millisecond), n, tc.records)
			if n != tc.records {
				t.Errorf("caught up before reading everything: %d of %d", n, tc.records)
			}
		case <-time.After(20 * time.Second):
			t.Errorf("%s: never caught up", tc.topic)
		}
		tcancel()
	}
}
