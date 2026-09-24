package cluster

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
)

func placementMsg(t *testing.T, pipeline string, epoch int64, assign map[string]int) kafka.Message {
	t.Helper()
	val, err := json.Marshal(placementRecord{Pipeline: pipeline, Epoch: epoch, Assignments: assign})
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Key: []byte(pipeline), Value: val}
}

func TestApplyIgnoresDeposedLeader(t *testing.T) {
	p := &placer{assignments: map[string]map[string]int{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	p.apply(placementMsg(t, "orders", 5, map[string]int{"a": 2, "b": 2}))
	p.apply(placementMsg(t, "orders", 4, map[string]int{"a": 4}))

	if got := p.assignments["orders"]["a"]; got != 2 {
		t.Fatalf("a late write from epoch 4 overrode epoch 5: node a has %d workers", got)
	}
}

func TestApplyTombstoneRemovesPipeline(t *testing.T) {
	p := &placer{assignments: map[string]map[string]int{}, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	p.apply(placementMsg(t, "orders", 1, map[string]int{"a": 1}))
	p.apply(kafka.Message{Key: []byte("orders")})

	if _, ok := p.assignments["orders"]; ok {
		t.Fatal("expected a tombstone to remove the pipeline's placement")
	}
}
