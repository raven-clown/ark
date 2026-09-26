package dlq

import (
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestBrowserSkipsEntriesFromOtherPipelines(t *testing.T) {
	b := NewBrowser(nil, "shared.dlq", "orders", nil, 10, nil, slog.Default())
	b.add(kafka.Message{Offset: 1, Value: []byte("mine"), Headers: []kafka.Header{{Key: "X-Ark-Pipeline", Value: []byte("orders")}}})
	b.add(kafka.Message{Offset: 2, Value: []byte("theirs"), Headers: []kafka.Header{{Key: "X-Ark-Pipeline", Value: []byte("payments")}}})
	b.add(kafka.Message{Offset: 3, Value: []byte("unlabeled")})

	got := b.List()
	if len(got) != 2 || got[0].Value != "mine" || got[1].Value != "unlabeled" {
		t.Fatalf("expected this pipeline's entry and the unlabeled one, got %+v", got)
	}
}
