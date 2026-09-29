package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
)

// BatchSizeHeader tells a batch target how many items the body holds.
const BatchSizeHeader = "X-Ark-Batch-Size"

type batchItem struct {
	id        string
	key       []byte
	value     []byte
	partition int
	done      chan batchResult
}

type batchResult struct {
	resp    *callback.Response
	url     string
	elapsed time.Duration
	err     error
	noURL   bool
}

// batcher collects the messages that are waiting on one caller and posts
// them together, so every message keeps its own retries, rules, ordering
// and commit while the target sees one request per batch.
type batcher struct {
	c      *caller
	size   int
	linger time.Duration
	mu     sync.Mutex
	queue  []*batchItem
	timer  *time.Timer
}

func newBatcher(c *caller) *batcher {
	linger := c.target.BatchLingerMs
	if linger == 0 {
		linger = 5
	}
	return &batcher{c: c, size: c.target.BatchSize, linger: time.Duration(linger) * time.Millisecond}
}

type batchRequest struct {
	Items []batchRequestItem `json:"items"`
}

type batchRequestItem struct {
	ID    string `json:"id"`
	Key   string `json:"key,omitempty"`
	Value any    `json:"value"`
}

type batchResponse struct {
	Results []struct {
		ID     string          `json:"id"`
		Status int             `json:"status"`
		Body   json.RawMessage `json:"body"`
	} `json:"results"`
}

// submit queues one message and waits for its own result.
func (b *batcher) submit(ctx context.Context, it *batchItem) (batchResult, error) {
	it.done = make(chan batchResult, 1)
	if full := b.enqueue(it); full != nil {
		go b.flush(full) // #nosec G118 -- the batch carries other messages, so one message's cancelled ctx must not cancel it; the client timeout bounds the call
	}
	select {
	case r := <-it.done:
		return r, nil
	case <-ctx.Done():
		return batchResult{}, ctx.Err()
	}
}

// enqueue adds it and returns the batch when that filled it; otherwise the
// linger timer sends what is queued.
func (b *batcher) enqueue(it *batchItem) []*batchItem {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.queue = append(b.queue, it)
	if len(b.queue) >= b.size {
		return b.take()
	}
	if b.timer == nil {
		b.timer = time.AfterFunc(b.linger, b.flushQueued)
	}
	return nil
}

func (b *batcher) take() []*batchItem {
	items := b.queue
	b.queue = nil
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	return items
}

func (b *batcher) flushQueued() {
	b.mu.Lock()
	items := b.take()
	b.mu.Unlock()
	if len(items) > 0 {
		b.flush(items)
	}
}

func (b *batcher) flush(items []*batchItem) {
	answer := func(r func(*batchItem) batchResult) {
		for _, it := range items {
			it.done <- r(it)
		}
	}
	url, release, ok := b.c.pool.Pick(items[0].partition)
	if !ok {
		answer(func(*batchItem) batchResult { return batchResult{noURL: true} })
		return
	}
	req := batchRequest{Items: make([]batchRequestItem, len(items))}
	for i, it := range items {
		var v any = string(it.value)
		if json.Valid(it.value) {
			v = json.RawMessage(it.value)
		}
		req.Items[i] = batchRequestItem{ID: it.id, Key: string(it.key), Value: v}
	}
	body, err := json.Marshal(req)
	if err != nil {
		release()
		answer(func(*batchItem) batchResult {
			return batchResult{url: url, err: fmt.Errorf("encoding the batch: %w", err)}
		})
		return
	}
	headers := expandHeaders(b.c.headers)
	if headers == nil {
		headers = map[string]string{}
	}
	headers[BatchSizeHeader] = strconv.Itoa(len(items))

	start := time.Now()
	resp, err := b.c.client.Send(context.Background(), http.MethodPost, url, items[0].id, body, headers)
	release()
	elapsed := time.Since(start)
	if err != nil || !resp.Success() {
		// The whole request failed or answered one status for everything:
		// each message sees it as if it had been sent alone.
		answer(func(*batchItem) batchResult { return batchResult{resp: resp, url: url, elapsed: elapsed, err: err} })
		return
	}
	var out batchResponse
	if err := json.Unmarshal(resp.Body, &out); err != nil {
		answer(func(*batchItem) batchResult {
			return batchResult{url: url, elapsed: elapsed, err: fmt.Errorf("target %s answered a batch with something other than {\"results\": [...]}: %w", url, err)}
		})
		return
	}
	byID := make(map[string]int, len(out.Results))
	for i, r := range out.Results {
		byID[r.ID] = i
	}
	answer(func(it *batchItem) batchResult {
		i, found := byID[it.id]
		if !found {
			return batchResult{url: url, elapsed: elapsed, err: fmt.Errorf("target %s left message %s out of its batch results", url, it.id)}
		}
		r := out.Results[i]
		status := r.Status
		if status == 0 {
			status = http.StatusOK
		}
		return batchResult{url: url, elapsed: elapsed, resp: &callback.Response{StatusCode: status, Body: itemBody(r.Body), CorrelationID: it.id}}
	})
}

// itemBody is what a result's body becomes as a Kafka value: a JSON string
// as its text, anything else as JSON, nothing as an empty value.
func itemBody(raw json.RawMessage) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return []byte(s)
		}
	}
	return raw
}
