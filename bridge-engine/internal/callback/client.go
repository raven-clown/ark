package callback

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const CorrelationIDHeader = "X-Correlation-ID"

type Client struct {
	http *http.Client
}

func NewClient(timeout time.Duration) *Client {
	return &Client{
		http: &http.Client{Timeout: timeout},
	}
}

type Response struct {
	StatusCode    int
	Body          []byte
	CorrelationID string
	RetryAfter    time.Duration
}

// MessageCorrelationID is stable for a given Kafka record, so every retry
// and every redelivery after a crash carries the same ID and the target
// can use it as an idempotency key.
func MessageCorrelationID(topic string, partition int, offset int64) string {
	sum := sha256.Sum256([]byte(topic + "/" + strconv.Itoa(partition) + "/" + strconv.FormatInt(offset, 10)))
	return hex.EncodeToString(sum[:16])
}

// RetryLater reports the 4xx statuses that mean "come back later" rather
// than "this message is invalid". They don't use up a retry attempt and
// Retry-After is honored.
func (r *Response) RetryLater() bool {
	switch r.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return true
	}
	return false
}

func parseRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func (c *Client) Post(ctx context.Context, url string, correlationID string, payload []byte) (*Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("building callback request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(CorrelationIDHeader, correlationID)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling %s: %w", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading callback response from %s: %w", url, err)
	}

	return &Response{
		StatusCode:    resp.StatusCode,
		Body:          body,
		CorrelationID: correlationID,
		RetryAfter:    parseRetryAfter(resp.Header.Get("Retry-After")),
	}, nil
}

func (r *Response) Success() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}

func (c *Client) Probe(ctx context.Context, url string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building health check request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("probing %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return fmt.Errorf("probing %s: status %d", url, resp.StatusCode)
	}
	return nil
}
