package callback

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
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
}

func NewCorrelationID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generating correlation id: %w", err)
	}
	return hex.EncodeToString(buf), nil
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
	}, nil
}

func (r *Response) Success() bool {
	return r.StatusCode >= 200 && r.StatusCode < 300
}
