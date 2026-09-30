package source

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/callback"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/datarules"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/metrics"
	"github.com/raven-clown/ark/bridge-engine/internal/producer"
	"github.com/raven-clown/ark/bridge-engine/internal/sink"
)

// Header names ARK sets on messages that came in through a source.
const (
	SourceHeader = "X-Ark-Source"
)

type Source interface {
	Name() string
	Topic() string
}

type httpSource struct {
	cfg   config.Source
	out   sink.Sink
	prod  *producer.Producer
	rules *datarules.Checker
}

func (s *httpSource) Name() string  { return s.cfg.Name }
func (s *httpSource) Topic() string { return s.cfg.Topic }

// HTTP serves POST /ingest/<name> for every http source in the config.
type HTTP struct {
	brokers []string
	rf      int
	log     *slog.Logger
	mu      sync.RWMutex
	sources map[string]*httpSource
	// newSink is the Kafka writer for a topic; tests swap it.
	newSink func(topic string) (sink.Sink, *producer.Producer)
}

func NewHTTP(brokers []string, replicationFactor int, log *slog.Logger) *HTTP {
	return &HTTP{brokers: brokers, rf: replicationFactor, log: log, sources: map[string]*httpSource{},
		newSink: func(topic string) (sink.Sink, *producer.Producer) {
			p := producer.New(brokers, topic)
			return sink.Kafka{P: p}, p
		}}
}

func (h *HTTP) Set(ctx context.Context, list []config.Source) error {
	next := map[string]*httpSource{}
	var errs []error
	h.mu.RLock()
	old := h.sources
	h.mu.RUnlock()
	for _, c := range list {
		s := &httpSource{cfg: c}
		if c.DataRules != nil {
			chk, err := datarules.New(*c.DataRules)
			if err != nil {
				errs = append(errs, fmt.Errorf("source %s: %w", c.Name, err))
				continue
			}
			s.rules = chk
		}
		if prev, ok := old[c.Name]; ok && prev.cfg.Topic == c.Topic {
			s.out, s.prod = prev.out, prev.prod
		} else {
			if len(h.brokers) > 0 {
				if err := kafkaadmin.EnsureTopic(ctx, h.brokers, c.Topic, c.Partitions, h.rf); err != nil {
					errs = append(errs, fmt.Errorf("source %s: ensuring topic %s: %w", c.Name, c.Topic, err))
					continue
				}
			}
			s.out, s.prod = h.newSink(c.Topic)
		}
		next[c.Name] = s
	}
	h.mu.Lock()
	h.sources = next
	h.mu.Unlock()
	for name, s := range old {
		if n, ok := next[name]; (!ok || n.prod != s.prod) && s.prod != nil {
			_ = s.prod.Close()
		}
	}
	return errors.Join(errs...)
}

// List is the sources being served, for the console.
func (h *HTTP) List() []config.Source {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]config.Source, 0, len(h.sources))
	for _, s := range h.sources {
		out = append(out, s.cfg)
	}
	return out
}

type answer struct {
	Accepted      bool                  `json:"accepted"`
	Source        string                `json:"source,omitempty"`
	Topic         string                `json:"topic,omitempty"`
	CorrelationID string                `json:"correlation_id,omitempty"`
	Error         string                `json:"error,omitempty"`
	Violations    []datarules.Violation `json:"violations,omitempty"`
}

func (h *HTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	name := strings.Trim(strings.TrimPrefix(r.URL.Path, "/ingest/"), "/")
	h.mu.RLock()
	s, ok := h.sources[name]
	h.mu.RUnlock()
	reply := func(status int, a answer, outcome string) {
		if ok {
			metrics.SourceRequests.WithLabelValues(name, outcome).Inc()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(a)
	}
	if !ok {
		reply(http.StatusNotFound, answer{Error: "no source named " + name}, "")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		reply(http.StatusMethodNotAllowed, answer{Error: "send POST"}, "bad_request")
		return
	}
	token := os.Getenv(s.cfg.TokenEnv)
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if token == "" || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="ark"`)
		reply(http.StatusUnauthorized, answer{Error: "missing or wrong bearer token"}, "unauthorized")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(s.cfg.MaxBodyBytes)))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			reply(http.StatusRequestEntityTooLarge, answer{Error: fmt.Sprintf("body is over max_body_bytes (%d)", s.cfg.MaxBodyBytes)}, "too_large")
			return
		}
		reply(http.StatusBadRequest, answer{Error: "reading the body: " + err.Error()}, "bad_request")
		return
	}

	var key []byte
	if s.cfg.Key != "" || s.rules != nil {
		var obj map[string]any
		if err := json.Unmarshal(body, &obj); err != nil {
			reply(http.StatusBadRequest, answer{Error: "body must be a JSON object"}, "bad_request")
			return
		}
		if s.cfg.Key != "" {
			if v, found := lookup(obj, s.cfg.Key); found && v != nil {
				key = []byte(fmt.Sprint(v))
			}
		}
	}
	if s.rules != nil {
		if vs := s.rules.Check(key, nil, body); len(vs) > 0 {
			reply(http.StatusUnprocessableEntity, answer{Error: datarules.Summary(vs), Violations: vs}, "rejected")
			return
		}
	}

	correlationID := r.Header.Get(callback.CorrelationIDHeader)
	headers := map[string]string{SourceHeader: name}
	if correlationID != "" {
		headers[callback.CorrelationIDHeader] = correlationID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := s.out.Write(ctx, sink.Message{Key: key, Value: body, Headers: headers}); err != nil {
		h.log.Error("source could not write to Kafka", "source", name, "topic", s.cfg.Topic, "error", err)
		w.Header().Set("Retry-After", "5")
		reply(http.StatusServiceUnavailable, answer{Error: "Kafka didn't take the message; send it again"}, "unavailable")
		return
	}
	reply(http.StatusAccepted, answer{Accepted: true, Source: name, Topic: s.cfg.Topic, CorrelationID: correlationID}, "accepted")
}

func lookup(obj map[string]any, path string) (any, bool) {
	var cur any = obj
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[part]; !ok {
			return nil, false
		}
	}
	return cur, true
}
