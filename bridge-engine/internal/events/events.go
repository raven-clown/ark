// Package events keeps a bounded, in-memory history of the things an
// operator (or an AI agent answering one) needs to explain what happened
// and why: pipelines starting and stopping, pauses, breaker trips, messages
// retried, rejected or dead-lettered, and the reason for each.
package events

import (
	"sync"
	"time"
)

type Kind string

const (
	PipelineStarted     Kind = "pipeline_started"
	PipelineStartFailed Kind = "pipeline_start_failed"
	PipelineStopped     Kind = "pipeline_stopped"
	PipelineResized     Kind = "pipeline_resized"
	Paused              Kind = "paused"
	Resumed             Kind = "resumed"
	BreakerOpened       Kind = "breaker_opened"
	BreakerClosed       Kind = "breaker_closed"
	MessageRetrying     Kind = "message_retrying"
	MessageRejected     Kind = "message_rejected"
	MessageDeadLettered Kind = "message_dead_lettered"
	TargetRateLimited   Kind = "target_rate_limited"
	WorkerRestarted     Kind = "worker_restarted"
	ConfigApplied       Kind = "config_applied"
	LeaderChanged       Kind = "leader_changed"
	DLQRedriven         Kind = "dlq_redriven"
)

type Event struct {
	Time     time.Time         `json:"time"`
	Pipeline string            `json:"pipeline,omitempty"`
	Kind     Kind              `json:"kind"`
	Message  string            `json:"message"`
	Details  map[string]string `json:"details,omitempty"`
}

type Log struct {
	mu   sync.Mutex
	ring []Event
	next int
	full bool
}

func New(size int) *Log {
	return &Log{ring: make([]Event, size)}
}

func (l *Log) Add(e Event) {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ring[l.next] = e
	l.next = (l.next + 1) % len(l.ring)
	if l.next == 0 {
		l.full = true
	}
}

// Recent returns up to limit events, newest first. An empty pipeline
// matches every pipeline; an empty kinds list matches every kind.
func (l *Log) Recent(pipeline string, since time.Time, limit int, kinds ...Kind) []Event {
	want := make(map[Kind]bool, len(kinds))
	for _, k := range kinds {
		want[k] = true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	n := l.next
	if l.full {
		n = len(l.ring)
	}
	out := make([]Event, 0, min(limit, n))
	for i := 1; i <= n && len(out) < limit; i++ {
		e := l.ring[(l.next-i+len(l.ring))%len(l.ring)]
		if pipeline != "" && e.Pipeline != pipeline {
			continue
		}
		if !since.IsZero() && e.Time.Before(since) {
			break
		}
		if len(want) > 0 && !want[e.Kind] {
			continue
		}
		out = append(out, e)
	}
	return out
}

// Default is the process-wide log every package records into, the same way
// Prometheus has a default registry.
var Default = New(2000)

func Record(pipeline string, kind Kind, message string, details map[string]string) {
	Default.Add(Event{Pipeline: pipeline, Kind: kind, Message: message, Details: details})
}
