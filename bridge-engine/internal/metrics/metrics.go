package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Processed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_processed_total",
		Help: "Messages successfully processed and produced to destination_topic.",
	}, []string{"pipeline", "worker", "tenant"})

	Rejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_rejected_total",
		Help: "Messages routed to reject_topic after a 4xx callback response.",
	}, []string{"pipeline", "worker", "tenant"})

	DeadLettered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_dead_lettered_total",
		Help: "Messages routed to dead_letter_topic after retries were exhausted.",
	}, []string{"pipeline", "worker", "tenant"})

	Failed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_failed_total",
		Help: "Messages that failed processing with no DLQ/reject topic to route to.",
	}, []string{"pipeline", "worker", "tenant"})

	Backpressured = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_backpressured_total",
		Help: "Messages left uncommitted because the circuit breaker was open (destination-wide outage), not routed to DLQ since the message itself isn't the problem.",
	}, []string{"pipeline", "worker", "tenant"})

	CallbackDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ark_callback_duration_seconds",
		Help:    "Latency of HTTP callback calls to a pipeline's target.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline", "tenant"})

	OldestUncommittedAge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_oldest_uncommitted_age_seconds",
		Help: "How long the oldest fetched-but-uncommitted message on a worker has been waiting. Everything fetched after it is also held back from commit, so a high value means a crash now would redeliver a large batch.",
	}, []string{"pipeline", "worker", "tenant"})

	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_consumer_lag",
		Help: "Messages waiting in source_topic that this worker hasn't consumed yet.",
	}, []string{"pipeline", "worker", "tenant"})

	CircuitBreakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_circuit_breaker_open",
		Help: "1 if a pipeline's circuit breaker is open (skipping callbacks), 0 otherwise.",
	}, []string{"pipeline", "tenant"})

	Paused = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_pipeline_paused",
		Help: "1 if a pipeline is paused, 0 otherwise.",
	}, []string{"pipeline", "tenant"})

	WorkerUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_worker_up",
		Help: "1 while a pipeline worker's Run loop is active, 0 once it has stopped or crashed.",
	}, []string{"pipeline", "worker", "tenant"})

	LastActivityTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_last_activity_timestamp_seconds",
		Help: "Unix timestamp of the last message this worker finished handling (success, reject, or dead-letter).",
	}, []string{"pipeline", "worker", "tenant"})

	FastPathMatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_fast_path_rule_matches_total",
		Help: "Messages matched by a fast_path_rule, skipping the HTTP callback entirely.",
	}, []string{"pipeline", "rule", "action", "tenant"})

	PostCallbackMatches = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_post_callback_rule_matches_total",
		Help: "Callback responses matched by a post_callback_rule.",
	}, []string{"pipeline", "rule", "action", "tenant"})
)

func init() {
	prometheus.MustRegister(Processed, Rejected, DeadLettered, Failed, Backpressured, CallbackDuration, ConsumerLag, OldestUncommittedAge, CircuitBreakerOpen, Paused, WorkerUp, LastActivityTimestamp, FastPathMatches, PostCallbackMatches)
}
