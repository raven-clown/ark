package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	Processed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_processed_total",
		Help: "Messages successfully processed and produced to destination_topic.",
	}, []string{"pipeline", "worker"})

	Rejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_rejected_total",
		Help: "Messages routed to reject_topic after a 4xx callback response.",
	}, []string{"pipeline", "worker"})

	DeadLettered = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_dead_lettered_total",
		Help: "Messages routed to dead_letter_topic after retries were exhausted.",
	}, []string{"pipeline", "worker"})

	Failed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ark_messages_failed_total",
		Help: "Messages that failed processing with no DLQ/reject topic to route to.",
	}, []string{"pipeline", "worker"})

	CallbackDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ark_callback_duration_seconds",
		Help:    "Latency of HTTP callback calls to a pipeline's target.",
		Buckets: prometheus.DefBuckets,
	}, []string{"pipeline"})

	ConsumerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_consumer_lag",
		Help: "Messages waiting in source_topic that this worker hasn't consumed yet.",
	}, []string{"pipeline", "worker"})

	CircuitBreakerOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_circuit_breaker_open",
		Help: "1 if a pipeline's circuit breaker is open (skipping callbacks), 0 otherwise.",
	}, []string{"pipeline"})

	Paused = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_pipeline_paused",
		Help: "1 if a pipeline is paused, 0 otherwise.",
	}, []string{"pipeline"})

	WorkerUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_worker_up",
		Help: "1 while a pipeline worker's Run loop is active, 0 once it has stopped or crashed.",
	}, []string{"pipeline", "worker"})

	LastActivityTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ark_last_activity_timestamp_seconds",
		Help: "Unix timestamp of the last message this worker finished handling (success, reject, or dead-letter).",
	}, []string{"pipeline", "worker"})
)

func init() {
	prometheus.MustRegister(Processed, Rejected, DeadLettered, Failed, CallbackDuration, ConsumerLag, CircuitBreakerOpen, Paused, WorkerUp, LastActivityTimestamp)
}
