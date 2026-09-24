package mcpserver

import (
	"context"
	"fmt"
	"math"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
)

type tuningOut struct {
	Pipeline        string    `json:"pipeline"`
	Measured        tuningNow `json:"measured"`
	Capacity        string    `json:"capacity"`
	Recommendations []Finding `json:"recommendations"`
	ConfigReview    []Finding `json:"config_review"`
	NodeSizing      string    `json:"node_sizing"`
}

type tuningNow struct {
	Workers         int     `json:"workers"`
	MaxInFlight     int     `json:"max_in_flight"`
	Partitions      int     `json:"source_partitions"`
	AvgCallbackMs   float64 `json:"avg_callback_ms"`
	CeilingPerSec   float64 `json:"estimated_ceiling_msgs_per_sec"`
	TargetPerSec    float64 `json:"target_msgs_per_sec,omitempty"`
	LatencyMeasured bool    `json:"latency_measured"`
}

// recommendTuning estimates what a pipeline can sustain from its measured
// callback latency (throughput ceiling is concurrency / latency) and
// suggests settings for a target rate, then reviews its config against
// ARK's best practices.
func recommendTuning(ctx context.Context, d Deps, p config.Pipeline, targetPerSec float64) tuningOut {
	out := tuningOut{Pipeline: p.Name}
	diag := diagnose(d, p)

	partitions, _ := kafkaadmin.PartitionCount(ctx, d.Brokers, p.SourceTopic)
	latency := diag.Numbers.AvgCallbackMs
	measured := latency > 0
	if !measured {
		latency = 100 // a conservative assumption until there's traffic to measure
	}
	workers := max(p.Workers, 1)
	if partitions > 0 {
		workers = min(workers, partitions)
	}
	ceiling := float64(workers*p.Concurrency.MaxInFlight) * 1000 / latency

	out.Measured = tuningNow{Workers: p.Workers, MaxInFlight: p.Concurrency.MaxInFlight, Partitions: partitions,
		AvgCallbackMs: math.Round(latency*10) / 10, CeilingPerSec: math.Round(ceiling), TargetPerSec: targetPerSec, LatencyMeasured: measured}
	basis := "measured"
	if !measured {
		basis = "assumed (no callbacks measured yet)"
	}
	out.Capacity = fmt.Sprintf("About %.0f msgs/s at most: %d effective worker(s) x %d in flight / %.0fms callback latency (%s). Per-key ordering lowers this if a few keys carry most of the traffic, since one key is processed one message at a time.",
		ceiling, workers, p.Concurrency.MaxInFlight, latency, basis)

	add := func(list *[]Finding, sev, what, why string, actions ...string) {
		*list = append(*list, Finding{Severity: sev, What: what, Why: why, Actions: actions})
	}

	if targetPerSec > 0 {
		needed := targetPerSec * latency / 1000 // concurrent requests needed
		if needed <= float64(workers*p.Concurrency.MaxInFlight) {
			add(&out.Recommendations, SeverityOK, fmt.Sprintf("Current settings can reach %.0f msgs/s (needs about %.0f requests in flight, has %d).", targetPerSec, math.Ceil(needed), workers*p.Concurrency.MaxInFlight), "")
		} else {
			maxW := workers
			if partitions > 0 {
				maxW = partitions
			}
			perWorker := int(math.Ceil(needed / float64(maxW)))
			rec := fmt.Sprintf("To reach %.0f msgs/s you need about %.0f requests in flight.", targetPerSec, math.Ceil(needed))
			actions := []string{fmt.Sprintf("Set workers: %d (one per partition) and concurrency.max_in_flight: %d.", maxW, perWorker)}
			if perWorker > 100 {
				actions = append(actions, fmt.Sprintf("That's a lot per worker; add partitions to %s so more workers can share the load (Kafka can't have more active consumers than partitions).", p.SourceTopic))
			}
			actions = append(actions, "Check the target can take that many concurrent requests, or it will answer 429 and ARK will slow down anyway.", "Making the callback faster raises the ceiling as much as adding concurrency does.")
			add(&out.Recommendations, SeverityWarning, rec, fmt.Sprintf("Right now the ceiling is about %.0f msgs/s.", ceiling), actions...)
		}
	}

	if partitions > 0 && p.Workers > partitions {
		add(&out.Recommendations, SeverityInfo, fmt.Sprintf("workers (%d) is more than the source topic's partitions (%d); the extra workers sit idle.", p.Workers, partitions), "Kafka gives each partition to one consumer in a group.",
			fmt.Sprintf("Lower workers to %d, or add partitions to %s.", partitions, p.SourceTopic))
	}
	if measured && float64(p.Target.TimeoutMs) < 3*latency {
		add(&out.Recommendations, SeverityWarning, fmt.Sprintf("target.timeout_ms (%d) is less than 3x the average callback latency (%.0fms).", p.Target.TimeoutMs, latency), "Normal slow requests will time out and count as failures.",
			fmt.Sprintf("Raise target.timeout_ms to at least %d.", int(math.Ceil(latency*5/1000))*1000))
	}

	out.ConfigReview = reviewConfig(d, p)
	out.NodeSizing = "ARK spends most of its time waiting on the network, so CPU is rarely the limit: as a rough guide, 1 vCPU handles a few thousand msgs/s of small payloads. Memory is roughly 50MB plus workers x max_in_flight x average message size (request and response are both held while in flight). Measure with ark_callback_duration_seconds and consumer lag before resizing; these are estimates, not guarantees."
	return out
}

// reviewConfig checks a pipeline against ARK's best practices. It's shared
// by recommend_tuning and validate_pipeline_config.
func reviewConfig(d Deps, p config.Pipeline) []Finding {
	var out []Finding
	add := func(sev, what, why string, actions ...string) {
		out = append(out, Finding{Severity: sev, What: what, Why: why, Actions: actions})
	}
	if p.RejectTopic == "" {
		add(SeverityInfo, "No reject_topic.", "Messages the target rejects as invalid (4xx) go to the dead-letter topic, mixed with ones that failed for other reasons.",
			"Add reject_topic to keep bad data separate from delivery failures.")
	}
	if p.Target.HealthCheckURL == "" && len(p.Target.HealthCheckURLs) == 0 {
		add(SeverityInfo, "No target health check URL.", "When the circuit breaker opens, ARK only notices the target recovered when it retries.",
			"Set target.health_check_url so recovery is detected right away.")
	}
	if p.Retry.MaxAttempts <= 1 {
		add(SeverityWarning, "retry.max_attempts is 1.", "A single transient failure sends the message straight to the dead-letter topic.",
			"Use 3 or more attempts.")
	}
	if p.DeadLetterTopic != "" && !p.DeadLetterRedrive.Enabled() {
		add(SeverityInfo, "No dead_letter_redrive.", "Dead-lettered messages stay there until someone retries them by hand.",
			"If most failures are temporary, add dead_letter_redrive: {after_seconds: 3600, max_times: 3}.")
	}
	if p.Ordering == config.OrderingNone {
		add(SeverityInfo, "ordering: none.", "Messages with the same key may reach the target out of order.",
			"Use per_key (the default) unless order truly doesn't matter.")
	}
	if p.OnExhausted == config.OnExhaustedBlock {
		add(SeverityWarning, "on_exhausted: block.", "One message that keeps failing holds back everything after it on its partition.",
			"Prefer a dead_letter_topic unless strict in-order delivery matters more than progress.")
	}
	if d.ReplicationFactor == 1 {
		add(SeverityWarning, "topics.replication_factor is 1.", "Topics ARK creates (DLQ, reject, internal) are lost if the broker holding them fails.",
			"Use 3 in production.")
	}
	if p.MCPAccess == config.MCPAccessReadWrite {
		add(SeverityInfo, "mcp_access: read_write.", "AI agents with operator/admin tokens can pause this pipeline, retry DLQ entries and change its config.")
	}
	if p.Tenant == "" {
		add(SeverityInfo, "No tenant.", "Metrics and logs can't be filtered per team.", "Set tenant if several teams share this ARK.")
	}
	return out
}
