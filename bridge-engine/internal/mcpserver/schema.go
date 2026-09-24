package mcpserver

// fieldDoc describes one pipeline config field for get_pipeline_schema, so
// an agent can write valid config without guessing.
type fieldDoc struct {
	Field       string   `json:"field"`
	Type        string   `json:"type"`
	Required    string   `json:"required"`
	Default     string   `json:"default,omitempty"`
	Allowed     []string `json:"allowed,omitempty"`
	Description string   `json:"description"`
}

var pipelineSchema = []fieldDoc{
	{"name", "string", "yes", "", nil, "Unique pipeline name."},
	{"tenant", "string", "no", "", nil, "Team or owner label, added to every metric and log line."},
	{"enabled", "bool", "no", "true", nil, "false keeps the pipeline configured but not running."},
	{"mcp_access", "string", "no", "read_only", []string{"read_only", "read_write", "none"}, "What AI agents may do with this pipeline. none hides it from MCP entirely; read_write lets operator/admin tokens pause it, retry its DLQ and change its config."},
	{"source_topic", "string", "yes", "", nil, "Kafka topic to consume. Created with `workers` partitions if missing."},
	{"destination_topic", "string", "yes (enabled)", "", nil, "Where the callback's response body is produced."},
	{"dead_letter_topic", "string", "yes (enabled), unless on_exhausted: block", "", nil, "Where messages go when retries are exhausted, so nothing is silently dropped."},
	{"reject_topic", "string", "no", "", nil, "Where messages the target rejects as invalid (4xx) go. Without it they go to dead_letter_topic."},
	{"on_exhausted", "string", "no", "", []string{"block"}, "block: keep retrying a failing message in place (holding back its partition) instead of requiring a dead_letter_topic."},
	{"consumer_group", "string", "yes (enabled)", "", nil, "Kafka consumer group. Every ARK process with the same group shares the partitions."},
	{"workers", "int", "no", "1", nil, "Consumers for this pipeline (cluster-wide in cluster mode). More than the source topic's partitions just idle."},
	{"ordering", "string", "no", "per_key", []string{"per_key", "per_partition", "none"}, "per_key: same-key messages one at a time, in order; other keys in parallel."},
	{"concurrency.max_in_flight", "int", "no", "10", nil, "Callbacks in flight per worker."},
	{"retry.max_attempts", "int", "no", "3", nil, "Callback attempts before dead-lettering (429/425/408 don't count)."},
	{"retry.backoff_ms", "int", "no", "1000", nil, "Wait between attempts."},
	{"target.mode", "string", "no", "single_url", []string{"single_url", "multi_url"}, "One endpoint, or a pool of them."},
	{"target.url", "string", "yes (single_url)", "", nil, "Endpoint ARK POSTs each message to. The response body becomes the destination message."},
	{"target.urls", "[]string", "yes (multi_url)", "", nil, "Endpoints for multi_url."},
	{"target.strategy", "string", "no", "round_robin", []string{"round_robin", "least_inflight", "sticky_partition"}, "How multi_url picks an endpoint."},
	{"target.health_check_url", "string", "no", "", nil, "Probed while the circuit breaker is open, so recovery is noticed without new traffic."},
	{"target.health_check_urls", "[]string", "no", "", nil, "One per target.urls entry, for multi_url."},
	{"target.health_check_interval_seconds", "int", "no", "10", nil, "Probe interval."},
	{"target.timeout_ms", "int", "no", "30000", nil, "Callback timeout."},
	{"target.reject_statuses", "[]int", "no", "every 4xx except 408, 425, 429", nil, "Statuses routed to reject_topic."},
	{"fast_path_rules", "[]rule", "no", "", nil, "Rules checked before the callback: {name, condition, action, destination_override | webhook_override}. condition uses expr syntax over `data` (the JSON message), e.g. \"data.amount < 1000\" (use nil, not null)."},
	{"post_callback_rules", "[]rule", "no", "", nil, "Same shape, checked after the callback, with `response.status` and `response.body` available."},
	{"rule.action", "string", "yes (per rule)", "", []string{"pass_through", "reject", "drop", "dead_letter", "transform_route"}, "transform_route needs exactly one of destination_override or webhook_override."},
	{"dead_letter_redrive", "{after_seconds, max_times}", "no", "", nil, "Resend dead-lettered messages automatically once they're after_seconds old, at most max_times each."},
	{"placement.node_selector", "map", "no", "", nil, "Cluster mode: only run on nodes whose cluster.labels contain all these key/values."},
}

const exampleYAML = `name: orders-to-fraud-check
tenant: payments
mcp_access: read_write
source_topic: orders.raw
destination_topic: orders.checked
dead_letter_topic: orders.dlq
reject_topic: orders.rejected
consumer_group: orders-fraud-check
workers: 3
target:
  url: http://fraud-service:8080/check
  health_check_url: http://fraud-service:8080/health
  timeout_ms: 5000
retry:
  max_attempts: 3
  backoff_ms: 1000
fast_path_rules:
  - name: skip-small-orders
    condition: "data.amount < 100"
    action: pass_through
dead_letter_redrive:
  after_seconds: 3600
  max_times: 3
`
