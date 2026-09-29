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
	{"destination_topic", "string", "yes (enabled, without flow)", "", nil, "Where the callback's response body is produced. Not used by a flow pipeline."},
	{"dead_letter_topic", "string", "yes (enabled, without flow), unless on_exhausted: block", "", nil, "Where messages go when retries are exhausted, so nothing is silently dropped. In a flow it is only a fallback for steps that don't say where their failures go."},
	{"reject_topic", "string", "no", "", nil, "Where messages the target rejects as invalid (4xx) go. Without it they go to dead_letter_topic. In a flow it is only a fallback."},
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
	{"target.batch_size", "int", "no", "0 (one message per call)", nil, "2 to 1000 sends that many messages in one POST as {\"items\": [{\"id\", \"key\", \"value\"}]}; the target answers {\"results\": [{\"id\", \"status\", \"body\"}]} and each message is routed by its own status. Needs workers x max_in_flight of at least batch_size to fill."},
	{"target.batch_linger_ms", "int", "no", "5", nil, "The longest a message waits for its batch to fill."},
	{"fast_path_rules", "[]rule", "no", "", nil, "Rules checked before the callback: {name, condition, action, destination_override | webhook_override}. condition uses expr syntax over `data` (the JSON message), e.g. \"data.amount < 1000\" (use nil, not null)."},
	{"post_callback_rules", "[]rule", "no", "", nil, "Same shape, checked after the callback, with `response.status` and `response.body` available."},
	{"rule.action", "string", "yes (per rule)", "", []string{"pass_through", "reject", "drop", "dead_letter", "transform_route"}, "transform_route needs exactly one of destination_override or webhook_override."},
	{"dead_letter_redrive", "{after_seconds, max_times}", "no", "", nil, "Resend dead-lettered messages automatically once they're after_seconds old, at most max_times each."},
	{"placement.node_selector", "map", "no", "", nil, "Cluster mode: only run on nodes whose cluster.labels contain all these key/values."},
	{"circuit_breaker", "{failure_threshold, cooldown_seconds}", "no", "5, 30", nil, "After failure_threshold failures in a row, stop calling the target and hold messages for cooldown_seconds."},
	{"data_rules.on_violation", "string", "no", "reject", []string{"reject", "dead_letter", "tag"}, "What happens to a message that breaks a data rule. tag lets it through with an X-Ark-Violations header, useful to observe before enforcing."},
	{"data_rules.allow_unknown_fields", "bool", "no", "true", nil, "false flags top-level fields no rule mentions."},
	{"data_rules.max_bytes", "int", "no", "", nil, "Largest allowed message body."},
	{"data_rules.key", "{required, pattern, enum, max_length}", "no", "", nil, "Rules for the Kafka message key."},
	{"data_rules.headers", "[]{name, required, pattern, enum, max_length}", "no", "", nil, "Rules for Kafka headers."},
	{"data_rules.fields", "[]{path, required, type, min, max, min_length, max_length, pattern, enum, format}", "no", "", nil, "Rules for JSON body fields by dot path (customer.id). type: string, number, integer, boolean, object, array, null. format: email, uuid, date-time, date, url, ipv4. Checked before fast_path_rules and before the callback."},
	{"flow", "{start, steps}", "no", "", nil, "Replaces target, destination_topic and the rules with steps joined in any shape without loops, like a workflow. start lists the first steps. A message is committed once every path it took is done and counted once. See flow_example_yaml."},
	{"flow.steps[].id", "string", "yes (per step)", "", nil, "Unique step id, used by next and the other outputs."},
	{"flow.steps[].type", "string", "yes (per step)", "", []string{"call", "condition", "data_check", "topic", "webhook", "reject", "dead_letter", "drop"}, "call: POST to an app and route on its answer. condition: split on expr conditions. data_check: check data rules. topic: produce to a topic. webhook: send without routing on the answer. reject / dead_letter: send with a reason, then carry on to next if set. drop: discard."},
	{"flow.steps[].next", "[]string", "no", "", nil, "Steps that run after this one. Several ids fan the message out. Every type but drop can have next."},
	{"flow.steps[].app", "string", "no", "", nil, "Product the step talks to (opensearch, elasticsearch, nifi, kafka, slack, discord, teams, http). Only changes how the console draws it."},
	{"flow.steps[].target / retry / circuit_breaker", "same as the pipeline fields", "target: yes (call)", "pipeline values", nil, "call: the app to call, with its own retries and breaker. Unset values come from the pipeline."},
	{"flow.steps[].on_reject", "[]string", "call: yes unless reject_topic or dead_letter_topic is set", "", nil, "call: steps for a reject status."},
	{"flow.steps[].on_failure", "[]string", "call, webhook: yes unless dead_letter_topic or on_exhausted: block is set", "", nil, "call and webhook: steps once retries are used up."},
	{"flow.steps[].branches", "[]{name, when, next}", "yes (condition)", "", nil, "condition: when is an expr over data (and response/original after a call), e.g. \"data.amount >= 1000\"."},
	{"flow.steps[].match / otherwise", "string / []string", "no", "first", []string{"first", "all"}, "condition: first takes only the first branch that holds, all takes every one. otherwise runs when none holds."},
	{"flow.steps[].rules / on_fail", "data_rules / []string", "rules: yes (data_check)", "", nil, "data_check: data_rules as above; on_fail runs when a rule breaks (needed unless on_violation: tag or a fallback topic exists)."},
	{"flow.steps[].topic / brokers", "string / []string", "topic: yes (topic)", "", nil, "topic, reject, dead_letter: where to produce (reject and dead_letter fall back to the pipeline topics). brokers sends to another Kafka cluster."},
	{"flow.steps[].url / method / headers", "string / string / map", "url: yes (webhook)", "POST", []string{"POST", "PUT", "PATCH"}, "webhook: the url may be a template after http(s)://host/ (e.g. http://opensearch:9200/orders/_doc/{{path .data.order_id}}; path escapes a value), the host itself can't come from the message. webhook and call headers: ${NAME} in a header value is read from the environment, so secrets stay out of the config."},
	{"flow.steps[].body / message / message_field", "template / template / string", "no", "message_field: text", nil, "webhook: body is a Go template over data, original, response, reason, key and headers (e.g. {{json .data}}). message sends {\"<message_field>\": text} for chat apps (use content for Discord). Only one of body and message."},
	{"flow.steps[].reason", "string", "no", "", nil, "reject and dead_letter: why, set as the X-Ark-Reason header."},
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
data_rules:
  on_violation: reject
  fields:
    - {path: order_id, required: true, type: string, pattern: "^ORD-[0-9]+$"}
    - {path: amount, required: true, type: number, min: 0}
    - {path: currency, enum: [THB, USD, EUR]}
    - {path: customer.email, format: email}
`

const flowExampleYAML = `name: orders-flow
source_topic: orders.raw
consumer_group: orders-flow
flow:
  start: [check]
  steps:
    - id: check
      type: data_check
      rules:
        fields:
          - {path: order_id, required: true, type: string}
      next: [size]
      on_fail: [bad]
    - id: size
      type: condition
      branches:
        - {name: large, when: "data.amount >= 1000", next: [fraud]}
      otherwise: [archive]
    - id: fraud
      type: call
      target: {url: http://fraud-service:8080/check}
      next: [checked, search]
      on_reject: [bad]
      on_failure: [dlq, alert]
    - id: checked
      type: topic
      topic: orders.checked
    - id: search
      type: webhook
      app: opensearch
      url: http://opensearch:9200/orders/_doc
      body: "{{json .data}}"
      on_failure: [dlq]
    - id: archive
      type: topic
      topic: orders.small
    - id: bad
      type: reject
      topic: orders.rejected
    - id: dlq
      type: dead_letter
      topic: orders.dlq
    - id: alert
      type: webhook
      app: slack
      url: https://hooks.slack.com/services/${SLACK_PATH}
      message: "order {{.data.order_id}} failed: {{.reason}}"
      on_failure: [drop]
    - id: drop
      type: drop
`
