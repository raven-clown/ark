# ARK

**Connect any Kafka topic to any HTTP app, without writing a Kafka
client.** ARK sits between Kafka and your app's existing endpoint. It
consumes, calls your app, and produces the result, with retries, dead
lettering, a circuit breaker, and horizontal scaling built in. One Go
binary, minutes to deploy.

```mermaid
flowchart LR
    A[("Kafka topic A")] -->|consume| ARK["ARK"]
    ARK -->|"HTTP POST"| App["Your app"]
    App -->|"HTTP response"| ARK
    ARK -->|produce| B[("Kafka topic B")]
    ARK -.->|"on failure"| D[("reject / DLQ topic")]

    style ARK fill:#5b8def,stroke:#3a63c2,color:#fff
    style A fill:#2d3748,stroke:#4a5568,color:#fff
    style B fill:#2d3748,stroke:#4a5568,color:#fff
    style D fill:#742a2a,stroke:#9b2c2c,color:#fff
    style App fill:#276749,stroke:#2f855a,color:#fff
```

## Why

Teams that already have a working app want to plug it into Kafka
without every team writing its own consumer/producer code and handling
retries, dead lettering, offset commits, and rebalances by hand. With
ARK, your app just implements one HTTP endpoint it already knows how
to build, and ARK does the rest.

**The bet:** be the smallest, most opinionated tool that does exactly
"consume, validate or route, call back, produce," rather than a
general-purpose orchestrator like Kafka Connect, NiFi, n8n, or Camel
that needs a cluster of its own just to move messages between a topic
and an endpoint. The full reasoning and comparison live in
[PLAN.md](PLAN.md).

## What works today

Everything below is implemented and verified against a live
docker-compose Kafka, not just designed on paper.

```yaml
pipelines:
  - name: order-processor
    source_topic: orders.raw
    destination_topic: orders.processed
    dead_letter_topic: orders.dlq
    reject_topic: orders.rejected
    consumer_group: order-processor-group
    workers: 3                     # 3 consumer-group members in one process

    target:
      url: http://order-app:8080/process
      health_check_url: http://order-app:8080/
      health_check_interval_seconds: 5

    retry:
      max_attempts: 3
      backoff_ms: 1000
```

That's the whole config for a working pipeline. With it:

- **Nothing is lost on a crash.** Offsets only commit after a
  successful produce, so killing the process mid-message just means
  the next worker to pick up that partition redelivers it.
- **A callback that returns 4xx** (the message itself is the problem,
  not the app) routes to `reject_topic` with no retry.
- **Retries that fail past `max_attempts`** (5xx, timeout, a network
  error) don't flood `dead_letter_topic` forever once the destination
  is genuinely down. The circuit breaker opens, ARK stops hammering
  your app, and the message just blocks in place instead of getting
  dead-lettered. It's still sitting in `orders.raw`, right where it
  was, waiting.
- **Your app coming back is all it takes to unstick things.** Set
  `health_check_url` and ARK probes it while the breaker is open,
  closing the breaker the moment it responds. No need to wait for new
  traffic to trigger a retry. This was tested directly: 3 messages
  queued up behind a dead endpoint, nothing lost, and all three were
  genuinely retried and delivered the moment the app came back.
- **`workers: 3`** means Kafka's own consumer-group protocol splits
  `orders.raw`'s partitions across 3 goroutines in this one process.
  Scale a single pipeline up without deploying anything new.
- **Pause it without killing the process:**
  `POST /api/v1/pipelines/order-processor/pause` (and `/resume`).
- **See it live.** `GET /metrics` (Prometheus) exposes throughput,
  callback latency, consumer lag, circuit breaker state, worker
  liveness, and a last-activity timestamp per pipeline and worker, so
  "is this actually flowing data right now" has a direct answer
  instead of something you have to infer.
- **`fast_path_rules` skip the callback entirely** for messages that
  match a condition, before ARK ever calls your app:

  ```yaml
  fast_path_rules:
    - name: auto-approve-small-orders
      condition: "data.amount < 1000 && data.risk_score < 0.3"
      action: pass_through
    - name: suspicious-fraud-flag
      condition: "data.risk_score > 0.9"
      action: dead_letter
  ```

  `pass_through`, `reject`, `drop`, and `dead_letter` all route
  without a callback round trip. Conditions use
  [`expr-lang/expr`](https://expr-lang.org) syntax (`nil`, not `null`).
- **`post_callback_rules` branch on the callback's response instead,**
  after it comes back: check whether it actually succeeded by
  app-level standards (not just HTTP status), and route the result to
  a different Kafka topic or a different HTTP endpoint entirely,
  without a second callback:

  ```yaml
  post_callback_rules:
    - name: high-value-order-alert
      condition: "response.status == 200 && response.body.amount > 10000"
      action: transform_route
      destination_override: orders.high-value   # or webhook_override: http://...
  ```

- **Browse, retry, or discard what landed in `dead_letter_topic` or
  `reject_topic`,** without a separate Kafka console tool:
  `GET /api/v1/pipelines/order-processor/dlq` lists recent entries,
  `POST .../dlq/{id}/retry` re-enters the message into the pipeline
  from `source_topic` (fast_path_rules and all), `POST .../dlq/{id}/discard`
  removes it from the list. Same routes under `.../reject`.
- **Spread a pipeline's callbacks across more than one backend
  instance** with `target.mode: multi_url`:

  ```yaml
  target:
    mode: multi_url
    urls:
      - http://order-app-1:8080/process
      - http://order-app-2:8080/process
    health_check_urls:
      - http://order-app-1:8080/
      - http://order-app-2:8080/
    health_check_interval_seconds: 5
    strategy: round_robin   # or least_inflight, sticky_partition
  ```

  Each URL is probed independently on `health_check_urls`; a message
  only ever goes to a healthy one, and if every URL is down the
  message blocks in place instead of getting dead-lettered, the same
  way a single_url pipeline behaves when its circuit breaker is open.
  `sticky_partition` keeps a given Kafka partition landing on the same
  backend for as long as that backend stays healthy, useful when the
  backend keeps per-partition local state.
- **Point Claude, or any MCP client, at a running ARK.** Set
  `ARK_MCP_VIEWER_TOKENS` / `ARK_MCP_OPERATOR_TOKENS` /
  `ARK_MCP_ADMIN_TOKENS` and ARK mounts a real MCP server at `/mcp` on
  the same port as the REST API, calling the same internal registry
  the REST handlers use rather than a separate reimplementation. A
  `viewer` token can ask what the error rate on order-processor is
  right now or list what landed in the DLQ; `operator` and `admin`
  tokens can also pause, resume, retry a DLQ entry, or discard one.
  Every pipeline also carries its own `mcp_access`
  (`read_only` / `read_write` / `none`), so a token's scope is a
  ceiling, not a grant: an `operator` token still can't write to a
  pipeline whose `mcp_access` is `read_only`. Every write call is
  audit-logged with the scope, action, pipeline, and entry, never the
  token itself. Verified against a scripted MCP client covering all
  three scopes, and separately against two different local models
  (`qwen2.5:7b`, `qwen3:8b` via Ollama) driving the server from plain
  English with no hardcoded tool-call logic, to check the tool
  descriptions actually explain themselves rather than only making
  sense to one model.

## Where it's going

Designed in detail in [PLAN.md](PLAN.md), not built yet:

- **MCP config tools.** `validate_pipeline_config`,
  `apply_pipeline_config`, `create_pipeline`, `get_pipeline_schema`,
  and `list_topics`, so an agent can draft a new pipeline from a plain
  language description and, once you confirm, apply it. Waiting on
  Phase 4's hot-reload, since there is no live config to apply to yet.
- **ARK Cluster.** Run several ARK processes across machines and let
  them split a pipeline's workers automatically, with failover if one
  dies. No etcd, no separate Raft cluster to operate: it reuses
  Kafka's own consumer-group coordination as the election and
  placement mechanism, since ARK already depends on Kafka being up
  anyway.
- **Dashboard UI.** A separate deployable service that talks to ARK's
  REST API: pipeline list, live metrics, a DLQ browser, all without
  touching Kafka directly.

## Repo layout

- `bridge-engine/` is the Go engine (consumer, rule engine, callback
  client, producer, REST API, MCP server).
- `bridge-ui/` is the dashboard UI, deployed separately from the
  engine.

## Running locally

```
docker compose up
```

This starts a single-node Kafka broker and the bridge engine wired to
`bridge-engine/config.example.yaml`. Kafka's own data directory is a
named volume by default. Point it at another disk or mount with:

```
KAFKA_DATA_DIR=/mnt/other-disk/kafka docker compose up
```

To run the engine directly:

```
cd bridge-engine
go run ./cmd/bridge -config config.example.yaml
```

## API

- `GET /healthz`: liveness
- `GET /metrics`: Prometheus metrics
- `GET /api/v1/pipelines`: status for every pipeline worker
- `GET /api/v1/pipelines/{name}`: status for one pipeline's workers
- `POST /api/v1/pipelines/{name}/pause` and `/resume`
- `GET /api/v1/pipelines/{name}/dlq` and `/reject`: recent entries
- `GET /api/v1/pipelines/{name}/dlq/{id}` and `/reject/{id}`: one entry
- `POST .../dlq/{id}/retry` and `/discard` (same for `/reject`)

## Contributing

PRs welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for dev setup,
what a good PR looks like, and where ARK's scope is deliberately
bounded (worth reading before proposing something big).

## License

Apache License 2.0. See [LICENSE](LICENSE).
