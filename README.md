# ARK

**Connect any Kafka topic to any HTTP app, without writing a Kafka
client.** ARK sits between Kafka and your app's existing endpoint:
consume → callback → produce, with retries, dead-lettering, a circuit
breaker, and horizontal scaling built in — one Go binary, minutes to
deploy.

```
[Kafka topic A] --consume--> [ARK] --HTTP POST--> [your app]
                                 |                      |
                                 |<---- HTTP response ---+
                                 v
                         [Kafka topic B]  (or reject/DLQ if it fails)
```

## Why

Teams that already have a working app want to plug it into Kafka
without every team writing its own consumer/producer code, handling
retries, dead-lettering, offset commits, and rebalances by hand. With
ARK, your app implements one HTTP endpoint it already knows how to
build — ARK does the rest.

**The bet:** be the smallest, most opinionated tool that does exactly
"consume → validate/route → callback → produce," instead of a
general-purpose orchestrator (Kafka Connect, NiFi, n8n, Camel) that
needs a cluster of its own just to move messages between a topic and
an endpoint. Full reasoning and comparison in [PLAN.md](PLAN.md).

## What works today

Everything below is implemented and verified against a live
docker-compose Kafka — not just designed on paper.

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
  successful produce — kill the process mid-message and the next
  worker to pick up that partition redelivers it.
- **A callback that returns 4xx** (bad request — the message itself is
  the problem) routes to `reject_topic`, no retry.
- **Retries fail past `max_attempts`** (5xx, timeout, network error)
  and no `dead_letter_topic` is around to eat every message forever:
  once the circuit breaker opens, ARK stops hammering your app and
  **blocks the message in place** instead of dead-lettering it. It's
  still sitting in `orders.raw`, exactly where it was, waiting.
- **Your app coming back is all it takes.** If `health_check_url` is
  set, ARK probes it while the breaker is open and closes the breaker
  the moment it responds — no need to wait for new traffic to trigger
  a retry. Verified: 3 messages queued behind a dead endpoint, zero
  lost, all three genuinely retried and delivered the moment the app
  came back up.
- **`workers: 3`** means Kafka's own consumer-group protocol splits
  `orders.raw`'s partitions across 3 goroutines in this one process —
  scale a single pipeline up without deploying anything new.
- **Pause it without killing the process:**
  `POST /api/v1/pipelines/order-processor/pause` (and `/resume`).
- **See it live:** `GET /metrics` (Prometheus) exposes throughput,
  callback latency, consumer lag, circuit breaker state, worker
  liveness, and last-activity timestamp per pipeline/worker — so
  "is this actually flowing data right now?" has a direct answer
  instead of one you have to infer.

## Where it's going

Designed in detail in [PLAN.md](PLAN.md), not yet built:

- **Rule engine** — `fast_path_rules` skip the callback entirely for
  messages that match a condition (`pass_through` / `reject` / `drop`
  / `dead_letter`), and `post_callback_rules` branch on the callback's
  *response* — check whether it actually succeeded by app-level
  standards, transform the result, route it to a different topic, all
  without a second callback.
- **MCP server** — point Claude (or any MCP client) at a running ARK
  and ask it "what's the error rate on order-processor right now?",
  have it pause a stuck pipeline, or describe a new pipeline in plain
  language and have the agent draft, validate, and — after you
  confirm — create it. Scoped by token (`viewer`/`operator`/`admin`)
  and per-pipeline `mcp_access`, so a read-only analysis agent can
  never accidentally touch production.
- **ARK Cluster** — run several ARK processes across machines and
  have them split a pipeline's workers automatically, with failover if
  one dies. No etcd, no Raft cluster to operate: it reuses Kafka's own
  consumer-group coordination as the election/placement mechanism,
  since ARK already depends on Kafka being up anyway.
- **Dashboard UI** — a separate deployable service that talks to ARK's
  REST API: pipeline list, live metrics, DLQ browser, all without
  touching Kafka directly.

## Repo layout

- `bridge-engine/` — the Go engine (consumer, rule engine, callback
  client, producer, REST API, MCP server)
- `bridge-ui/` — dashboard UI, deployed separately from the engine

## Running locally

```
docker compose up
```

Starts a single-node Kafka broker and the bridge engine wired to
`bridge-engine/config.example.yaml`. Kafka's own data directory is a
named volume by default; point it at another disk or mount with:

```
KAFKA_DATA_DIR=/mnt/other-disk/kafka docker compose up
```

To run the engine directly:

```
cd bridge-engine
go run ./cmd/bridge -config config.example.yaml
```

## API

- `GET /healthz` — liveness
- `GET /metrics` — Prometheus metrics
- `GET /api/v1/pipelines` — status for every pipeline worker
- `GET /api/v1/pipelines/{name}` — status for one pipeline's workers
- `POST /api/v1/pipelines/{name}/pause` / `/resume`

## Contributing

PRs welcome — see [CONTRIBUTING.md](CONTRIBUTING.md) for dev setup,
what a good PR looks like, and where ARK's scope is deliberately
bounded (worth reading before proposing something big).

## License

Apache License 2.0 — see [LICENSE](LICENSE).
