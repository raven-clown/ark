# Kafka Callback Bridge — Project Plan

> Config-driven middleware that connects Kafka topics to plain HTTP apps
> (consume → validate/route → callback → produce), with an MCP interface
> so AI agents can operate it, deployed as an open-source, self-hosted tool.

Owner: fe.laxxy@gmail.com
License: Apache License 2.0
Status: Phase 1 & 2 done, Phase 4 (workers: N) and Phase 5 (status/pause/
resume/metrics) partially done — see checkboxes below

---

## 1. Problem statement

Teams that already have working apps (any language) want to plug into
Kafka without every team writing its own Kafka consumer/producer code,
handling retries, dead-lettering, offset commits, and rebalances by hand.

**The Bridge** sits between Kafka and an app's existing HTTP endpoint:

```
[Kafka topic A] --consume--> [Bridge] --HTTP POST--> [App endpoint]
                                  |                        |
                                  |<---- HTTP response -----+
                                  v
                          [Kafka topic B] (or reject/DLQ/drop)
```

The app never touches a Kafka client library. It just implements one
HTTP endpoint it already knows how to build.

The `[Kafka topic A]` on the left is also the Bridge's queue, the same
role NiFi's inter-processor connections play — pausing or stopping a
pipeline doesn't drop anything, messages just sit in that topic
(bounded by its retention setting) until consumption resumes. Since the
queue lives in Kafka rather than on any Bridge instance's own disk, a
Bridge node crashing mid-message doesn't touch it either: the offset
for that message was never committed (commit only happens after a
successful produce), so it's simply re-delivered to whichever Bridge
node picks up that partition next.

## 2. Non-goals (v1)

- Not a general-purpose iPaaS (not competing with n8n / Camel / Zapier
  breadth). Scope stays Kafka-native and HTTP-callback-shaped.
- Not a hosted/managed service. Self-hosted, single binary or Docker
  image, operator controls their own uptime.
- Not a new storage/database engine. Uses Kafka itself as the buffer;
  no custom persistence layer in v1.

## 3. Differentiation (why build this vs. existing tools)

| Existing tool | Gap it leaves |
|---|---|
| Kafka Connect | Powerful but heavy — needs a Connect cluster, JVM, custom connector code for anything off the beaten path |
| Strimzi/AMQ HTTP Bridge | Protocol translator (HTTP↔Kafka RPC-style), not an orchestrator; each instance pins consumer state in memory, doesn't scale horizontally well for this pattern |
| n8n / Camel / Benthos | General-purpose, hundreds of connectors, heavier to deploy and reason about for a single narrow pattern |

**Our bet:** be the smallest, most opinionated tool that does exactly
"consume → validate/route → callback → produce" with built-in
correlation IDs, retry, dead-lettering, and sticky-partition ordering —
config in YAML, one binary, minutes to deploy. And: give AI agents a
native way to operate it (MCP), which none of the above do today.

## 4. Architecture overview

```
                     ┌─────────────────────────────┐
                     │        Bridge Engine          │  (Go binary)
                     │                                │
  Kafka topic ──────►│  Consumer ──► Rule Engine       │
                     │                 │   │            │
                     │        pass_through/drop/        │
                     │        reject/dead_letter         │
                     │                 │                  │
                     │                 ▼ (else)            │
                     │           HTTP Callback Client       │
                     │                 │                      │
                     │                 ▼                       │
                     │              Producer ──────────────►  Kafka topic(s)
                     │                                │
                     │  REST API  ◄── queried by ──►  Dashboard UI (separate deploy)
                     │  MCP Server ◄── queried by ──►  AI agents (Claude, etc.)
                     │  Metrics (Prometheus)                    │
                     └─────────────────────────────┘
```

Two additional interfaces sit alongside the REST API, both read from the
same internal state (pipeline registry, metrics, DLQ store) — they are
thin adapters, not separate sources of truth:

- **REST API** — for the Dashboard UI (separate deployable service).
- **MCP Server** — for AI agents (Claude Desktop, Claude Code, or any
  MCP-capable client) to inspect and operate the Bridge conversationally.

## 5. Core concepts / config schema

```yaml
pipelines:
  - name: order-processor
    tenant: team-a                      # for multi-team isolation/metrics
    mcp_access: read_write              # read_only | read_write | none (default: read_only)
    source_topic: orders.raw
    destination_topic: orders.processed
    dead_letter_topic: orders.dlq
    reject_topic: orders.rejected

    consumer_group: order-processor-group
    workers: 3                          # consumer-group members (goroutines) in this process
    consumer:
      max_poll_records: 50
      max_poll_interval_ms: 300000

    target:
      mode: single_url                  # or multi_url
      url: http://order-app:8080/process
      health_check_url: http://order-app:8080/health   # optional
      health_check_interval_seconds: 10                 # default when health_check_url is set
      # mode: multi_url
      # urls: [http://worker-1:8080/process, ...]
      # strategy: sticky_partition       # or round_robin, least_inflight

    concurrency:
      max_in_flight: 10

    retry:
      max_attempts: 3
      backoff_ms: 1000

    fast_path_rules:                    # evaluated before any HTTP callback
      - name: auto-approve-small-orders
        condition: "data.amount < 1000 && data.risk_score < 0.3"
        action: pass_through
      - name: invalid-missing-field
        condition: "data.customer_id == null"
        action: reject
      - name: drop-test-data
        condition: "data.env == 'test'"
        action: drop
      - name: suspicious-fraud-flag
        condition: "data.risk_score > 0.9"
        action: dead_letter
```

Rule actions: `pass_through`, `reject`, `drop`, `dead_letter`. Anything
not matched by a rule falls through to the normal callback flow.

**Chaining pipelines:** there's no special "multi-stage pipeline"
feature, and none is needed for the common case of wanting several
processing stages — a `destination_topic` of one pipeline can simply be
the `source_topic` of another: `orders.raw → pipeline A → orders.staged
→ pipeline B → orders.processed`. Each stage is an ordinary entry in
`pipelines:`, independently pausable/resumable, independently
observable in metrics. This covers "long, multi-step pipelines" without
any engine changes; see §6's Phase 3 extensions for `post_callback_rules`,
which covers the other half — smarter branching *within* one stage,
still without becoming a general DAG engine (see §11 for why).

## 6. Phased roadmap

### Backend — Phase 1: Core engine (MVP) — done
- [x] Go module scaffold (`cmd/bridge`, `internal/...`)
- [x] Config loader (YAML → typed struct, validation)
- [x] Kafka consumer wrapper (single pipeline, single partition strategy)
- [x] HTTP callback client with correlation ID injection
- [x] Kafka producer wrapper (destination topic)
- [x] At-least-once semantics (commit offset only after produce succeeds)
- [x] Structured JSON logging with correlation ID
- **Exit criteria:** one pipeline, one topic in, one topic out, via HTTP
  callback, running locally against docker-compose Kafka. — verified
  end to end against live docker-compose Kafka.

### Backend — Phase 2: Reliability — done
- [x] Retry with backoff on callback failure/timeout
- [x] Retry with backoff on produce failure too (destination/DLQ/reject
      topics), not just the HTTP callback — found by testing that a
      transient produce error was failing a message permanently for
      the running process instead of being retried
- [x] Dead-letter topic on exhausted retries — but only for a
      message-specific failure (the destination is otherwise healthy
      and responding, this particular message's attempts all failed).
      When the circuit breaker is open — a destination-wide outage, not
      a bad message — `process()` blocks in place re-checking the
      breaker instead of giving up, so nothing goes to DLQ during an
      outage; the message just sits in `source_topic` (which is already
      the queue, see §1) until the destination is reachable again.
- [x] Reject action (4xx callback response, non-retryable) → reject_topic
- [x] Concurrency limiter (max_in_flight) with in-order offset commit
- [x] Consumer pause/resume (`POST /api/v1/pipelines/{name}/pause|resume`
      — moved up from Phase 5 since it only needed the engine, not the
      full REST API)
- [x] Circuit breaker around the callback client
- [x] Optional `target.health_check_url`: while the breaker is open, a
      background probe periodically checks it and closes the breaker on
      success — recovery doesn't depend on new traffic arriving to
      trigger a retry. Doubles as a way to wake a cold/sleeping
      destination using whatever health endpoint it already exposes.
- **Exit criteria:** a flaky/slow downstream app no longer causes message
  loss or unbounded queueing; DLQ and reject topics populate correctly.
  — verified: retries exhaust against a message-specific failure → DLQ
  populates; a destination-wide outage → breaker opens, messages block
  in place with zero commits, `target.health_check_url` closes the
  breaker without new traffic, and the whole backlog is genuinely
  retried (not skipped) once the destination is reachable again. A real
  bug was found and fixed here: an earlier version let a later message
  on the same partition commit past an unresolved earlier one, since
  Kafka's offset commit is a single resume pointer, not a per-message
  ledger — skipping any offset at all silently drops it.

### Backend — Phase 3: Rule engine (fast path)
- [ ] Expression evaluator (`expr` or `cel-go`) wired to rule config
- [ ] `pass_through` / `drop` / `reject` / `dead_letter` actions
- [ ] Rule ordering (first match wins) + per-rule metrics
- **Exit criteria:** messages matching a rule skip the HTTP callback
  entirely; metrics show the split between fast-path and callback traffic.

#### Phase 3 extensions (post-v1, staged by real need)

`fast_path_rules` starts with exactly the 4 actions above and nothing
else — the extensions below are real, useful, and each adds real
complexity, so they're staged rather than built all at once:

- **v1 (Phase 3 itself):** `pass_through` / `reject` / `drop` /
  `dead_letter` only. Covers most routing needs.
- **v1.5 — schema validation as a first-class action:**
  ```yaml
  - name: schema-check
    action: reject
    unless_schema_valid: "./schemas/order.schema.json"
  ```
  Replaces hand-written null-check conditions with a JSON Schema
  reference. Highest value-to-complexity ratio of this whole list —
  next thing to build after v1.
- **v2 — transform, per-destination routing override, dedup, rate
  limit:**
  - `transform_pass_through`: redact/add fields on the fast path
    without a callback round-trip (e.g. strip `ssn`/`credit_card`,
    stamp `processed_by`/`ts`) — useful for PII redaction or metadata
    tagging before forwarding.
  - `destination_override` on a rule: send a match to a topic other
    than the pipeline's default `destination_topic` (e.g. route by
    region). Fan-out to multiple destinations from one rule ties into
    the Phase 7 fan-out sink work.
  - `sample_rate` on a rule: let only a configured fraction of matches
    through (e.g. keep 10% of debug-tier events).
  - Per-rule rate limiting: matches beyond a threshold/sec get
    dropped or dead-lettered instead of passing — guards against a
    bug that suddenly floods a rule.
  - Time-based conditions (`time.hour`, `time.weekday`) for
    business-hours-only routing.
  - A rule-level safety valve: auto-disable (and alert, not just log)
    a rule whose match rate suddenly spikes far outside its historical
    norm, so a bad rule can't silently bypass every callback.
- **v2.5+ — stateful conditions, enrichment lookups:** the two most
  complex items, both needing a state store (in-memory + TTL, or an
  embedded KV) that v1 deliberately has none of:
  - Deduplication (drop a repeated key within N seconds) and
    windowed thresholds ("same customer errors 5x in 1 min →
    dead_letter") — both require memory across messages, not just a
    single message's fields.
  - Enrichment lookups (e.g. check `customer_id` against a blocklist)
    before a rule decision — must stay local-cache-only; a rule that
    calls out to a network service on every message defeats the point
    of a "fast path."

**`post_callback_rules` — the mirror-image feature, evaluated on the
callback's *response* instead of the incoming message:**

```yaml
post_callback_rules:
  - name: high-value-order-alert
    condition: "response.status == 200 && response.body.amount > 10000"
    action: transform_route
    destination_override: orders.high-value
  - name: callback-said-retry
    condition: "response.body.retry_after != null"
    action: dead_letter
```

Answers "the callback came back — now what? check if it actually
succeeded by app-level standards (not just HTTP status), transform the
result, decide where it goes" — still exactly one callback per message,
just richer post-processing of its result. This is the bounded way to
get that behavior without turning the engine into a general DAG/flow
graph (see §11 for the explicit scope decision behind this). Same
staging logic as `fast_path_rules`: land after schema validation
(v1.5), alongside `destination_override`/`transform_pass_through` (v2),
since it's the same rule-evaluation machinery pointed at a different
input.

### Backend — Phase 4: Multi-pipeline & multi-tenant
- [x] Multiple pipelines in one config, isolated goroutine pools
- [x] Per-pipeline `workers` count — N consumer-group members (goroutines)
      in one process; Kafka's own group coordinator splits the source
      topic's partitions across them, no extra deploys needed to use
      more than one core for a single pipeline
- [ ] Tenant label on every metric/log line
- [ ] Hot-reload config without downtime (file watch or reload endpoint)
- [ ] `multi_url` target mode: round-robin, least-in-flight, sticky-partition
- [ ] Health-checked worker pool for multi_url targets
- **Exit criteria:** two independent teams' pipelines run in one Bridge
  process without interfering with each other's throughput or config.

#### Phase 4b: ARK Cluster (multi-node, opt-in)

`workers: N` (above) parallelizes a pipeline across goroutines *within
one process* — it does nothing if one machine's capacity is the actual
ceiling. ARK Cluster is the answer to that case: spreading a pipeline's
workers across multiple ARK processes/machines, with automatic
placement and failover if a node dies. It is entirely opt-in — a single
`docker compose up` with no cluster config stays a single-node
deployment, exactly as built through Phase 4. Whether to run one node
or a cluster is the operator's call, not something the Bridge forces.

The design deliberately reuses Kafka itself as the coordination
backbone instead of embedding a Raft/gossip implementation or taking a
dependency on an external coordinator (etcd, ZooKeeper, k8s):

1. **Heartbeat** — every ARK node produces periodically to an internal
   topic (`__ark_cluster_nodes`), reporting that it's alive and its
   current capacity.
2. **Leader election** — every node tries to consume a single-partition
   internal topic (`__ark_leader_election`) under one consumer group.
   Kafka only ever hands that one partition to one group member at a
   time, so whichever node holds it is the leader; if that node dies,
   Kafka's own rebalance immediately hands the partition to a
   survivor, which becomes the new leader — no separate election
   protocol to write.
3. **Placement** — the leader reads current heartbeats, decides which
   node should run which pipeline's worker slots, and writes that
   decision to another internal topic (`__ark_placements`).
4. **Execution** — every node consumes `__ark_placements`; a node spins
   up the worker slots it's been assigned and ignores the rest.
5. **Failure** — a node's heartbeat goes stale, the leader notices and
   reassigns its slots to the remaining nodes via a new placement
   record; on restart a node just rejoins and waits for its next
   assignment.

Net effect: scaling `order-processor` to 3 workers via MCP/UI is one
call regardless of node count — the cluster decides *where* those 3
workers run, and adding a new ARK process (any host, any way it's
launched) makes it join and pick up a share of the work automatically,
with no orchestration script involved. All coordination state
(heartbeats, placements) lives in ordinary replicated Kafka topics —
consistent with §2's non-goal of not building a new storage engine;
"distributed state" here means Kafka's own replication, not a new DB.

This is real distributed-systems work (failure detection timing,
split-brain edges during a leader handoff, placement rebalancing
policy) and is staged after Phase 4's single-node multi-pipeline
support is solid — build it when a real deployment actually needs more
than one node's capacity, not speculatively.

**Config distribution.** Today every node reads `pipelines:` from its
own local YAML file, which is fine for one node but wrong for a
cluster: node A and B can silently disagree if only one file gets
edited. Fix: pipeline config becomes a compacted Kafka topic
(`__ark_pipeline_config`, key = pipeline name) that every node consumes
to build its in-memory config; the local YAML file becomes just the
bootstrap seed for an empty topic, not the ongoing source of truth.
This gets replication/durability for free from Kafka's own topic
replication (consistent with §2 — still no new storage engine), and it
directly plugs into Phase 4's "hot-reload" item and Phase 6's
`apply_pipeline_config`/`create_pipeline`, which would write to this
topic instead of touching a file.

**What happens if Kafka itself is down (not just one ARK node).** Two
separate failure modes, two different answers:
- *Kafka fully unreachable while ARK is already running:* every ARK
  node loses coordination (heartbeat/leader-election/placement) and
  the data plane (consume/callback/produce) at the same time, since
  both depend on the same Kafka. This can't produce a split-brain,
  because no node believes it's "still working alone" while others are
  cut off — nobody can do anything without Kafka regardless of cluster
  design. The moment Kafka comes back, coordination and processing both
  resume from where committed offsets/state left off. This is a direct
  consequence of putting coordination on the same substrate as the
  actual work, not a gap to fix.
- *A node restarts while Kafka is briefly unreachable:* this is the
  real gap — with config living only in the `__ark_pipeline_config`
  topic, a restarting node can't even boot. Fix: every node keeps a
  local on-disk snapshot of the last config it consumed, written on
  every update. On startup, try Kafka first; if unreachable, boot from
  the local snapshot instead of failing, and resync once Kafka is
  reachable again. This is a per-node resilience cache, not a second
  source of truth — it holds no state that isn't also in Kafka.

**Why Kafka-coordinator-based leader election instead of a real Raft
quorum (embedded `hashicorp/raft`, or an external etcd/Consul):**

| | Kafka consumer-group coordinator (chosen) | Raft quorum (etcd/Consul/embedded) |
|---|---|---|
| Extra infra | None — reuses Kafka, which ARK already requires | A separate etcd/Consul cluster, or an embedded Raft log/snapshot implementation |
| Code size | ~200–400 lines: heartbeat + consume a topic + react to rebalance | A full consensus implementation or another dependency to operate |
| Node count | Any N ≥ 1, no wasted nodes | Must stay odd for full fault tolerance (majority = ⌊N/2⌋+1, so N=2 tolerates zero failures) |
| Split-brain protection | Kafka's own rebalance `generation` ID fences stale members — same idea as Raft's term number | Purpose-built for this, more battle-tested at the edges |
| Failure-detection speed | Timeout-based (session timeout, ~10–45s), tied to Kafka's settings | Also timeout-based, but tunable independently and typically faster |
| Operator familiarity | A repurposed mechanism — less standard mental model to debug against | Very standard ("3-node Raft, need 2 up") — widely recognized |
| Fits ARK's positioning (§3) | Yes — one binary, minutes to deploy | No — reintroduces the exact heavyweight-dependency problem §3 differentiates against |

**Decision:** Kafka-coordinator-based election. The thing being decided
("which node runs which worker slot") isn't strongly-consistent data
where a brief inconsistency is dangerous — at-least-once processing
already tolerates a worker being briefly absent during a handoff. A
full Raft implementation would be solving a harder problem than ARK
actually has, at a cost (extra infra, more code, odd-node-count
constraint) that directly contradicts §3's differentiation bet.
Revisit only if the Kafka-coordinator approach proves unreliable in
practice, not preemptively.

### Backend — Phase 5: Observability — mostly done
- [x] Prometheus metrics endpoint: per-pipeline/worker throughput
      counters (processed/rejected/dead_lettered/failed/backpressured),
      callback latency histogram, consumer lag gauge (from kafka-go's
      `Reader.Stats().Lag`), circuit breaker state, pause state, worker
      liveness (`ark_worker_up`), last-activity timestamp (answers "is
      this pipeline actually flowing data right now" directly instead
      of inferring it from lag + throughput separately)
- [ ] Per-tenant metric breakdown (tenant label isn't wired into
      metrics yet, only into `Status`)
- [x] REST API: list pipelines (`GET /api/v1/pipelines`), pipeline
      detail (`GET /api/v1/pipelines/{name}`), pause/resume
      (`POST .../pause`, `POST .../resume`)
- [ ] View DLQ, retry/discard DLQ message
- **Exit criteria:** an operator can answer "is this healthy?" from
  metrics alone, and act on a stuck DLQ message via API. — metrics half
  done; DLQ browsing/retry still open.

### Backend — Phase 6: MCP server (AI-agent interface)

**Permission model — two layers, both enforced server-side (never trust
the calling agent to self-restrict):**

1. **Token-level scope** — the API token used to connect the MCP server
   carries one of:
   - `viewer` — every read tool works; every write tool returns a
     permission error, full stop, regardless of any pipeline's own
     `mcp_access` setting.
   - `operator` — read tools work everywhere; write tools work only on
     pipelines whose own `mcp_access` allows it (see below).
   - `admin` — read/write everywhere, including config tools
     (`apply_pipeline_config`).
   Tokens are issued and revocable independently of each other, so a
   token handed to an analysis-only agent can simply be `viewer` and
   nothing else needs configuring per pipeline.

2. **Per-pipeline `mcp_access`** (config field, shown above) — narrows
   what an `operator`/`admin` token may do to *that specific pipeline*:
   - `read_only` (default) — status/metrics/DLQ browsing only; any write
     tool call targeting this pipeline is rejected even for an `operator`
     or `admin` token. This is the setting for pipelines you want an
     analysis agent to see but never touch — e.g. a production billing
     pipeline that should only ever be read for reporting.
   - `read_write` — write tools (pause/resume/retry/discard) are allowed
     if the token scope also allows them.
   - `none` — pipeline is invisible to MCP entirely (excluded from
     `list_pipelines` and all other tool results for that connection).

   Net effect: a pipeline's own `read_only` flag is a hard ceiling — no
   token scope can override it. Token scope is a floor/ceiling on the
   *agent*; pipeline `mcp_access` is a ceiling on the *pipeline*. Both
   must allow an action for it to execute.

- [ ] Stand up an MCP server process (can be embedded in the Bridge
      binary or a thin sidecar that calls the REST API — decide based on
      how Phase 5's API turns out)
- [ ] Define token scopes (`viewer` / `operator` / `admin`) and a token
      issuance/revocation mechanism (reuse REST API's auth store)
- [ ] Enforce per-pipeline `mcp_access` on every tool call server-side,
      before touching Kafka or the callback target
- [ ] Expose read tools (available to all scopes, filtered by each
      pipeline's `mcp_access != none`):
      `list_pipelines`, `get_pipeline_status`, `get_metrics`,
      `list_dlq_messages`, `get_dlq_message`
- [ ] Expose write tools (require `operator`/`admin` token AND pipeline
      `mcp_access: read_write`): `pause_pipeline`, `resume_pipeline`,
      `retry_dlq_message`, `discard_dlq_message`
- [ ] Expose config tools (require `admin` token only, gated behind
      explicit confirmation in the calling client):
      `validate_pipeline_config`, `apply_pipeline_config` (hot-reload)
- [ ] Audit log every write/config tool call (who, token id, pipeline,
      action, timestamp) — separate from the general structured log, so
      "what did the AI agent change and when" is always answerable
- **Exit criteria:** an analysis-only agent connected with a `viewer`
  token (or an `operator` token on a `read_only`-flagged pipeline) can
  answer "what's the error rate on order-processor right now?" but a
  call to `pause_pipeline` from that same connection is rejected with a
  clear permission error — verified with an automated test for each of
  the three token scopes × three `mcp_access` settings (9 combinations).

### Backend — Phase 7: Extensibility
- [ ] `Source` and `Sink` interfaces (Kafka is one implementation of each)
- [ ] Database sink (direct insert/upsert, bypassing HTTP callback)
- [ ] Fan-out destinations (topic + webhook + DB in one pipeline)
- [ ] Revisit: CDC source, RabbitMQ/NATS, schedule trigger — only if
      real demand shows up after Phase 1–6 are solid

### Frontend — Phase A: Read-only metrics (fastest path)
- [ ] Ensure Prometheus metrics are scrape-ready
- [ ] Ship a starter Grafana dashboard JSON (no custom UI code needed)
- **Exit criteria:** `docker-compose up` gives Bridge + Prometheus +
  Grafana with a working dashboard out of the box.

### Frontend — Phase B: Dashboard UI (separate deployable service)
- [ ] Separate repo/binary (`bridge-ui`), own Docker image
- [ ] Calls Bridge REST API only (never touches Kafka directly)
- [ ] Views: pipeline list + status, pipeline detail + live metrics,
      DLQ browser with retry/discard actions
- [ ] Own auth/login, versioned API client (`/api/v1/...`)
- **Exit criteria:** an operator manages pipelines and DLQ entirely from
  the web UI, deployed and released independently from the engine.

### Frontend — Phase C: Visual rule builder (deferred)
- On hold. Only revisit if Phase A/B in production reveals real demand
  for editing rules without touching YAML — and even then, re-evaluate
  against adopting n8n instead of building a visual builder from scratch.

## 7. Tech stack

- Language: Go (concurrency model fits consume/callback/produce; single
  static binary; strong Kafka client libraries — `franz-go` or
  `segmentio/kafka-go`)
- Expression engine: `google/cel-go` or `expr-lang/expr` for
  `fast_path_rules` conditions
- Metrics: Prometheus client library
- MCP: official Go MCP SDK (or a thin JSON-RPC-over-stdio/HTTP shim if
  no mature Go SDK is available at implementation time — confirm during
  Phase 6)
- Dashboard UI: Go + HTMX or a lightweight SPA — decide at Phase B,
  not before (avoid committing to a frontend framework early)

## 8. Repo layout (proposed)

```
kafka-bridge/
├── PLAN.md
├── bridge-engine/
│   ├── cmd/bridge/
│   ├── internal/
│   │   ├── config/
│   │   ├── consumer/
│   │   ├── producer/
│   │   ├── rules/
│   │   ├── callback/
│   │   ├── api/            # REST API (Phase 5)
│   │   └── mcp/             # MCP server (Phase 6)
│   ├── go.mod
│   └── Dockerfile
└── bridge-ui/               # separate deploy (Phase B)
    ├── cmd/
    ├── go.mod
    └── Dockerfile
```

## 9. Natural-language pipeline creation ("describe it, don't write YAML")

Goal: a person describes what they want in plain language ("ดึงจาก topic
orders.raw ส่งไป http://app:8080/process แล้วโยนไป orders.processed, ถ้า
amount ต่ำกว่า 1000 ให้ผ่านเลยไม่ต้องเรียก callback") and an AI agent
connected via MCP turns that into a valid pipeline config, shows it back
for confirmation, and applies it — no hand-written YAML required.

This does **not** need a new NLP component in the Bridge itself. The
Bridge stays a deterministic engine; the "understand what the person
wants" part is done by whichever AI agent is connected (Claude, etc.).
The Bridge's job is only to (a) tell the agent the exact schema so it
doesn't hallucinate fields, and (b) validate + apply safely.

**Two creation modes — both call the same `create_pipeline` tool, just
with a different completeness level:**

- **Full mode** — person describes everything needed (source topic,
  target, what happens on error, etc.) in the conversation; agent fills
  a complete, valid config and creates a pipeline that's immediately
  running after confirmation. Best when the person already knows the
  shape of what they want.

- **Draft mode** — person gives only the part they know right now
  ("ทำ pipeline ชื่อ order-processor ดึงจาก orders.raw ก่อน ที่เหลือไปตั้ง
  ใน UI") and the agent creates the pipeline in a **disabled/draft
  state**: only `name` and `source_topic` are required, every other
  field gets a safe placeholder (`target: null`, `destination_topic:
  null`, `enabled: false`). A pipeline with `enabled: false` is fully
  visible in the config store and the Dashboard UI, but the Consumer
  never starts for it — so nothing runs, nothing can callback into a
  URL that doesn't exist yet, nothing produces to a topic that was
  never specified. The person then opens the Dashboard UI, fills in the
  rest (target URL, rules, retry policy, etc.), and flips it to
  `enabled: true` there — which calls the same REST endpoint the UI
  always uses to update a pipeline, no MCP involved at that point.

This means `create_pipeline`'s only hard requirement is `name` +
`source_topic`; everything else is optional at creation time, but the
pipeline stays `enabled: false` until the required-for-running fields
(`target`, `destination_topic` or an explicit "no destination" flag) are
present — the engine itself refuses to flip a pipeline to `enabled: true`
if those are still missing, whether that flip is attempted from MCP or
the UI. This is the one validation rule that matters most in practice:
**it should be impossible to accidentally activate a half-configured
pipeline**, regardless of which interface (chat or UI) did the "flip
enabled" step.

**New tools needed for this (added to Phase 6):**

- `get_pipeline_schema` — returns the current JSON Schema for a pipeline
  config (field names, types, enums for `action`/`strategy`/`mcp_access`,
  which fields are required vs optional, defaults). The agent fetches
  this once at the start of a creation conversation so its generated
  YAML/JSON is structurally valid before it ever calls `validate_*`.
- `list_topics` (read, requires a Kafka admin-client connection from the
  Bridge) — lets the agent offer real topic names instead of guessing,
  and warn the person if a topic they named doesn't exist yet.
- `create_pipeline` (admin scope) — accepts a full pipeline config
  object (not a YAML diff), runs the same validation as
  `validate_pipeline_config`, and if valid, writes it into the running
  config store and hot-reloads. Distinct from `apply_pipeline_config`
  (Phase 6) in that it's additive (new pipeline name must not already
  exist) rather than replacing an existing one.

**Conversation flow (what actually happens end to end):**

1. Person describes intent in natural language to their AI agent.
2. Agent calls `get_pipeline_schema` (+ `list_topics` if useful) so it
   knows the real shape and real topic names to work with.
3. Agent fills in whatever the person specified and asks the person
   directly for anything required but missing (e.g. "you didn't say
   what happens if the callback times out — how many retries?") rather
   than inventing defaults for things that matter.
4. Agent calls `validate_pipeline_config` with the draft — this is a
   dry run, nothing is created yet.
5. Agent shows the resulting config (or a plain-language summary of it)
   back to the person for a yes/no before doing anything live.
6. Only on explicit confirmation does the agent call `create_pipeline`.
   This call requires an `admin`-scope token — same rule as
   `apply_pipeline_config` — and is audit-logged.
7. Bridge confirms creation; agent tells the person the pipeline name
   and where to watch it (dashboard link or `get_pipeline_status`).

**Why this is safe despite being "AI creates infra config":**
- Nothing is created without step 5's explicit confirmation — the agent
  is not authorized to skip straight from description to live pipeline.
- `create_pipeline` still runs full schema + semantic validation
  server-side (valid topic names, no duplicate pipeline name, sane
  numeric ranges) — the agent's output is never trusted blindly.
- Requires an `admin` token, so this capability can be withheld from any
  MCP connection that should only ever read or only ever operate
  existing pipelines (see the scope table below).
- Every creation is audit-logged the same as any other write action.

**Where this lands in the roadmap:** implement right after Phase 6's
core read/write tools work, since it reuses the same auth and audit
plumbing — treat it as **Phase 6b**, not a separate later phase.

## 10. MCP tool reference (target shape for Phase 6)

| Tool | Scope required | Pipeline `mcp_access` required | Notes |
|---|---|---|---|
| `list_pipelines` | any | (filters out `none`) | shows `mcp_access` value per pipeline in output so the agent knows what it can/can't do next |
| `get_pipeline_status` | any | `read_only` or `read_write` | throughput, lag, error rate |
| `get_metrics` | any | `read_only` or `read_write` | time-ranged metrics query |
| `list_dlq_messages` / `get_dlq_message` | any | `read_only` or `read_write` | read-only inspection |
| `pause_pipeline` / `resume_pipeline` | `operator`, `admin` | `read_write` | audit-logged |
| `retry_dlq_message` / `discard_dlq_message` | `operator`, `admin` | `read_write` | audit-logged |
| `validate_pipeline_config` | `admin` | n/a (validates a proposed config, doesn't touch a live one) | dry-run only |
| `apply_pipeline_config` | `admin` | n/a (acts on whichever pipeline the new config names) | audit-logged, requires explicit confirmation step in the client |

## 11. Open decisions to revisit

- Embed MCP server in the engine binary vs. a separate sidecar process
  — decide once Phase 5's REST API shape is settled.
- Whether `reject` needs a synchronous response path back to an
  upstream caller, or is always fire-and-forget into `reject_topic`
  (current assumption: fire-and-forget/async).
- Single Git repo (monorepo, two binaries) vs. two repos — leaning
  monorepo for now to share config/schema types.
- Manual partition pinning was requested (an operator choosing exactly
  which partition each worker reads, instead of Kafka's group
  coordinator deciding). Checked against kafka-go: `GroupID` and
  `Partition` are mutually exclusive on a Reader, so manual pinning
  means giving up group-committed offsets and persisting them
  ourselves — reintroducing the "new storage layer" §2 rules out, and
  losing the automatic-failover behavior `workers: N` already gets for
  free. Current recommendation: don't build it — `workers: N` plus
  Kafka's own assignor already distributes a topic's partitions across
  workers without that cost. Revisit only if a concrete need for
  guaranteed partition→worker pinning shows up that `workers: N` can't
  satisfy.
- A general NiFi/n8n-style DAG engine (arbitrary chained
  processors — transform → filter → enrich → callback — each
  independently start/stoppable, wired together in a UI) was raised
  and explicitly declined in favor of two bounded features that cover
  the same real use cases without the redesign: chaining separate
  `pipelines:` entries via intermediate topics (§5, already supported,
  no engine change needed) for multi-stage flows, and
  `post_callback_rules` (§6, Phase 3 extensions) for branching on a
  callback's result within one stage. A full DAG engine would directly
  contradict §2/§3's positioning against n8n/NiFi/Camel and is a
  multi-week redesign, not a config addition — revisit only if a real
  deployment hits a case neither bounded feature can express, and treat
  that as a deliberate positioning change requiring its own decision,
  not an incremental add.
