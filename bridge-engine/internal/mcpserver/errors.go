package mcpserver

import (
	"strings"
	"time"
)

// knownError explains one family of errors ARK or Kafka can produce, in
// terms an operator can act on.
type knownError struct {
	Match     []string `json:"-"`
	Title     string   `json:"title"`
	Component string   `json:"component"`
	Meaning   string   `json:"meaning"`
	Causes    []string `json:"likely_causes"`
	Fixes     []string `json:"how_to_fix"`
	Transient bool     `json:"usually_fixes_itself"`
}

var errorCatalog = []knownError{
	{
		Match:     []string{"unknown topic or partition"},
		Title:     "Kafka topic does not exist (yet)",
		Component: "Kafka broker",
		Meaning:   "ARK asked for a topic or partition the broker doesn't have.",
		Causes:    []string{"The topic is being created and hasn't propagated yet (normal for a few seconds after a pipeline starts).", "A typo in source_topic / destination_topic / dead_letter_topic.", "The broker has auto.create.topics.enable=false and nobody created the topic."},
		Fixes:     []string{"If it clears within seconds, ignore it.", "Check the topic names with list_topics.", "Create the topic, or let ARK create it (it creates source, DLQ and reject topics it needs)."},
		Transient: true,
	},
	{
		Match:     []string{"group coordinator not available", "coordinator not available", "not coordinator"},
		Title:     "Kafka consumer group coordinator not ready",
		Component: "Kafka broker",
		Meaning:   "The broker that manages consumer groups isn't ready to answer yet.",
		Causes:    []string{"Kafka just started and its internal __consumer_offsets topic isn't ready.", "A broker is restarting or leadership is moving."},
		Fixes:     []string{"Wait; ARK retries on its own. If it lasts more than a minute, check broker health."},
		Transient: true,
	},
	{
		Match:     []string{"rebalance in progress", "generation has ended", "illegal generation"},
		Title:     "Consumer group rebalance",
		Component: "Kafka consumer group",
		Meaning:   "Group membership changed (a worker or node joined or left) and partitions are being reassigned.",
		Causes:    []string{"Scaling workers, a node joining or leaving, a restart, or a leader handoff in cluster mode."},
		Fixes:     []string{"Nothing to do; it settles in seconds. Frequent rebalances mean nodes or workers keep restarting (see recent events)."},
		Transient: true,
	},
	{
		Match:     []string{"no such host", "lookup "},
		Title:     "Hostname can't be resolved",
		Component: "Network / DNS",
		Meaning:   "ARK couldn't turn a hostname into an address.",
		Causes:    []string{"A typo in brokers or a target URL.", "Kafka advertises a hostname (advertised.listeners) that only resolves inside its own network, e.g. 'kafka' inside docker-compose, while ARK runs outside it."},
		Fixes:     []string{"Check the hostname in the error against brokers / target URLs.", "Make Kafka advertise an address ARK can reach, or run ARK on the same network."},
	},
	{
		Match:     []string{"no broker reachable", "dialing", "connect: connection refused", "i/o timeout"},
		Title:     "Can't connect",
		Component: "Network",
		Meaning:   "A TCP connection was refused or timed out.",
		Causes:    []string{"If the address is a broker: Kafka is down or the brokers setting points at the wrong host/port.", "If the address is your app: the target service is down or listening on another port.", "A firewall or security group in between."},
		Fixes:     []string{"Check the address in the error is reachable from the machine running ARK.", "For targets, the circuit breaker holds messages until it recovers; nothing is lost."},
	},
	{
		Match:     []string{"context deadline exceeded", "client.timeout exceeded", "timeout awaiting response"},
		Title:     "Callback timed out",
		Component: "Your target app",
		Meaning:   "The target didn't answer within target.timeout_ms.",
		Causes:    []string{"The app is overloaded or slow for some messages.", "target.timeout_ms is lower than the app's normal latency."},
		Fixes:     []string{"Compare avg callback latency (diagnose_pipeline) with target.timeout_ms.", "Raise timeout_ms, lower max_in_flight to send less at once, or scale the app."},
	},
	{
		Match:     []string{"answered status 5", "status 50", "status 502", "status 503", "status 504"},
		Title:     "Target returned a server error (5xx)",
		Component: "Your target app",
		Meaning:   "The app received the message but failed while handling it.",
		Causes:    []string{"A bug or dependency failure inside the app.", "The app is overloaded (503) or behind a gateway that can't reach it (502/504)."},
		Fixes:     []string{"Check the app's own logs using the X-Correlation-ID header ARK sent.", "ARK retries up to retry.max_attempts, then dead-letters; repeated failures open the breaker."},
	},
	{
		Match:     []string{"reject status", "answered status 4", "rejected"},
		Title:     "Target rejected the message (4xx)",
		Component: "Your target app / the data",
		Meaning:   "The app says this specific message is invalid, so retrying won't help.",
		Causes:    []string{"Bad or unexpected data from whoever produces to the source topic.", "An API change in the app that old messages don't match."},
		Fixes:     []string{"Look at the entries in the reject topic (list_dlq_messages kind=reject) and fix the producer or the app.", "target.reject_statuses controls which 4xx codes count as rejects."},
	},
	{
		Match:     []string{"answered 429", "answered 425", "answered 408", "retry later", "rate limit"},
		Title:     "Target asked ARK to slow down",
		Component: "Your target app",
		Meaning:   "429/425/408: come back later. ARK waits (honoring Retry-After) and retries without using up attempts.",
		Causes:    []string{"ARK sends more concurrent requests than the app or its gateway allows."},
		Fixes:     []string{"Lower concurrency.max_in_flight or workers, or raise the app's rate limit.", "See recommend_tuning for concurrency numbers."},
		Transient: true,
	},
	{
		Match:     []string{"circuit open", "circuit breaker"},
		Title:     "Circuit breaker open",
		Component: "ARK",
		Meaning:   "The target failed repeatedly, so ARK pauses calls to it and holds messages in place.",
		Causes:    []string{"The target is down or failing every request."},
		Fixes:     []string{"Fix the target; the breaker closes itself (faster with target.health_check_url set)."},
		Transient: true,
	},
	{
		Match:     []string{"every target.urls endpoint is unhealthy"},
		Title:     "All backends unhealthy",
		Component: "ARK multi_url pool",
		Meaning:   "Every URL in target.urls failed its health check, so messages wait.",
		Causes:    []string{"All app instances are down, or the health check URLs are wrong."},
		Fixes:     []string{"Check target.health_check_urls answer non-5xx."},
	},
	{
		Match:     []string{"no topic configured", "on_exhausted: block"},
		Title:     "Message has nowhere to go",
		Component: "ARK config",
		Meaning:   "The pipeline uses on_exhausted: block, so a message that can't be completed is retried in place and holds back its partition.",
		Causes:    []string{"The target keeps failing or rejecting this message and there's no dead_letter_topic."},
		Fixes:     []string{"Fix the target, or add a dead_letter_topic so such messages move aside instead of blocking."},
	},
	{
		Match:     []string{"message size too large", "message_too_large", "record is too large"},
		Title:     "Message too large for Kafka",
		Component: "Kafka broker",
		Meaning:   "A produced message is bigger than the topic's max.message.bytes.",
		Causes:    []string{"The callback response (which becomes the destination message) is bigger than the input."},
		Fixes:     []string{"Raise max.message.bytes on the destination topic, or return a smaller response."},
	},
	{
		Match:     []string{"authorization failed", "sasl", "not authorized"},
		Title:     "Kafka refused access",
		Component: "Kafka security",
		Meaning:   "The broker's ACLs or authentication rejected ARK.",
		Causes:    []string{"Missing ACLs for a topic or consumer group ARK uses (including __ark_* topics in cluster mode)."},
		Fixes:     []string{"Grant ARK's principal read/write on the pipeline topics, its consumer groups, and the __ark_ topics."},
	},
	{
		Match:     []string{"invalid session timeout", "session timeout"},
		Title:     "Session timeout rejected by Kafka",
		Component: "ARK cluster config",
		Meaning:   "cluster.node_timeout_seconds is outside the broker's allowed group session timeout range.",
		Causes:    []string{"node_timeout_seconds below group.min.session.timeout.ms or above group.max.session.timeout.ms."},
		Fixes:     []string{"Set node_timeout_seconds within the broker's range (default 6 to 1800 seconds)."},
	},
	{
		Match:     []string{"invalid or missing bearer token", "401", "unauthorized"},
		Title:     "Missing or wrong API token",
		Component: "ARK REST/MCP auth",
		Meaning:   "The request didn't carry a token ARK knows.",
		Causes:    []string{"No Authorization: Bearer header, or a token from the other token set (REST and MCP tokens are separate)."},
		Fixes:     []string{"REST uses ARK_API_*_TOKENS, MCP uses ARK_MCP_*_TOKENS."},
	},
	{
		Match:     []string{"mcp_access"},
		Title:     "Pipeline not writable over MCP",
		Component: "ARK MCP permissions",
		Meaning:   "The pipeline's mcp_access doesn't allow this action.",
		Causes:    []string{"mcp_access is read_only (or none) for this pipeline, which caps any token."},
		Fixes:     []string{"An operator can change mcp_access in the config if agents should be allowed to act on it."},
	},
	{
		Match:     []string{"deposed leader"},
		Title:     "Stale leader write ignored",
		Component: "ARK cluster",
		Meaning:   "A node that just lost leadership wrote a placement after losing it; the others ignored it, as designed.",
		Causes:    []string{"A normal leader handoff."},
		Fixes:     []string{"Nothing to do."},
		Transient: true,
	},
	{
		Match:     []string{"validating config", "is required", "unknown "},
		Title:     "Config validation error",
		Component: "ARK config",
		Meaning:   "The config was refused before being applied; the running pipelines are unchanged.",
		Causes:    []string{"A missing or invalid field; the message names it."},
		Fixes:     []string{"See get_pipeline_schema for the field, then validate_pipeline_config before applying."},
	},
}

type errorOccurrence struct {
	Time     time.Time `json:"time"`
	Pipeline string    `json:"pipeline,omitempty"`
	Kind     string    `json:"event"`
	Message  string    `json:"message"`
}

type explainErrorOut struct {
	Matches     []knownError      `json:"matches"`
	Occurrences []errorOccurrence `json:"recent_occurrences"`
	Note        string            `json:"note,omitempty"`
}

func explainError(d Deps, text string) explainErrorOut {
	log := d.events()
	lower := strings.ToLower(text)
	var out explainErrorOut
	for _, k := range errorCatalog {
		for _, m := range k.Match {
			if strings.Contains(lower, m) {
				out.Matches = append(out.Matches, k)
				break
			}
		}
	}

	needle := lower
	if len(needle) > 60 {
		needle = needle[:60]
	}
	for _, e := range log.Recent("", time.Time{}, 2000) {
		hay := strings.ToLower(e.Message)
		for _, v := range e.Details {
			hay += " " + strings.ToLower(v)
		}
		if strings.Contains(hay, needle) {
			out.Occurrences = append(out.Occurrences, errorOccurrence{Time: d.localize(e.Time), Pipeline: e.Pipeline, Kind: string(e.Kind), Message: e.Message})
			if len(out.Occurrences) == 10 {
				break
			}
		}
	}

	if len(out.Matches) == 0 {
		out.Note = "Not a known error pattern. Check recent_occurrences for where it happened, and diagnose_pipeline for that pipeline."
	}
	return out
}
