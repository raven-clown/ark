package config

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

type MCPAccess string

const (
	MCPAccessReadOnly  MCPAccess = "read_only"
	MCPAccessReadWrite MCPAccess = "read_write"
	MCPAccessNone      MCPAccess = "none"
)

type TargetMode string

const (
	TargetModeSingleURL TargetMode = "single_url"
	TargetModeMultiURL  TargetMode = "multi_url"
)

type TargetStrategy string

const (
	StrategyStickyPartition TargetStrategy = "sticky_partition"
	StrategyRoundRobin      TargetStrategy = "round_robin"
	StrategyLeastInFlight   TargetStrategy = "least_inflight"
)

type Ordering string

const (
	OrderingPerKey       Ordering = "per_key"
	OrderingPerPartition Ordering = "per_partition"
	OrderingNone         Ordering = "none"
)

// OnExhausted says what happens to a message that can't be completed and
// has nowhere to be routed. The only non-default value is "block": keep
// retrying it in place, holding back its partition, instead of requiring a
// dead_letter_topic.
type OnExhausted string

const OnExhaustedBlock OnExhausted = "block"

type RuleAction string

const (
	ActionPassThrough    RuleAction = "pass_through"
	ActionReject         RuleAction = "reject"
	ActionDrop           RuleAction = "drop"
	ActionDeadLetter     RuleAction = "dead_letter"
	ActionTransformRoute RuleAction = "transform_route"
)

type Target struct {
	Mode            TargetMode     `yaml:"mode"`
	URL             string         `yaml:"url"`
	URLs            []string       `yaml:"urls"`
	Strategy        TargetStrategy `yaml:"strategy"`
	HealthCheckURL  string         `yaml:"health_check_url"`
	HealthCheckURLs []string       `yaml:"health_check_urls"`
	HealthCheckSecs int            `yaml:"health_check_interval_seconds"`
	TimeoutMs       int            `yaml:"timeout_ms"`
	RejectStatuses  []int          `yaml:"reject_statuses"`
}

// IsReject reports whether a callback status routes the message to
// reject_topic. With no reject_statuses configured, every 4xx except the
// "try again later" ones (408, 425, 429) is a reject.
func (t Target) IsReject(status int) bool {
	if len(t.RejectStatuses) > 0 {
		for _, s := range t.RejectStatuses {
			if s == status {
				return true
			}
		}
		return false
	}
	switch status {
	case 408, 425, 429:
		return false
	}
	return status >= 400 && status < 500
}

type ConsumerSettings struct {
	MaxPollRecords    int `yaml:"max_poll_records"`
	MaxPollIntervalMs int `yaml:"max_poll_interval_ms"`
}

type Concurrency struct {
	MaxInFlight int `yaml:"max_in_flight"`
}

type Retry struct {
	MaxAttempts int `yaml:"max_attempts"`
	BackoffMs   int `yaml:"backoff_ms"`
}

type FastPathRule struct {
	Name                string     `yaml:"name"`
	Condition           string     `yaml:"condition"`
	Action              RuleAction `yaml:"action"`
	DestinationOverride string     `yaml:"destination_override"`
	WebhookOverride     string     `yaml:"webhook_override"`
}

type Pipeline struct {
	Name   string `yaml:"name"`
	Tenant string `yaml:"tenant"`
	// Project is the project this pipeline belongs to (see projects).
	Project           string           `yaml:"project,omitempty"`
	MCPAccess         MCPAccess        `yaml:"mcp_access"`
	SourceTopic       string           `yaml:"source_topic"`
	DestinationTopic  string           `yaml:"destination_topic"`
	DeadLetterTopic   string           `yaml:"dead_letter_topic"`
	RejectTopic       string           `yaml:"reject_topic"`
	ConsumerGroup     string           `yaml:"consumer_group"`
	Workers           int              `yaml:"workers"`
	Ordering          Ordering         `yaml:"ordering"`
	Consumer          ConsumerSettings `yaml:"consumer"`
	Target            Target           `yaml:"target"`
	Concurrency       Concurrency      `yaml:"concurrency"`
	Retry             Retry            `yaml:"retry"`
	FastPathRules     []FastPathRule   `yaml:"fast_path_rules"`
	PostCallbackRules []FastPathRule   `yaml:"post_callback_rules"`
	Placement         Placement        `yaml:"placement"`
	CircuitBreaker    CircuitBreaker   `yaml:"circuit_breaker"`
	DataRules         DataRules        `yaml:"data_rules"`
	DeadLetterRedrive Redrive          `yaml:"dead_letter_redrive"`
	OnExhausted       OnExhausted      `yaml:"on_exhausted"`
	Enabled           *bool            `yaml:"enabled"`
	// Flow replaces target, rules and output topics with steps joined in any
	// shape. Pipelines without one keep the fixed path.
	Flow *Flow `yaml:"flow,omitempty"`
}

type Cluster struct {
	Enabled                  bool              `yaml:"enabled"`
	Name                     string            `yaml:"name"`
	NodeID                   string            `yaml:"node_id"`
	HeartbeatIntervalSeconds int               `yaml:"heartbeat_interval_seconds"`
	NodeTimeoutSeconds       int               `yaml:"node_timeout_seconds"`
	PlacementIntervalSeconds int               `yaml:"placement_interval_seconds"`
	Labels                   map[string]string `yaml:"labels"`
}

// DataRules describe what a valid message looks like. Every message is
// checked before fast_path_rules and before the callback; one that breaks a
// rule is handled by OnViolation, with the broken rules as the reason.
type DataRules struct {
	// OnViolation: reject (default; reject_topic, or the DLQ without one),
	// dead_letter, or tag (let it through with an X-Ark-Violations header).
	OnViolation string `yaml:"on_violation,omitempty"`
	// AllowUnknownFields: false flags top-level fields no rule mentions.
	AllowUnknownFields *bool        `yaml:"allow_unknown_fields,omitempty"`
	MaxBytes           int          `yaml:"max_bytes,omitempty"`
	Key                *ValueRule   `yaml:"key,omitempty"`
	Headers            []HeaderRule `yaml:"headers,omitempty"`
	Fields             []FieldRule  `yaml:"fields,omitempty"`
}

func (d DataRules) Enabled() bool {
	return len(d.Fields) > 0 || d.Key != nil || len(d.Headers) > 0 || d.MaxBytes > 0 || (d.AllowUnknownFields != nil && !*d.AllowUnknownFields)
}

// ValueRule constrains one string value (a message key or a header).
type ValueRule struct {
	Required  bool     `yaml:"required,omitempty"`
	Pattern   string   `yaml:"pattern,omitempty"`
	Enum      []string `yaml:"enum,omitempty"`
	MaxLength int      `yaml:"max_length,omitempty"`
}

type HeaderRule struct {
	Name      string `yaml:"name"`
	ValueRule `yaml:",inline"`
}

// FieldRule constrains one field of a JSON message, addressed by a dot
// path such as "customer.id".
type FieldRule struct {
	Path      string   `yaml:"path"`
	Required  bool     `yaml:"required,omitempty"`
	Type      string   `yaml:"type,omitempty"`
	Min       *float64 `yaml:"min,omitempty"`
	Max       *float64 `yaml:"max,omitempty"`
	MinLength *int     `yaml:"min_length,omitempty"`
	MaxLength *int     `yaml:"max_length,omitempty"`
	Pattern   string   `yaml:"pattern,omitempty"`
	Enum      []string `yaml:"enum,omitempty"`
	Format    string   `yaml:"format,omitempty"`
}

// CircuitBreaker controls when ARK stops calling a failing target:
// after FailureThreshold different messages fail in a row it holds messages in place for
// CooldownSeconds before trying again.
type CircuitBreaker struct {
	FailureThreshold int `yaml:"failure_threshold"`
	CooldownSeconds  int `yaml:"cooldown_seconds"`
}

// Redrive automatically resends dead-lettered messages to source_topic
// once they are AfterSeconds old, at most MaxTimes per original message.
type Redrive struct {
	AfterSeconds int `yaml:"after_seconds"`
	MaxTimes     int `yaml:"max_times"`
}

func (r Redrive) Enabled() bool { return r.AfterSeconds > 0 }

// Placement restricts which cluster nodes may run a pipeline: only nodes
// whose cluster.labels contain every key/value in NodeSelector.
type Placement struct {
	NodeSelector map[string]string `yaml:"node_selector"`
}

type Topics struct {
	ReplicationFactor int `yaml:"replication_factor"`
}

// Assistant tunes the MCP assistant. Lexicon adds words (in any
// language) that signal an intent, on top of the built-in English, Thai
// and Chinese ones, e.g. {diagnose: ["langsam", "lento"]}.
type Assistant struct {
	Lexicon map[string][]string `yaml:"lexicon"`
	// Model is the default model for the console's assistant, used by
	// pipelines outside a project and projects without their own.
	Model *AssistantModel `yaml:"model,omitempty"`
}

type Config struct {
	Brokers []string `yaml:"brokers"`
	// Timezone (IANA name, e.g. Asia/Bangkok) used when showing times over
	// the API and MCP. Times are always ISO 8601 with an offset; storage and
	// logs stay in UTC. Default UTC.
	Timezone  string    `yaml:"timezone"`
	Assistant Assistant `yaml:"assistant"`
	Topics    Topics    `yaml:"topics"`
	Cluster   Cluster   `yaml:"cluster"`
	// Tuning holds engine-wide knobs; every one defaults to the value
	// ARK used before it was configurable.
	Tuning    tuning.Values `yaml:"tuning,omitempty"`
	Projects  []Project     `yaml:"projects,omitempty"`
	Pipelines []Pipeline    `yaml:"pipelines"`
}

// minBrokerSessionTimeoutSeconds is Kafka's default
// group.min.session.timeout.ms; a lower node_timeout_seconds makes every
// election JoinGroup fail.
const minBrokerSessionTimeoutSeconds = 6

func (p *Pipeline) IsEnabled() bool {
	if p.Enabled == nil {
		return true
	}
	return *p.Enabled
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path) // #nosec G304 -- path is an operator-supplied CLI flag, not untrusted network input
	if err != nil {
		return nil, fmt.Errorf("reading config %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config %s: %w", path, err)
	}

	applyDefaults(&cfg)

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config %s: %w", path, err)
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Timezone == "" {
		cfg.Timezone = "UTC"
	}
	if cfg.Topics.ReplicationFactor == 0 {
		cfg.Topics.ReplicationFactor = 3
	}
	if cfg.Cluster.Enabled {
		if cfg.Cluster.Name == "" {
			cfg.Cluster.Name = "default"
		}
		if cfg.Cluster.HeartbeatIntervalSeconds == 0 {
			cfg.Cluster.HeartbeatIntervalSeconds = 5
		}
		if cfg.Cluster.NodeTimeoutSeconds == 0 {
			cfg.Cluster.NodeTimeoutSeconds = 20
		}
		if cfg.Cluster.PlacementIntervalSeconds == 0 {
			cfg.Cluster.PlacementIntervalSeconds = 5
		}
	}

	for i := range cfg.Pipelines {
		ApplyPipelineDefaults(&cfg.Pipelines[i])
	}
	for i := range cfg.Projects {
		ApplyProjectDefaults(&cfg.Projects[i])
	}
	if cfg.Assistant.Model != nil && cfg.Assistant.Model.MaxSteps == 0 {
		cfg.Assistant.Model.MaxSteps = 8
	}
}

// ApplyPipelineDefaults fills every unset pipeline field with its default,
// the same way loading a config file does.
func ApplyPipelineDefaults(p *Pipeline) {
	if p.MCPAccess == "" {
		p.MCPAccess = MCPAccessReadOnly
	}
	if p.Target.Mode == "" {
		p.Target.Mode = TargetModeSingleURL
	}
	if p.Target.Mode == TargetModeMultiURL && p.Target.Strategy == "" {
		p.Target.Strategy = StrategyRoundRobin
	}
	if p.Target.HealthCheckURL != "" && p.Target.HealthCheckSecs == 0 {
		p.Target.HealthCheckSecs = 10
	}
	if len(p.Target.HealthCheckURLs) > 0 && p.Target.HealthCheckSecs == 0 {
		p.Target.HealthCheckSecs = 10
	}
	if p.Workers < 1 {
		p.Workers = 1
	}
	if p.Consumer.MaxPollRecords == 0 {
		p.Consumer.MaxPollRecords = 50
	}
	if p.Consumer.MaxPollIntervalMs == 0 {
		p.Consumer.MaxPollIntervalMs = 300000
	}
	if p.Concurrency.MaxInFlight == 0 {
		p.Concurrency.MaxInFlight = 10
	}
	if p.Retry.MaxAttempts == 0 {
		p.Retry.MaxAttempts = 3
	}
	if p.Retry.BackoffMs == 0 {
		p.Retry.BackoffMs = 1000
	}
	if p.Target.TimeoutMs == 0 {
		p.Target.TimeoutMs = 30000
	}
	if p.Ordering == "" {
		p.Ordering = OrderingPerKey
	}
	if p.CircuitBreaker.FailureThreshold == 0 {
		p.CircuitBreaker.FailureThreshold = 5
	}
	if p.CircuitBreaker.CooldownSeconds == 0 {
		p.CircuitBreaker.CooldownSeconds = 30
	}
	if p.Flow != nil {
		applyFlowDefaults(p)
	}
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers: at least one broker address is required")
	}

	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return fmt.Errorf("timezone %q is not a valid IANA time zone name (e.g. UTC, Asia/Bangkok): %w", c.Timezone, err)
	}

	if c.Topics.ReplicationFactor < 1 {
		return fmt.Errorf("topics.replication_factor must be at least 1, got %d", c.Topics.ReplicationFactor)
	}

	if c.Cluster.Enabled {
		cl := c.Cluster
		if cl.HeartbeatIntervalSeconds < 1 || cl.PlacementIntervalSeconds < 1 {
			return fmt.Errorf("cluster: heartbeat_interval_seconds and placement_interval_seconds must be at least 1")
		}
		if cl.NodeTimeoutSeconds < minBrokerSessionTimeoutSeconds {
			return fmt.Errorf("cluster: node_timeout_seconds (%d) must be at least %d, Kafka's default group.min.session.timeout.ms", cl.NodeTimeoutSeconds, minBrokerSessionTimeoutSeconds)
		}
		if cl.NodeTimeoutSeconds <= cl.HeartbeatIntervalSeconds {
			return fmt.Errorf("cluster: node_timeout_seconds (%d) must be greater than heartbeat_interval_seconds (%d)", cl.NodeTimeoutSeconds, cl.HeartbeatIntervalSeconds)
		}
		for _, r := range cl.Name {
			if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				return fmt.Errorf("cluster.name %q may only contain lowercase letters, digits, '-' and '_'", cl.Name)
			}
		}
	}

	if err := c.Tuning.Validate(); err != nil {
		return err
	}
	if err := ValidateModel("assistant.model", c.Assistant.Model); err != nil {
		return err
	}
	if err := ValidateProjects(c.Projects, c.Pipelines); err != nil {
		return err
	}
	return ValidatePipelines(c.Pipelines)
}

// ValidatePipelines checks a pipeline set on its own, for config that
// arrives from somewhere other than a full file (the cluster config topic).
func ValidatePipelines(pipelines []Pipeline) error {
	seen := map[string]bool{}
	for i, p := range pipelines {
		if p.Name == "" {
			return fmt.Errorf("pipelines[%d]: name is required", i)
		}
		if seen[p.Name] {
			return fmt.Errorf("pipelines[%d]: duplicate pipeline name %q", i, p.Name)
		}
		seen[p.Name] = true

		if p.SourceTopic == "" {
			return fmt.Errorf("pipeline %q: source_topic is required", p.Name)
		}

		if !p.IsEnabled() {
			continue
		}

		if p.Flow != nil {
			if err := validateFlowPipeline(p); err != nil {
				return err
			}
			continue
		}

		if p.DestinationTopic == "" {
			return fmt.Errorf("pipeline %q: destination_topic is required to enable this pipeline", p.Name)
		}

		switch p.Target.Mode {
		case TargetModeSingleURL:
			if p.Target.URL == "" {
				return fmt.Errorf("pipeline %q: target.url is required for target.mode single_url to enable this pipeline", p.Name)
			}
		case TargetModeMultiURL:
			if len(p.Target.URLs) == 0 {
				return fmt.Errorf("pipeline %q: target.urls is required for target.mode multi_url to enable this pipeline", p.Name)
			}
			switch p.Target.Strategy {
			case StrategyRoundRobin, StrategyLeastInFlight, StrategyStickyPartition:
			default:
				return fmt.Errorf("pipeline %q: unknown target.strategy %q", p.Name, p.Target.Strategy)
			}
			if n := len(p.Target.HealthCheckURLs); n > 0 && n != len(p.Target.URLs) {
				return fmt.Errorf("pipeline %q: target.health_check_urls must have the same length as target.urls (%d) if set, got %d", p.Name, len(p.Target.URLs), n)
			}
		default:
			return fmt.Errorf("pipeline %q: unknown target.mode %q", p.Name, p.Target.Mode)
		}

		if p.ConsumerGroup == "" {
			return fmt.Errorf("pipeline %q: consumer_group is required to enable this pipeline", p.Name)
		}

		switch p.OnExhausted {
		case "":
			if p.DeadLetterTopic == "" {
				return fmt.Errorf("pipeline %q: dead_letter_topic is required, so a message that can't be delivered is never dropped; set on_exhausted: block to retry such messages in place instead", p.Name)
			}
		case OnExhaustedBlock:
		default:
			return fmt.Errorf("pipeline %q: unknown on_exhausted %q (the only option is block)", p.Name, p.OnExhausted)
		}

		if rd := p.DeadLetterRedrive; rd != (Redrive{}) {
			if p.DeadLetterTopic == "" {
				return fmt.Errorf("pipeline %q: dead_letter_redrive needs a dead_letter_topic", p.Name)
			}
			if rd.AfterSeconds < 1 || rd.MaxTimes < 1 {
				return fmt.Errorf("pipeline %q: dead_letter_redrive needs after_seconds and max_times of at least 1", p.Name)
			}
		}

		switch p.Ordering {
		case OrderingPerKey, OrderingPerPartition, OrderingNone:
		default:
			return fmt.Errorf("pipeline %q: unknown ordering %q (want per_key, per_partition or none)", p.Name, p.Ordering)
		}

		if err := validateDataRules(p.DataRules); err != nil {
			return fmt.Errorf("pipeline %q: data_rules: %w", p.Name, err)
		}

		if p.CircuitBreaker.FailureThreshold < 1 || p.CircuitBreaker.CooldownSeconds < 1 {
			return fmt.Errorf("pipeline %q: circuit_breaker.failure_threshold and cooldown_seconds must be at least 1", p.Name)
		}

		if p.Target.TimeoutMs < 1 {
			return fmt.Errorf("pipeline %q: target.timeout_ms must be positive", p.Name)
		}
		for _, s := range p.Target.RejectStatuses {
			if s < 400 || s > 499 {
				return fmt.Errorf("pipeline %q: target.reject_statuses may only contain 4xx codes, got %d", p.Name, s)
			}
		}

		if err := validateRules(p.Name, "fast_path_rules", p.FastPathRules); err != nil {
			return err
		}
		if err := validateRules(p.Name, "post_callback_rules", p.PostCallbackRules); err != nil {
			return err
		}
	}

	return nil
}

func validateRules(pipelineName, field string, rules []FastPathRule) error {
	for j, r := range rules {
		if r.Condition == "" {
			return fmt.Errorf("pipeline %q: %s[%d]: condition is required", pipelineName, field, j)
		}
		switch r.Action {
		case ActionPassThrough, ActionReject, ActionDrop, ActionDeadLetter:
		case ActionTransformRoute:
			if r.DestinationOverride == "" && r.WebhookOverride == "" {
				return fmt.Errorf("pipeline %q: %s[%d]: transform_route requires destination_override or webhook_override", pipelineName, field, j)
			}
			if r.DestinationOverride != "" && r.WebhookOverride != "" {
				return fmt.Errorf("pipeline %q: %s[%d]: transform_route takes destination_override or webhook_override, not both", pipelineName, field, j)
			}
		default:
			return fmt.Errorf("pipeline %q: %s[%d]: unknown action %q", pipelineName, field, j, r.Action)
		}
	}
	return nil
}

var (
	fieldTypes   = map[string]bool{"": true, "string": true, "number": true, "integer": true, "boolean": true, "object": true, "array": true, "null": true}
	fieldFormats = map[string]bool{"": true, "email": true, "uuid": true, "date-time": true, "date": true, "url": true, "ipv4": true}
)

func validateDataRules(d DataRules) error {
	switch d.OnViolation {
	case "", "reject", "dead_letter", "tag":
	default:
		return fmt.Errorf("on_violation must be reject, dead_letter or tag, got %q", d.OnViolation)
	}
	checkValue := func(where string, v ValueRule) error {
		if v.Pattern != "" {
			if _, err := regexp.Compile(v.Pattern); err != nil {
				return fmt.Errorf("%s: pattern: %w", where, err)
			}
		}
		return nil
	}
	if d.Key != nil {
		if err := checkValue("key", *d.Key); err != nil {
			return err
		}
	}
	for i, h := range d.Headers {
		if h.Name == "" {
			return fmt.Errorf("headers[%d]: name is required", i)
		}
		if err := checkValue("header "+h.Name, h.ValueRule); err != nil {
			return err
		}
	}
	for i, f := range d.Fields {
		if f.Path == "" {
			return fmt.Errorf("fields[%d]: path is required", i)
		}
		if !fieldTypes[f.Type] {
			return fmt.Errorf("field %s: unknown type %q (string, number, integer, boolean, object, array, null)", f.Path, f.Type)
		}
		if !fieldFormats[f.Format] {
			return fmt.Errorf("field %s: unknown format %q (email, uuid, date-time, date, url, ipv4)", f.Path, f.Format)
		}
		if f.Pattern != "" {
			if _, err := regexp.Compile(f.Pattern); err != nil {
				return fmt.Errorf("field %s: pattern: %w", f.Path, err)
			}
		}
		if f.Min != nil && f.Max != nil && *f.Min > *f.Max {
			return fmt.Errorf("field %s: min is greater than max", f.Path)
		}
	}
	return nil
}
