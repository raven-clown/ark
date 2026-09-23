package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
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
	Name              string           `yaml:"name"`
	Tenant            string           `yaml:"tenant"`
	MCPAccess         MCPAccess        `yaml:"mcp_access"`
	SourceTopic       string           `yaml:"source_topic"`
	DestinationTopic  string           `yaml:"destination_topic"`
	DeadLetterTopic   string           `yaml:"dead_letter_topic"`
	RejectTopic       string           `yaml:"reject_topic"`
	ConsumerGroup     string           `yaml:"consumer_group"`
	Workers           int              `yaml:"workers"`
	Consumer          ConsumerSettings `yaml:"consumer"`
	Target            Target           `yaml:"target"`
	Concurrency       Concurrency      `yaml:"concurrency"`
	Retry             Retry            `yaml:"retry"`
	FastPathRules     []FastPathRule   `yaml:"fast_path_rules"`
	PostCallbackRules []FastPathRule   `yaml:"post_callback_rules"`
	Enabled           *bool            `yaml:"enabled"`
}

type Config struct {
	Brokers   []string   `yaml:"brokers"`
	Pipelines []Pipeline `yaml:"pipelines"`
}

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
	for i := range cfg.Pipelines {
		p := &cfg.Pipelines[i]
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
	}
}

func (c *Config) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("brokers: at least one broker address is required")
	}

	seen := map[string]bool{}
	for i, p := range c.Pipelines {
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
