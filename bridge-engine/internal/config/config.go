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
	ActionPassThrough RuleAction = "pass_through"
	ActionReject      RuleAction = "reject"
	ActionDrop        RuleAction = "drop"
	ActionDeadLetter  RuleAction = "dead_letter"
)

type Target struct {
	Mode     TargetMode     `yaml:"mode"`
	URL      string         `yaml:"url"`
	URLs     []string       `yaml:"urls"`
	Strategy TargetStrategy `yaml:"strategy"`
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
	Name      string     `yaml:"name"`
	Condition string     `yaml:"condition"`
	Action    RuleAction `yaml:"action"`
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
	Consumer          ConsumerSettings `yaml:"consumer"`
	Target            Target           `yaml:"target"`
	Concurrency       Concurrency      `yaml:"concurrency"`
	Retry             Retry            `yaml:"retry"`
	FastPathRules     []FastPathRule   `yaml:"fast_path_rules"`
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
	raw, err := os.ReadFile(path)
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
		default:
			return fmt.Errorf("pipeline %q: unknown target.mode %q", p.Name, p.Target.Mode)
		}

		if p.ConsumerGroup == "" {
			return fmt.Errorf("pipeline %q: consumer_group is required to enable this pipeline", p.Name)
		}

		for j, r := range p.FastPathRules {
			switch r.Action {
			case ActionPassThrough, ActionReject, ActionDrop, ActionDeadLetter:
			default:
				return fmt.Errorf("pipeline %q: fast_path_rules[%d]: unknown action %q", p.Name, j, r.Action)
			}
		}
	}

	return nil
}
