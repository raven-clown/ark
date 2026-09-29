package config

import (
	"fmt"
	"regexp"
)

// SourceTypeHTTP turns HTTP requests into Kafka messages.
const SourceTypeHTTP = "http"

// Source brings data into Kafka from outside, so a pipeline can read it
// like any other topic. An HTTP source answers POST /ingest/<name>.
type Source struct {
	Name  string `yaml:"name"`
	Type  string `yaml:"type"`
	Topic string `yaml:"topic"`
	// TokenEnv names the environment variable holding the bearer token
	// callers must send; the token itself never sits in the config.
	TokenEnv string `yaml:"token_env"`
	// Key is a dotted path into the JSON body used as the Kafka key, so
	// messages about the same thing keep their order.
	Key          string     `yaml:"key,omitempty"`
	MaxBodyBytes int        `yaml:"max_body_bytes,omitempty"`
	Partitions   int        `yaml:"partitions,omitempty"`
	DataRules    *DataRules `yaml:"data_rules,omitempty"`
}

var (
	sourceName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	envVarName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
)

func validateSources(sources []Source) error {
	seen := map[string]bool{}
	for i := range sources {
		s := &sources[i]
		if !sourceName.MatchString(s.Name) {
			return fmt.Errorf("sources[%d]: name %q must be lowercase letters, digits, - or _", i, s.Name)
		}
		if seen[s.Name] {
			return fmt.Errorf("sources: %q is used twice", s.Name)
		}
		seen[s.Name] = true
		if s.Type == "" {
			s.Type = SourceTypeHTTP
		}
		if s.Type != SourceTypeHTTP {
			return fmt.Errorf("source %q: unknown type %q (the only one is http)", s.Name, s.Type)
		}
		if s.Topic == "" {
			return fmt.Errorf("source %q: topic is required", s.Name)
		}
		if !envVarName.MatchString(s.TokenEnv) {
			return fmt.Errorf("source %q: token_env must name an environment variable such as ARK_SOURCE_%s_TOKEN", s.Name, "SHOP")
		}
		if s.MaxBodyBytes == 0 {
			s.MaxBodyBytes = 1 << 20
		}
		if s.MaxBodyBytes < 1 || s.MaxBodyBytes > 16<<20 {
			return fmt.Errorf("source %q: max_body_bytes must be between 1 and %d", s.Name, 16<<20)
		}
		if s.Partitions == 0 {
			s.Partitions = 1
		}
		if s.DataRules != nil {
			if err := validateDataRules(*s.DataRules); err != nil {
				return fmt.Errorf("source %q: data_rules: %w", s.Name, err)
			}
		}
	}
	return nil
}
