package config

import (
	"fmt"
	"regexp"
	"strings"
	"text/template" // nosemgrep: go.lang.security.audit.xss.import-text-template.import-text-template -- builds request bodies for other services, not HTML
)

var templateStubs = template.FuncMap{"json": func(any) string { return "" }, "path": func(any) string { return "" }}

var urlTemplateHost = regexp.MustCompile(`^(https?://[^/?#{]+)[/?]`)

func URLTemplateHost(u string) (string, bool) {
	m := urlTemplateHost.FindStringSubmatch(u[:strings.Index(u+"{{", "{{")])
	if m == nil {
		return "", false
	}
	return m[1], true
}

// StepType is what a flow step does with the message it receives.
type StepType string

const (
	StepCall      StepType = "call"
	StepCondition StepType = "condition"
	StepCheck     StepType = "data_check"
	// StepTopic produces the message to a topic and continues on next.
	StepTopic      StepType = "topic"
	StepWebhook    StepType = "webhook"
	StepReject     StepType = "reject"
	StepDeadLetter StepType = "dead_letter"
	// StepDrop discards the message.
	StepDrop     StepType = "drop"
	StepDatabase StepType = "database"
)

type Database struct {
	// Driver is postgres, the only one so far.
	Driver   string            `yaml:"driver"`
	DSNEnv   string            `yaml:"dsn_env"`
	Table    string            `yaml:"table"`
	Columns  map[string]string `yaml:"columns"`
	UpsertOn []string          `yaml:"upsert_on,omitempty"`
}

var sqlIdent = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func SQLIdent(name string, qualified bool) bool {
	parts := []string{name}
	if qualified {
		parts = strings.Split(name, ".")
		if len(parts) > 2 {
			return false
		}
	}
	for _, p := range parts {
		if !sqlIdent.MatchString(p) {
			return false
		}
	}
	return true
}

func validateDatabase(d *Database) error {
	if d == nil {
		return fmt.Errorf("database is required")
	}
	if d.Driver != "postgres" {
		return fmt.Errorf("database.driver must be postgres, got %q", d.Driver)
	}
	if !envVarName.MatchString(d.DSNEnv) {
		return fmt.Errorf("database.dsn_env must name an environment variable such as ARK_ORDERS_DB")
	}
	if !SQLIdent(d.Table, true) {
		return fmt.Errorf("database.table %q must be a plain name like orders or public.orders", d.Table)
	}
	if len(d.Columns) == 0 {
		return fmt.Errorf("database.columns needs at least one column")
	}
	for col, src := range d.Columns {
		if !SQLIdent(col, false) {
			return fmt.Errorf("database.columns: %q is not a plain column name", col)
		}
		if _, err := template.New(col).Funcs(templateStubs).Parse(src); err != nil {
			return fmt.Errorf("database.columns.%s: %w", col, err)
		}
	}
	for _, col := range d.UpsertOn {
		if _, ok := d.Columns[col]; !ok {
			return fmt.Errorf("database.upsert_on: %q is not one of the columns", col)
		}
	}
	return nil
}

type Flow struct {
	Start []string `yaml:"start"`
	Steps []Step   `yaml:"steps"`
}

// Branch is one way out of a condition step.
type Branch struct {
	Name string   `yaml:"name,omitempty"`
	When string   `yaml:"when"`
	Next []string `yaml:"next"`
}

type Step struct {
	ID   string   `yaml:"id"`
	Type StepType `yaml:"type"`
	Name string   `yaml:"name,omitempty"`
	App  string   `yaml:"app,omitempty"`
	Next []string `yaml:"next,omitempty"`

	Headers map[string]string `yaml:"headers,omitempty"`

	// call
	Target         *Target         `yaml:"target,omitempty"`
	Retry          *Retry          `yaml:"retry,omitempty"`
	CircuitBreaker *CircuitBreaker `yaml:"circuit_breaker,omitempty"`
	OnReject       []string        `yaml:"on_reject,omitempty"`
	// call and webhook
	OnFailure []string `yaml:"on_failure,omitempty"`

	// condition
	Branches  []Branch `yaml:"branches,omitempty"`
	Otherwise []string `yaml:"otherwise,omitempty"`
	Match     string   `yaml:"match,omitempty"`

	// data_check
	Rules  *DataRules `yaml:"rules,omitempty"`
	OnFail []string   `yaml:"on_fail,omitempty"`

	Topic string `yaml:"topic,omitempty"`
	// webhook
	URL          string `yaml:"url,omitempty"`
	Method       string `yaml:"method,omitempty"`
	Body         string `yaml:"body,omitempty"`
	Message      string `yaml:"message,omitempty"`
	MessageField string `yaml:"message_field,omitempty"`
	// topic: brokers of another Kafka cluster to send to instead of this one.
	Brokers []string `yaml:"brokers,omitempty"`
	// reject and dead_letter
	Reason string `yaml:"reason,omitempty"`
	// database
	Database *Database `yaml:"database,omitempty"`
}

// Outputs lists every step this one can lead to.
func (s Step) Outputs() []string {
	out := append([]string{}, s.Next...)
	out = append(out, s.OnReject...)
	out = append(out, s.OnFailure...)
	out = append(out, s.OnFail...)
	out = append(out, s.Otherwise...)
	for _, b := range s.Branches {
		out = append(out, b.Next...)
	}
	return out
}

// Step returns the step with the given id.
func (f *Flow) Step(id string) (Step, bool) {
	for _, s := range f.Steps {
		if s.ID == id {
			return s, true
		}
	}
	return Step{}, false
}

func applyFlowDefaults(p *Pipeline) {
	for i := range p.Flow.Steps {
		s := &p.Flow.Steps[i]
		if s.Type != StepCall {
			continue
		}
		if s.Target == nil {
			s.Target = &Target{}
		}
		t := s.Target
		if t.Mode == "" {
			t.Mode = TargetModeSingleURL
		}
		if t.Mode == TargetModeMultiURL && t.Strategy == "" {
			t.Strategy = StrategyRoundRobin
		}
		if (t.HealthCheckURL != "" || len(t.HealthCheckURLs) > 0) && t.HealthCheckSecs == 0 {
			t.HealthCheckSecs = 10
		}
		if t.TimeoutMs == 0 {
			t.TimeoutMs = p.Target.TimeoutMs
		}
		if s.Retry == nil {
			r := p.Retry
			s.Retry = &r
		}
		if s.CircuitBreaker == nil {
			cb := p.CircuitBreaker
			s.CircuitBreaker = &cb
		}
	}
}

var stepIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

func validateFlow(p Pipeline) error {
	f := p.Flow
	fail := func(format string, a ...any) error {
		return fmt.Errorf("pipeline %q: flow: %s", p.Name, fmt.Sprintf(format, a...))
	}
	if len(f.Steps) == 0 {
		return fail("needs at least one step")
	}
	if len(f.Start) == 0 {
		return fail("start must name the first step (or steps)")
	}
	byID := map[string]Step{}
	for i, s := range f.Steps {
		if !stepIDPattern.MatchString(s.ID) {
			return fail("steps[%d]: id %q must be letters, digits, dot, dash or underscore", i, s.ID)
		}
		if _, dup := byID[s.ID]; dup {
			return fail("duplicate step id %q", s.ID)
		}
		byID[s.ID] = s
	}
	for _, id := range f.Start {
		if _, ok := byID[id]; !ok {
			return fail("start names unknown step %q", id)
		}
	}
	for _, s := range f.Steps {
		for _, next := range s.Outputs() {
			if _, ok := byID[next]; !ok {
				return fail("step %q leads to unknown step %q", s.ID, next)
			}
		}
		if err := validateStep(p, s); err != nil {
			return fail("step %q: %v", s.ID, err)
		}
	}
	if cycle := findCycle(f, byID); cycle != "" {
		return fail("steps loop back on themselves (%s); send the message to a topic that feeds this or another pipeline instead", cycle)
	}
	return nil
}

func validateStep(p Pipeline, s Step) error {
	for name := range s.Headers {
		if name == "" {
			return fmt.Errorf("headers: a header name is empty")
		}
	}
	terminal := func() error {
		if len(s.Outputs()) > 0 {
			return fmt.Errorf("a %s step ends the path and can't lead anywhere", s.Type)
		}
		return nil
	}
	switch s.Type {
	case StepCall:
		t := s.Target
		switch t.Mode {
		case TargetModeSingleURL:
			if t.URL == "" {
				return fmt.Errorf("target.url is required")
			}
		case TargetModeMultiURL:
			if len(t.URLs) == 0 {
				return fmt.Errorf("target.urls is required for multi_url")
			}
			switch t.Strategy {
			case StrategyRoundRobin, StrategyLeastInFlight, StrategyStickyPartition:
			default:
				return fmt.Errorf("unknown target.strategy %q", t.Strategy)
			}
		default:
			return fmt.Errorf("unknown target.mode %q", t.Mode)
		}
		if t.TimeoutMs < 1 {
			return fmt.Errorf("target.timeout_ms must be positive")
		}
		if err := validateBatch(*t); err != nil {
			return err
		}
		if s.Retry.MaxAttempts < 1 {
			return fmt.Errorf("retry.max_attempts must be at least 1")
		}
		if s.CircuitBreaker.FailureThreshold < 1 || s.CircuitBreaker.CooldownSeconds < 1 {
			return fmt.Errorf("circuit_breaker.failure_threshold and cooldown_seconds must be at least 1")
		}
		if len(s.OnFailure) == 0 && p.DeadLetterTopic == "" && p.OnExhausted != OnExhaustedBlock {
			return fmt.Errorf("needs on_failure, or a pipeline dead_letter_topic to fall back to")
		}
		if len(s.OnReject) == 0 && p.RejectTopic == "" && p.DeadLetterTopic == "" {
			return fmt.Errorf("needs on_reject, or a pipeline reject_topic or dead_letter_topic to fall back to")
		}
	case StepCondition:
		if len(s.Branches) == 0 {
			return fmt.Errorf("needs at least one branch")
		}
		for i, b := range s.Branches {
			if b.When == "" {
				return fmt.Errorf("branches[%d]: when is required", i)
			}
		}
		switch s.Match {
		case "", "first", "all":
		default:
			return fmt.Errorf("match must be first or all, got %q", s.Match)
		}
	case StepCheck:
		if s.Rules == nil {
			return fmt.Errorf("rules are required")
		}
		if err := validateDataRules(*s.Rules); err != nil {
			return fmt.Errorf("rules: %w", err)
		}
		if len(s.OnFail) == 0 && s.Rules.OnViolation != "tag" && p.RejectTopic == "" && p.DeadLetterTopic == "" {
			return fmt.Errorf("needs on_fail, or a pipeline reject_topic or dead_letter_topic to fall back to")
		}
	case StepTopic:
		if s.Topic == "" {
			return fmt.Errorf("topic is required")
		}
	case StepWebhook:
		if s.URL == "" {
			return fmt.Errorf("url is required")
		}
		if strings.Contains(s.URL, "{{") {
			if _, ok := URLTemplateHost(s.URL); !ok {
				return fmt.Errorf("url: a template may only fill in the path or query, after http(s)://host/")
			}
			if _, err := template.New(s.ID).Funcs(templateStubs).Parse(s.URL); err != nil {
				return fmt.Errorf("url: %w", err)
			}
		}
		switch s.Method {
		case "", "POST", "PUT", "PATCH":
		default:
			return fmt.Errorf("method must be POST, PUT or PATCH, got %q", s.Method)
		}
		if s.Body != "" && s.Message != "" {
			return fmt.Errorf("set body or message, not both")
		}
		for field, src := range map[string]string{"body": s.Body, "message": s.Message} {
			if _, err := template.New(s.ID).Funcs(templateStubs).Parse(src); err != nil {
				return fmt.Errorf("%s: %w", field, err)
			}
		}
		if len(s.OnFailure) == 0 && p.DeadLetterTopic == "" {
			return fmt.Errorf("needs on_failure, or a pipeline dead_letter_topic to fall back to")
		}
	case StepDatabase:
		if err := validateDatabase(s.Database); err != nil {
			return err
		}
		if len(s.OnFailure) == 0 && p.DeadLetterTopic == "" {
			return fmt.Errorf("needs on_failure, or a pipeline dead_letter_topic to fall back to")
		}
	case StepReject:
		if s.Topic == "" && p.RejectTopic == "" && p.DeadLetterTopic == "" {
			return fmt.Errorf("topic is required when the pipeline has no reject_topic or dead_letter_topic")
		}
	case StepDeadLetter:
		if s.Topic == "" && p.DeadLetterTopic == "" {
			return fmt.Errorf("topic is required when the pipeline has no dead_letter_topic")
		}
	case StepDrop:
		return terminal()
	default:
		return fmt.Errorf("unknown type %q (call, condition, data_check, topic, webhook, database, reject, dead_letter, drop)", s.Type)
	}
	return nil
}

// findCycle returns a description of the first loop it finds, or "".
func findCycle(f *Flow, byID map[string]Step) string {
	const (
		unseen = iota
		open
		done
	)
	state := map[string]int{}
	var path []string
	var visit func(id string) string
	visit = func(id string) string {
		switch state[id] {
		case open:
			return fmt.Sprintf("%v -> %s", path, id)
		case done:
			return ""
		}
		state[id] = open
		path = append(path, id)
		for _, next := range byID[id].Outputs() {
			if c := visit(next); c != "" {
				return c
			}
		}
		path = path[:len(path)-1]
		state[id] = done
		return ""
	}
	for _, s := range f.Steps {
		if c := visit(s.ID); c != "" {
			return c
		}
	}
	return ""
}

func validateFlowPipeline(p Pipeline) error {
	if p.ConsumerGroup == "" {
		return fmt.Errorf("pipeline %q: consumer_group is required to enable this pipeline", p.Name)
	}
	switch p.OnExhausted {
	case "", OnExhaustedBlock:
	default:
		return fmt.Errorf("pipeline %q: unknown on_exhausted %q (the only option is block)", p.Name, p.OnExhausted)
	}
	switch p.Ordering {
	case OrderingPerKey, OrderingPerPartition, OrderingNone:
	default:
		return fmt.Errorf("pipeline %q: unknown ordering %q (want per_key, per_partition or none)", p.Name, p.Ordering)
	}
	if rd := p.DeadLetterRedrive; rd != (Redrive{}) {
		if p.DeadLetterTopic == "" {
			return fmt.Errorf("pipeline %q: dead_letter_redrive needs a dead_letter_topic", p.Name)
		}
		if rd.AfterSeconds < 1 || rd.MaxTimes < 1 {
			return fmt.Errorf("pipeline %q: dead_letter_redrive needs after_seconds and max_times of at least 1", p.Name)
		}
	}
	if len(p.FastPathRules) > 0 || len(p.PostCallbackRules) > 0 {
		return fmt.Errorf("pipeline %q: a flow pipeline puts its rules in condition steps, not fast_path_rules or post_callback_rules", p.Name)
	}
	return validateFlow(p)
}
