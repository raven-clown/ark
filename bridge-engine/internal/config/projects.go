package config

import (
	"fmt"
	"regexp"
)

type AIAccess string

const (
	AIAccessNone      AIAccess = "none"
	AIAccessReadOnly  AIAccess = "read_only"
	AIAccessOperate   AIAccess = "operate"   // plus pause, resume, retry, discard
	AIAccessConfigure AIAccess = "configure" // plus create and change pipelines
)

var aiRank = map[AIAccess]int{AIAccessNone: 0, AIAccessReadOnly: 1, AIAccessOperate: 2, AIAccessConfigure: 3}

// Min returns the lower of two access levels.
func (a AIAccess) Min(b AIAccess) AIAccess {
	if aiRank[a] <= aiRank[b] {
		return a
	}
	return b
}

type Project struct {
	Name         string          `yaml:"name" json:"name"`
	Description  string          `yaml:"description,omitempty" json:"description,omitempty"`
	AIAccess     AIAccess        `yaml:"ai_access,omitempty" json:"ai_access,omitempty"`
	MCPEndpoints []MCPEndpoint   `yaml:"mcp_endpoints,omitempty" json:"mcp_endpoints,omitempty"`
	Assistant    *AssistantModel `yaml:"assistant,omitempty" json:"assistant,omitempty"`
}

type MCPEndpoint struct {
	Name      string   `yaml:"name" json:"name"`
	Access    AIAccess `yaml:"access" json:"access"`
	TokensEnv string   `yaml:"tokens_env" json:"tokens_env"`
	Tools     []string `yaml:"tools,omitempty" json:"tools,omitempty"`
}

type AssistantModel struct {
	Provider  string `yaml:"provider" json:"provider"`
	Model     string `yaml:"model" json:"model"`
	BaseURL   string `yaml:"base_url,omitempty" json:"base_url,omitempty"`
	APIKeyEnv string `yaml:"api_key_env,omitempty" json:"api_key_env,omitempty"`
	// MaxSteps caps tool calls per answer (default 8).
	MaxSteps int `yaml:"max_steps,omitempty" json:"max_steps,omitempty"`
}

var (
	projectName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)
	envName     = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)
	providers   = map[string]bool{"anthropic": true, "openai": true, "gemini": true, "openai_compatible": true}
)

// ApplyProjectDefaults fills in what a project leaves out.
func ApplyProjectDefaults(p *Project) {
	if p.AIAccess == "" {
		p.AIAccess = AIAccessReadOnly
	}
	for i := range p.MCPEndpoints {
		if p.MCPEndpoints[i].Access == "" {
			p.MCPEndpoints[i].Access = AIAccessReadOnly
		}
	}
	if p.Assistant != nil && p.Assistant.MaxSteps == 0 {
		p.Assistant.MaxSteps = 8
	}
}

// ValidateModel checks an assistant model setting.
func ValidateModel(where string, m *AssistantModel) error {
	if m == nil {
		return nil
	}
	if !providers[m.Provider] {
		return fmt.Errorf("%s: provider must be anthropic, openai, gemini or openai_compatible, got %q", where, m.Provider)
	}
	if m.Model == "" {
		return fmt.Errorf("%s: model is required", where)
	}
	if m.Provider == "openai_compatible" && m.BaseURL == "" {
		return fmt.Errorf("%s: base_url is required for openai_compatible", where)
	}
	if m.APIKeyEnv != "" && !envName.MatchString(m.APIKeyEnv) {
		return fmt.Errorf("%s: api_key_env %q must be an environment variable name like MY_API_KEY", where, m.APIKeyEnv)
	}
	if m.MaxSteps < 0 || m.MaxSteps > 50 {
		return fmt.Errorf("%s: max_steps must be between 1 and 50", where)
	}
	return nil
}

func ValidateProjects(projects []Project, pipelines []Pipeline) error {
	names := map[string]bool{}
	for i, p := range projects {
		if !projectName.MatchString(p.Name) {
			return fmt.Errorf("projects[%d]: name %q must be lowercase letters, digits, '-' or '_'", i, p.Name)
		}
		if names[p.Name] {
			return fmt.Errorf("projects[%d]: duplicate project name %q", i, p.Name)
		}
		names[p.Name] = true
		if _, ok := aiRank[p.AIAccess]; !ok {
			return fmt.Errorf("project %q: ai_access must be none, read_only, operate or configure, got %q", p.Name, p.AIAccess)
		}
		endpoints := map[string]bool{}
		for _, e := range p.MCPEndpoints {
			if !projectName.MatchString(e.Name) {
				return fmt.Errorf("project %q: endpoint name %q must be lowercase letters, digits, '-' or '_'", p.Name, e.Name)
			}
			if endpoints[e.Name] {
				return fmt.Errorf("project %q: duplicate endpoint %q", p.Name, e.Name)
			}
			endpoints[e.Name] = true
			if _, ok := aiRank[e.Access]; !ok || e.Access == AIAccessNone {
				return fmt.Errorf("project %q endpoint %q: access must be read_only, operate or configure, got %q", p.Name, e.Name, e.Access)
			}
			if !envName.MatchString(e.TokensEnv) {
				return fmt.Errorf("project %q endpoint %q: tokens_env must name an environment variable like ARK_MCP_%s_TOKENS", p.Name, e.Name, "PROJECT")
			}
		}
		if err := ValidateModel(fmt.Sprintf("project %q assistant", p.Name), p.Assistant); err != nil {
			return err
		}
	}
	for _, p := range pipelines {
		if p.Project != "" && !names[p.Project] {
			return fmt.Errorf("pipeline %q: project %q is not defined under projects", p.Name, p.Project)
		}
	}
	return nil
}
