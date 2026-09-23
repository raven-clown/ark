package rules

import (
	"encoding/json"
	"fmt"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

type compiledRule struct {
	config.FastPathRule
	program *vm.Program
}

type Engine struct {
	fastPath     []compiledRule
	postCallback []compiledRule
}

func Compile(p config.Pipeline) (*Engine, error) {
	fastPath, err := compileRules(p.FastPathRules, []string{"data"})
	if err != nil {
		return nil, fmt.Errorf("compiling fast_path_rules: %w", err)
	}

	postCallback, err := compileRules(p.PostCallbackRules, []string{"data", "response"})
	if err != nil {
		return nil, fmt.Errorf("compiling post_callback_rules: %w", err)
	}

	return &Engine{fastPath: fastPath, postCallback: postCallback}, nil
}

func compileRules(rules []config.FastPathRule, envKeys []string) ([]compiledRule, error) {
	env := make(map[string]any, len(envKeys))
	for _, k := range envKeys {
		env[k] = map[string]any{}
	}

	compiled := make([]compiledRule, 0, len(rules))
	for _, r := range rules {
		program, err := expr.Compile(r.Condition, expr.Env(env), expr.AsBool())
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", r.Name, err)
		}
		compiled = append(compiled, compiledRule{FastPathRule: r, program: program})
	}
	return compiled, nil
}

func (e *Engine) HasFastPath() bool {
	return len(e.fastPath) > 0
}

func (e *Engine) HasPostCallback() bool {
	return len(e.postCallback) > 0
}

func (e *Engine) EvaluateFastPath(value []byte) (*config.FastPathRule, error) {
	data, ok := parseJSON(value)
	if !ok {
		return nil, nil
	}
	return evaluate(e.fastPath, map[string]any{"data": data})
}

func (e *Engine) EvaluatePostCallback(value []byte, status int, responseBody []byte) (*config.FastPathRule, error) {
	data, ok := parseJSON(value)
	if !ok {
		data = map[string]any{}
	}
	respBody, _ := parseJSON(responseBody)

	env := map[string]any{
		"data": data,
		"response": map[string]any{
			"status": status,
			"body":   respBody,
		},
	}
	return evaluate(e.postCallback, env)
}

func evaluate(rules []compiledRule, env map[string]any) (*config.FastPathRule, error) {
	for i := range rules {
		result, err := expr.Run(rules[i].program, env)
		if err != nil {
			return nil, fmt.Errorf("rule %q: %w", rules[i].Name, err)
		}
		if matched, ok := result.(bool); ok && matched {
			rule := rules[i].FastPathRule
			return &rule, nil
		}
	}
	return nil, nil
}

func parseJSON(value []byte) (map[string]any, bool) {
	if len(value) == 0 {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(value, &m); err != nil {
		return nil, false
	}
	return m, true
}
