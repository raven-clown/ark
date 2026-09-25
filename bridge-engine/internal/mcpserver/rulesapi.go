package mcpserver

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/rules"
)

// asJSON turns a config value into plain data keyed by its YAML names, so
// the console sees the same field names as the config file.
func asJSON(v any) any {
	b, err := yaml.Marshal(v)
	if err != nil {
		return nil
	}
	var out any
	_ = yaml.Unmarshal(b, &out)
	return out
}

type ruleTestIn struct {
	Condition string `json:"condition"`
	Stage     string `json:"stage"` // fast_path (default) or post_callback
	Sample    int    `json:"sample"`
}

type ruleTestOut struct {
	Sampled  int      `json:"sampled"`
	Matched  int      `json:"matched"`
	Examples []string `json:"examples"`
	Error    string   `json:"error,omitempty"`
	Note     string   `json:"note,omitempty"`
}

// testRule evaluates a condition against recent messages. Fast path
// conditions run on the source topic; post-callback conditions run on the
// destination topic, treating each result as a 200 response body.
func (c *Console) testRule(r *http.Request, p config.Pipeline, in ruleTestIn) ruleTestOut {
	out := ruleTestOut{Examples: []string{}}
	if strings.TrimSpace(in.Condition) == "" {
		out.Error = "condition is empty"
		return out
	}
	n := in.Sample
	if n <= 0 || n > 500 {
		n = 50
	}
	probe := config.Pipeline{Name: p.Name}
	rule := config.FastPathRule{Name: "preview", Condition: in.Condition, Action: config.ActionPassThrough}
	topic := p.SourceTopic
	if in.Stage == "post_callback" {
		probe.PostCallbackRules = []config.FastPathRule{rule}
		topic = p.DestinationTopic
		out.Note = "Checked against recent results on " + topic + " as if the target had answered 200 with them."
	} else {
		probe.FastPathRules = []config.FastPathRule{rule}
	}
	engine, err := rules.Compile(probe)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	msgs, err := sampleTopic(r.Context(), c.d.Brokers, topic, n)
	if err != nil {
		out.Error = err.Error()
		return out
	}
	for _, m := range msgs {
		out.Sampled++
		var matched *config.FastPathRule
		if in.Stage == "post_callback" {
			matched, err = engine.EvaluatePostCallback(m.Value, 200, m.Value)
		} else {
			matched, err = engine.EvaluateFastPath(m.Value)
		}
		if err != nil {
			out.Error = err.Error()
			return out
		}
		if matched != nil {
			out.Matched++
			if len(out.Examples) < 5 {
				out.Examples = append(out.Examples, short(string(m.Value), 300))
			}
		}
	}
	return out
}

func (c *Console) rulesRoutes(mux *http.ServeMux) {
	d := c.d
	mux.HandleFunc("GET /api/v1/pipelines/{name}/rules", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		writeJSON(w, http.StatusOK, map[string]any{
			"fast_path_rules":     asJSON(p.FastPathRules),
			"post_callback_rules": asJSON(p.PostCallbackRules),
			"data_rules":          asJSON(p.DataRules),
		})
	}))

	mux.HandleFunc("POST /api/v1/pipelines/{name}/rules/test", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		var in ruleTestIn
		if !readJSON(w, r, &in) {
			return
		}
		writeJSON(w, http.StatusOK, c.testRule(r, p, in))
	}))

	// rules/preview replaces any of the three rule sets it is given and
	// returns the usual preview and confirm token; apply with
	// POST /api/v1/config/confirm.
	mux.HandleFunc("POST /api/v1/pipelines/{name}/rules/preview", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		var in map[string]json.RawMessage
		if !readJSON(w, r, &in) {
			return
		}
		// JSON is valid YAML, so decoding through yaml keeps the config's
		// own field names (destination_override, on_violation, ...).
		decode := func(key string, into any) error {
			raw, ok := in[key]
			if !ok {
				return nil
			}
			if err := yaml.Unmarshal(raw, into); err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			return nil
		}
		for key, into := range map[string]any{"fast_path_rules": &p.FastPathRules, "post_callback_rules": &p.PostCallbackRules, "data_rules": &p.DataRules} {
			if err := decode(key, into); err != nil {
				writeJSON(w, http.StatusBadRequest, errBody(err))
				return
			}
		}
		out, err := previewChange(r.Context(), d, c.cf, "console_apply", callerOf(r).ID, toYAML(p))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	}))
}
