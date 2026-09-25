package mcpserver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
)

const confirmTTL = 10 * time.Minute

type pendingChange struct {
	action   string
	pipeline config.Pipeline
	baseHash string
	userID   string
	expires  time.Time
}

// confirmations holds previewed config changes until the human behind the
// agent confirms them. A token only works for the caller that previewed
// it, only once, only within confirmTTL, and only if the pipeline hasn't
// changed in between.
type confirmations struct {
	mu sync.Mutex
	m  map[string]pendingChange
}

func newConfirmations() *confirmations { return &confirmations{m: make(map[string]pendingChange)} }

func (c *confirmations) put(pc pendingChange) string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	token := hex.EncodeToString(buf)
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, v := range c.m {
		if time.Now().After(v.expires) {
			delete(c.m, k)
		}
	}
	c.m[token] = pc
	return token
}

func (c *confirmations) take(token, userID string) (pendingChange, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pc, ok := c.m[token]
	if !ok {
		return pendingChange{}, fmt.Errorf("unknown or already used confirm_token; preview the change again")
	}
	delete(c.m, token)
	if time.Now().After(pc.expires) {
		return pendingChange{}, fmt.Errorf("confirm_token expired; preview the change again")
	}
	if pc.userID != userID {
		return pendingChange{}, fmt.Errorf("confirm_token was issued to a different caller")
	}
	return pc, nil
}

func callerID(req *mcp.CallToolRequest) string {
	if req != nil && req.Extra != nil && req.Extra.TokenInfo != nil {
		return req.Extra.TokenInfo.UserID
	}
	return ""
}

// parsePipelineYAML reads one pipeline from YAML, rejecting unknown or
// misspelled fields instead of silently ignoring them.
func parsePipelineYAML(src string) (config.Pipeline, error) {
	var p config.Pipeline
	dec := yaml.NewDecoder(strings.NewReader(src))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil {
		return p, fmt.Errorf("reading pipeline YAML: %w", err)
	}
	if p.Name == "" {
		return p, fmt.Errorf("pipeline YAML has no name")
	}
	config.ApplyPipelineDefaults(&p)
	return p, nil
}

func pipelineHash(p *config.Pipeline) string {
	if p == nil {
		return "absent"
	}
	b, _ := yaml.Marshal(p)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

func toYAML(p config.Pipeline) string {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	_ = enc.Encode(p)
	return buf.String()
}

// fieldDiff lists changed fields as "path: old -> new".
func fieldDiff(old, updated *config.Pipeline) []string {
	flat := func(p *config.Pipeline) map[string]string {
		out := map[string]string{}
		if p == nil {
			return out
		}
		var m map[string]any
		b, _ := yaml.Marshal(p)
		_ = yaml.Unmarshal(b, &m)
		var walk func(prefix string, v any)
		walk = func(prefix string, v any) {
			switch t := v.(type) {
			case map[string]any:
				for k, vv := range t {
					walk(strings.TrimPrefix(prefix+"."+k, "."), vv)
				}
			default:
				s, _ := yaml.Marshal(t)
				out[prefix] = strings.TrimSpace(string(s))
			}
		}
		walk("", m)
		return out
	}
	a, b := flat(old), flat(updated)
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	var out []string
	for k := range keys {
		if a[k] == b[k] {
			continue
		}
		from, to := a[k], b[k]
		if from == "" {
			from = "(unset)"
		}
		if to == "" {
			to = "(unset)"
		}
		out = append(out, fmt.Sprintf("%s: %s -> %s", k, from, to))
	}
	sort.Strings(out)
	return out
}

type validationOut struct {
	Valid          bool      `json:"valid"`
	Change         string    `json:"change"`
	Errors         []string  `json:"errors,omitempty"`
	Warnings       []Finding `json:"warnings,omitempty"`
	Diff           []string  `json:"diff,omitempty"`
	NormalizedYAML string    `json:"normalized_yaml,omitempty"`
	AppliesTo      string    `json:"applies_to"`
}

// validate checks a proposed pipeline against the whole current config and
// the live cluster (topics, partitions), without changing anything.
func validate(ctx context.Context, d Deps, src string) (config.Pipeline, *config.Pipeline, validationOut) {
	out := validationOut{AppliesTo: d.Config.Mode()}
	p, err := parsePipelineYAML(src)
	if err != nil {
		out.Errors = append(out.Errors, err.Error())
		return p, nil, out
	}

	current := d.Config.Pipelines()
	var existing *config.Pipeline
	merged := make([]config.Pipeline, 0, len(current)+1)
	for i := range current {
		if current[i].Name == p.Name {
			c := current[i]
			existing = &c
			merged = append(merged, p)
			continue
		}
		merged = append(merged, current[i])
	}
	if existing == nil {
		merged = append(merged, p)
		out.Change = "create"
	} else if reflect.DeepEqual(*existing, p) {
		out.Change = "unchanged"
	} else {
		out.Change = "update"
	}

	if err := config.ValidatePipelines(merged); err != nil {
		out.Errors = append(out.Errors, err.Error())
	}
	for _, q := range merged {
		if q.Name != p.Name && q.SourceTopic == p.SourceTopic && q.ConsumerGroup == p.ConsumerGroup {
			out.Errors = append(out.Errors, fmt.Sprintf("pipeline %q already consumes %s with consumer_group %s; sharing a group would split messages between the two pipelines", q.Name, p.SourceTopic, p.ConsumerGroup))
		}
	}

	if parts, err := kafkaadmin.PartitionCount(ctx, d.Brokers, p.SourceTopic); err == nil {
		switch {
		case parts == 0:
			out.Warnings = append(out.Warnings, Finding{Severity: SeverityInfo, What: fmt.Sprintf("source_topic %s doesn't exist yet; ARK will create it with %d partition(s).", p.SourceTopic, max(p.Workers, 1))})
		case p.Workers > parts:
			out.Warnings = append(out.Warnings, Finding{Severity: SeverityWarning, What: fmt.Sprintf("workers (%d) is more than %s's %d partitions; extra workers will idle.", p.Workers, p.SourceTopic, parts)})
		}
	}
	if p.MCPAccess == config.MCPAccessNone && !d.AllPipelines {
		out.Warnings = append(out.Warnings, Finding{Severity: SeverityWarning, What: "mcp_access: none hides this pipeline from AI agents entirely after it's applied, including from this conversation."})
	}
	out.Warnings = append(out.Warnings, reviewConfig(d, p)...)

	out.Valid = len(out.Errors) == 0
	out.Diff = fieldDiff(existing, &p)
	out.NormalizedYAML = toYAML(p)
	return p, existing, out
}

type yamlIn struct {
	YAML string `json:"yaml" jsonschema:"one pipeline as YAML, in the shape get_pipeline_schema describes"`
}

type applyIn struct {
	YAML         string `json:"yaml,omitempty" jsonschema:"one pipeline as YAML; required on the first (preview) call"`
	ConfirmToken string `json:"confirm_token,omitempty" jsonschema:"the token from the preview call; send it only after the user has explicitly confirmed the preview"`
}

type applyOut struct {
	State        string         `json:"state"`
	Preview      *validationOut `json:"preview,omitempty"`
	ConfirmToken string         `json:"confirm_token,omitempty"`
	NextStep     string         `json:"next_step,omitempty"`
}

func hiddenPipeline(d Deps, name string) (config.Pipeline, bool) {
	for _, p := range d.Config.Pipelines() {
		if p.Name == name && p.MCPAccess == config.MCPAccessNone {
			return p, true
		}
	}
	return config.Pipeline{}, false
}

type topicInfo struct {
	Name       string   `json:"name"`
	Partitions int      `json:"partitions"`
	UsedBy     []string `json:"used_by,omitempty"`
}

func listTopics(ctx context.Context, d Deps) ([]topicInfo, error) {
	conn, err := kafkaadmin.DialAny(ctx, d.Brokers)
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()
	parts, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, p := range parts {
		if strings.HasPrefix(p.Topic, "__") {
			continue
		}
		counts[p.Topic]++
	}
	usedBy := map[string][]string{}
	for _, p := range d.visiblePipelines() {
		for role, t := range map[string]string{"source": p.SourceTopic, "destination": p.DestinationTopic, "dead_letter": p.DeadLetterTopic, "reject": p.RejectTopic} {
			if t != "" {
				usedBy[t] = append(usedBy[t], p.Name+" ("+role+")")
			}
		}
	}
	out := make([]topicInfo, 0, len(counts))
	for name, n := range counts {
		u := usedBy[name]
		sort.Strings(u)
		out = append(out, topicInfo{Name: name, Partitions: n, UsedBy: u})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
