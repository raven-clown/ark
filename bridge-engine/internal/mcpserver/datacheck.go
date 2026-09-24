package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/mail"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/datarules"
	"github.com/raven-clown/ark/bridge-engine/internal/kafkaadmin"
	"github.com/raven-clown/ark/bridge-engine/internal/rules"
)

// check_data reads real recent messages and looks for what's odd about
// them: bad JSON, missing or mixed-type fields, values that break the
// usual format, outliers, missing keys, and data rule violations. It then
// proposes data_rules that match what the data actually looks like.

type fieldProfile struct {
	Path       string         `json:"path"`
	PresentPct float64        `json:"present_pct"`
	Types      map[string]int `json:"types"`
	Examples   []string       `json:"examples,omitempty"`
	Min        *float64       `json:"min,omitempty"`
	Max        *float64       `json:"max,omitempty"`
	Values     []string       `json:"distinct_values,omitempty"`
	Format     string         `json:"detected_format,omitempty"`

	nums     []float64
	distinct map[string]int
	formats  map[string]int
	strings  int
	oddities []string
}

type checkDataOut struct {
	Pipeline        string         `json:"pipeline"`
	Topic           string         `json:"topic"`
	Sampled         int            `json:"sampled"`
	InvalidJSON     int            `json:"invalid_json"`
	NotObject       int            `json:"not_an_object"`
	MissingKey      int            `json:"missing_key"`
	HeadersSeen     map[string]int `json:"headers_seen,omitempty"`
	Fields          []fieldProfile `json:"fields"`
	Anomalies       []Finding      `json:"anomalies"`
	RuleViolations  map[string]int `json:"data_rule_violations,omitempty"`
	ViolationSample []string       `json:"violation_examples,omitempty"`
	SuggestedRules  string         `json:"suggested_data_rules_yaml,omitempty"`
}

var detectors = []struct {
	name string
	ok   func(string) bool
}{
	{"uuid", regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`).MatchString},
	{"date-time", func(s string) bool { _, err := time.Parse(time.RFC3339, s); return err == nil }},
	{"date", func(s string) bool { _, err := time.Parse("2006-01-02", s); return err == nil }},
	{"email", func(s string) bool { a, err := mail.ParseAddress(s); return err == nil && a.Address == s }},
	{"url", regexp.MustCompile(`^https?://\S+$`).MatchString},
}

// sampleTopic reads up to n recent messages from topic, spread across its
// partitions, without joining any consumer group.
func sampleTopic(ctx context.Context, brokers []string, topic string, n int) ([]kafka.Message, error) {
	conn, err := kafkaadmin.DialAny(ctx, brokers)
	if err != nil {
		return nil, err
	}
	parts, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, nil
	}
	per := max(1, n/len(parts))
	var out []kafka.Message
	for _, p := range parts {
		lc, err := kafkaadmin.DialLeaderAny(ctx, brokers, topic, p.ID)
		if err != nil {
			continue
		}
		first, last, err := lc.ReadOffsets()
		_ = lc.Close()
		if err != nil || last <= first {
			continue
		}
		start := max(first, last-int64(per))
		r := kafka.NewReader(kafka.ReaderConfig{Brokers: brokers, Topic: topic, Partition: p.ID, MaxWait: 300 * time.Millisecond})
		if err := r.SetOffset(start); err == nil {
			for off := start; off < last; off++ {
				fctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				msg, err := r.FetchMessage(fctx)
				cancel()
				if err != nil {
					break
				}
				out = append(out, msg)
				if msg.Offset >= last-1 {
					break
				}
			}
		}
		_ = r.Close()
	}
	return out, nil
}

func flatten(prefix string, v any, into map[string]any) {
	m, ok := v.(map[string]any)
	if !ok {
		into[prefix] = v
		return
	}
	if prefix != "" {
		into[prefix] = v
	}
	for k, vv := range m {
		p := k
		if prefix != "" {
			p = prefix + "." + k
		}
		flatten(p, vv, into)
	}
}

func jsonType(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number:
		if !strings.ContainsAny(t.String(), ".eE") {
			return "integer"
		}
		return "number"
	case string:
		return "string"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "unknown"
}

func checkData(ctx context.Context, d Deps, p config.Pipeline, n int, from string) (checkDataOut, error) {
	topic := p.SourceTopic
	switch from {
	case "dlq":
		topic = p.DeadLetterTopic
	case "reject":
		topic = p.RejectTopic
	}
	out := checkDataOut{Pipeline: p.Name, Topic: topic, HeadersSeen: map[string]int{}, RuleViolations: map[string]int{}}
	if topic == "" {
		return out, fmt.Errorf("pipeline %s has no %s topic", p.Name, from)
	}
	if n <= 0 || n > 1000 {
		n = 100
	}
	msgs, err := sampleTopic(ctx, d.Brokers, topic, n)
	if err != nil {
		return out, err
	}
	out.Sampled = len(msgs)
	if len(msgs) == 0 {
		out.Anomalies = append(out.Anomalies, Finding{Severity: SeverityInfo, What: "No messages to look at yet in " + topic + "."})
		return out, nil
	}

	checker, _ := datarules.New(p.DataRules)
	profiles := map[string]*fieldProfile{}
	objects := 0
	for _, m := range msgs {
		if len(m.Key) == 0 {
			out.MissingKey++
		}
		hdrs := map[string]string{}
		for _, h := range m.Headers {
			out.HeadersSeen[h.Key]++
			hdrs[h.Key] = string(h.Value)
		}
		if checker != nil {
			if vs := checker.Check(m.Key, hdrs, m.Value); len(vs) > 0 {
				for _, v := range vs {
					out.RuleViolations[v.Rule]++
				}
				if len(out.ViolationSample) < 5 {
					out.ViolationSample = append(out.ViolationSample, fmt.Sprintf("offset %d/%d: %s", m.Partition, m.Offset, datarules.Summary(vs)))
				}
			}
		}

		var doc any
		dec := json.NewDecoder(bytes.NewReader(m.Value))
		dec.UseNumber()
		if err := dec.Decode(&doc); err != nil {
			out.InvalidJSON++
			continue
		}
		if _, ok := doc.(map[string]any); !ok {
			out.NotObject++
			continue
		}
		objects++
		flat := map[string]any{}
		flatten("", doc, flat)
		for path, v := range flat {
			fp := profiles[path]
			if fp == nil {
				fp = &fieldProfile{Path: path, Types: map[string]int{}, distinct: map[string]int{}, formats: map[string]int{}}
				profiles[path] = fp
			}
			t := jsonType(v)
			fp.Types[t]++
			rendered := ""
			switch x := v.(type) {
			case json.Number:
				f, _ := x.Float64()
				fp.nums = append(fp.nums, f)
				rendered = x.String()
			case string:
				fp.strings++
				rendered = x
				for _, det := range detectors {
					if det.ok(x) {
						fp.formats[det.name]++
						break
					}
				}
			case bool:
				rendered = fmt.Sprint(x)
			case nil:
				rendered = "null"
			}
			if rendered != "" {
				fp.distinct[rendered]++
				if len(fp.Examples) < 3 && !contains(fp.Examples, rendered) {
					fp.Examples = append(fp.Examples, short(rendered, 40))
				}
			}
		}
	}

	add := func(sev, what, why string, actions ...string) {
		out.Anomalies = append(out.Anomalies, Finding{Severity: sev, What: what, Why: why, Actions: actions})
	}
	if out.InvalidJSON > 0 {
		add(SeverityWarning, fmt.Sprintf("%d of %d messages are not valid JSON.", out.InvalidJSON, out.Sampled), "The producer is sending something else (plain text, a truncated payload, another encoding).",
			"Find the producer by the message key/headers; a data_rules entry with any field makes ARK reject non-JSON messages up front.")
	}
	if out.NotObject > 0 {
		add(SeverityWarning, fmt.Sprintf("%d messages are JSON but not objects (arrays or bare values).", out.NotObject), "Rules and field checks expect an object at the top level.")
	}
	if out.MissingKey > 0 && out.MissingKey < out.Sampled {
		add(SeverityInfo, fmt.Sprintf("%d of %d messages have no key.", out.MissingKey, out.Sampled), "Without a key, per-key ordering can't keep related messages in order.")
	}

	for _, fp := range profiles {
		total := 0
		for _, c := range fp.Types {
			total += c
		}
		fp.PresentPct = round1(100 * float64(total) / float64(max(objects, 1)))
		kinds := map[string]int{}
		for t, c := range fp.Types {
			if t == "integer" {
				t = "number"
			}
			kinds[t] += c
		}
		if len(kinds) > 1 {
			fp.oddities = append(fp.oddities, fmt.Sprintf("mixed types %v", kinds))
			add(SeverityWarning, fmt.Sprintf("Field %s has mixed types: %v.", fp.Path, kinds), "Some producer sends it differently (e.g. a number as a string, or null).",
				"Pin it with a data rule: {path: "+fp.Path+", type: <expected>}.")
		}
		if fp.PresentPct < 100 && fp.PresentPct >= 50 {
			add(SeverityInfo, fmt.Sprintf("Field %s is missing in %.0f%% of messages.", fp.Path, 100-fp.PresentPct), "Either it's optional, or some producer forgets it.")
		}
		if len(fp.nums) > 0 {
			sort.Float64s(fp.nums)
			lo, hi := fp.nums[0], fp.nums[len(fp.nums)-1]
			fp.Min, fp.Max = &lo, &hi
			if len(fp.nums) >= 10 {
				q1, q3 := fp.nums[len(fp.nums)/4], fp.nums[3*len(fp.nums)/4]
				iqr := q3 - q1
				if iqr > 0 {
					var outliers []string
					for _, x := range fp.nums {
						if x < q1-3*iqr || x > q3+3*iqr {
							outliers = append(outliers, strconv.FormatFloat(x, 'f', -1, 64))
						}
					}
					if len(outliers) > 0 {
						add(SeverityWarning, fmt.Sprintf("Field %s has %d unusual value(s) far from the rest: %s (typical range %s to %s).", fp.Path, len(outliers), strings.Join(firstN(outliers, 5), ", "), strconv.FormatFloat(q1, 'f', -1, 64), strconv.FormatFloat(q3, 'f', -1, 64)),
							"Could be a unit mix-up, a test message, or a real edge case.", "Set min/max on it in data_rules if these should never happen.")
					}
				}
			}
			if lo < 0 && fp.nums[len(fp.nums)/2] > 0 {
				add(SeverityInfo, fmt.Sprintf("Field %s is usually positive but has negative values (min %v).", fp.Path, lo), "")
			}
		}
		if fp.strings >= 5 {
			for name, c := range fp.formats {
				pct := float64(c) / float64(fp.strings)
				if pct >= 0.8 {
					fp.Format = name
					if pct < 1 {
						add(SeverityWarning, fmt.Sprintf("Field %s is a %s in %.0f%% of messages, but not in the rest.", fp.Path, name, pct*100), "Those are probably malformed.",
							"Add {path: "+fp.Path+", format: "+name+"} to data_rules to catch them.")
					}
				}
			}
		}
		if len(fp.distinct) <= 8 && total >= 20 {
			var rare []string
			for v, c := range fp.distinct {
				if c >= 2 {
					fp.Values = append(fp.Values, v)
				} else {
					rare = append(rare, v)
				}
			}
			sort.Strings(fp.Values)
			if len(rare) > 0 && len(fp.Values) > 0 {
				sort.Strings(rare)
				add(SeverityWarning, fmt.Sprintf("Field %s is almost always one of %v, but %v appeared once.", fp.Path, fp.Values, rare), "A one-off value in a field with few fixed values is often a typo or an unsupported case.",
					"Add enum to data_rules to catch it.")
			}
		}
		out.Fields = append(out.Fields, *fp)
	}
	sort.Slice(out.Fields, func(i, j int) bool { return out.Fields[i].Path < out.Fields[j].Path })

	if len(out.RuleViolations) > 0 {
		kinds := make([]string, 0, len(out.RuleViolations))
		for k := range out.RuleViolations {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		parts := make([]string, len(kinds))
		for i, k := range kinds {
			parts[i] = fmt.Sprintf("%s (%d)", k, out.RuleViolations[k])
		}
		add(SeverityWarning, "Messages in this sample break the pipeline's data rules: "+strings.Join(parts, ", ")+".", "See violation_examples.")
	}
	if len(out.Anomalies) == 0 {
		add(SeverityOK, fmt.Sprintf("Nothing unusual in %d recent messages.", out.Sampled), "")
	}
	out.SuggestedRules = suggestRules(out.Fields, objects)
	return out, nil
}

// suggestRules drafts data_rules from what the sample looks like, starting
// with on_violation: tag so an operator can watch before rejecting.
func suggestRules(fields []fieldProfile, objects int) string {
	if objects == 0 {
		return ""
	}
	dr := config.DataRules{OnViolation: "tag"}
	for _, fp := range fields {
		if strings.Contains(fp.Path, ".") && fp.Types["object"] > 0 {
			continue
		}
		counts := map[string]int{}
		for t, c := range fp.Types {
			if t == "integer" && fp.Types["number"] > 0 {
				t = "number"
			}
			counts[t] += c
		}
		typ, best := "", 0
		for t, c := range counts {
			if c > best && t != "null" {
				typ, best = t, c
			}
		}
		if typ == "object" || typ == "array" {
			continue
		}
		r := config.FieldRule{Path: fp.Path, Type: typ, Required: fp.PresentPct == 100}
		if typ == "integer" {
			r.Type = "integer"
		}
		if fp.Format != "" {
			r.Format = fp.Format
		}
		if len(fp.Values) > 0 && typ == "string" {
			r.Enum = fp.Values
		}
		if fp.Min != nil && *fp.Min >= 0 && (typ == "number" || typ == "integer") {
			zero := 0.0
			r.Min = &zero
		}
		dr.Fields = append(dr.Fields, r)
	}
	b, _ := yaml.Marshal(map[string]any{"data_rules": dr})
	return string(b)
}

type testMessageIn struct {
	Name    string            `json:"name" jsonschema:"the pipeline name"`
	Payload string            `json:"payload" jsonschema:"the message body exactly as it would be produced"`
	Key     string            `json:"key,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type testMessageOut struct {
	Outcome    string                `json:"outcome"`
	Violations []datarules.Violation `json:"data_rule_violations,omitempty"`
	MatchedBy  string                `json:"matched_rule,omitempty"`
	Explained  string                `json:"explanation"`
}

// testMessage predicts what the pipeline would do with a message, without
// sending it anywhere.
func testMessage(p config.Pipeline, in testMessageIn) (testMessageOut, error) {
	var out testMessageOut
	checker, err := datarules.New(p.DataRules)
	if err != nil {
		return out, err
	}
	if checker != nil {
		out.Violations = checker.Check([]byte(in.Key), in.Headers, []byte(in.Payload))
		if len(out.Violations) > 0 {
			switch checker.OnViolation() {
			case "tag":
				out.Explained = "It breaks data rules but on_violation is tag, so it continues with an X-Ark-Violations header. "
			case "dead_letter":
				out.Outcome = "dead_lettered"
				out.Explained = "It breaks data rules, so it goes to " + p.DeadLetterTopic + " with that as the reason; the target is never called."
				return out, nil
			default:
				out.Outcome = "rejected"
				dest := p.RejectTopic
				if dest == "" {
					dest = p.DeadLetterTopic + " (no reject_topic)"
				}
				out.Explained = "It breaks data rules, so it's rejected to " + dest + " with that as the reason; the target is never called."
				return out, nil
			}
		}
	}
	engine, err := rules.Compile(p)
	if err != nil {
		return out, err
	}
	if engine.HasFastPath() {
		rule, err := engine.EvaluateFastPath([]byte(in.Payload))
		if err != nil {
			out.Explained += "A fast_path_rule failed to evaluate on it (" + err.Error() + "), so it falls through to the callback. "
		} else if rule != nil {
			out.MatchedBy = rule.Name
			out.Outcome = string(rule.Action)
			out.Explained += fmt.Sprintf("fast_path_rule %q matches, so action %s happens without calling the target.", rule.Name, rule.Action)
			return out, nil
		}
	}
	out.Outcome = "callback"
	out.Explained += fmt.Sprintf("It would be POSTed to %s; what happens next depends on the response (2xx: produced to %s, reject statuses: rejected, failures: retried up to %d times then dead-lettered).",
		targetDescription(p), p.DestinationTopic, p.Retry.MaxAttempts)
	return out, nil
}

func round1(x float64) float64 { return float64(int(x*10+0.5)) / 10 }

func firstN(s []string, n int) []string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func short(s string, n int) string {
	if len([]rune(s)) > n {
		return string([]rune(s)[:n]) + "..."
	}
	return s
}
