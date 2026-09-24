// Package datarules checks messages against a pipeline's data_rules: the
// shape of the JSON body, the key, the headers and the size.
package datarules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// Violation is one broken rule, phrased so an operator can act on it.
type Violation struct {
	Where   string `json:"where"`
	Rule    string `json:"rule"`
	Message string `json:"message"`
}

type Checker struct {
	rules     config.DataRules
	patterns  map[string]*regexp.Regexp
	known     map[string]bool
	strictTop bool
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// New compiles rules; it returns nil if no rules are configured.
func New(rules config.DataRules) (*Checker, error) {
	if !rules.Enabled() {
		return nil, nil
	}
	c := &Checker{rules: rules, patterns: map[string]*regexp.Regexp{}, known: map[string]bool{}}
	add := func(key, pattern string) error {
		if pattern == "" {
			return nil
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return err
		}
		c.patterns[key] = re
		return nil
	}
	if rules.Key != nil {
		if err := add("key", rules.Key.Pattern); err != nil {
			return nil, err
		}
	}
	for _, h := range rules.Headers {
		if err := add("header:"+h.Name, h.Pattern); err != nil {
			return nil, err
		}
	}
	for _, f := range rules.Fields {
		if err := add("field:"+f.Path, f.Pattern); err != nil {
			return nil, err
		}
		c.known[strings.SplitN(f.Path, ".", 2)[0]] = true
	}
	c.strictTop = rules.AllowUnknownFields != nil && !*rules.AllowUnknownFields
	return c, nil
}

func (c *Checker) OnViolation() string {
	if c.rules.OnViolation == "" {
		return "reject"
	}
	return c.rules.OnViolation
}

// Check returns every rule the message breaks, or nil if it's valid.
func (c *Checker) Check(key []byte, headers map[string]string, value []byte) []Violation {
	var out []Violation
	add := func(where, rule, msg string, args ...any) {
		out = append(out, Violation{Where: where, Rule: rule, Message: fmt.Sprintf(msg, args...)})
	}

	if c.rules.MaxBytes > 0 && len(value) > c.rules.MaxBytes {
		add("message", "max_bytes", "message is %d bytes, limit is %d", len(value), c.rules.MaxBytes)
	}
	if c.rules.Key != nil {
		c.checkValue(&out, "key", "key", string(key), len(key) > 0, *c.rules.Key)
	}
	for _, h := range c.rules.Headers {
		v, ok := headers[h.Name]
		c.checkValue(&out, "header "+h.Name, "header:"+h.Name, v, ok, h.ValueRule)
	}

	if len(c.rules.Fields) == 0 && !c.strictTop {
		return out
	}
	var doc any
	dec := json.NewDecoder(bytes.NewReader(value))
	dec.UseNumber()
	if err := dec.Decode(&doc); err != nil {
		add("message", "json", "message body is not valid JSON: %v", err)
		return out
	}
	obj, isObj := doc.(map[string]any)
	if !isObj {
		add("message", "json", "message body is JSON but not an object (got %s)", typeOf(doc))
		return out
	}
	if c.strictTop {
		for k := range obj {
			if !c.known[k] {
				add("field "+k, "allow_unknown_fields", "unexpected field %q (no data rule mentions it)", k)
			}
		}
	}
	for _, f := range c.rules.Fields {
		c.checkField(&out, obj, f)
	}
	return out
}

func (c *Checker) checkValue(out *[]Violation, where, patKey, v string, present bool, r config.ValueRule) {
	if !present || v == "" {
		if r.Required {
			*out = append(*out, Violation{Where: where, Rule: "required", Message: where + " is missing"})
		}
		return
	}
	if r.MaxLength > 0 && len([]rune(v)) > r.MaxLength {
		*out = append(*out, Violation{Where: where, Rule: "max_length", Message: fmt.Sprintf("%s is %d characters, limit is %d", where, len([]rune(v)), r.MaxLength)})
	}
	if re := c.patterns[patKey]; re != nil && !re.MatchString(v) {
		*out = append(*out, Violation{Where: where, Rule: "pattern", Message: fmt.Sprintf("%s %q doesn't match %s", where, short(v), re.String())})
	}
	if len(r.Enum) > 0 && !contains(r.Enum, v) {
		*out = append(*out, Violation{Where: where, Rule: "enum", Message: fmt.Sprintf("%s %q is not one of %v", where, short(v), r.Enum)})
	}
}

func lookup(obj map[string]any, path string) (any, bool) {
	var cur any = obj
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

func (c *Checker) checkField(out *[]Violation, obj map[string]any, f config.FieldRule) {
	where := "field " + f.Path
	add := func(rule, msg string, args ...any) {
		*out = append(*out, Violation{Where: where, Rule: rule, Message: fmt.Sprintf(msg, args...)})
	}
	v, ok := lookup(obj, f.Path)
	if !ok {
		if f.Required {
			add("required", "%s is missing", f.Path)
		}
		return
	}
	actual := typeOf(v)
	if f.Type != "" && !typeMatches(f.Type, v) {
		add("type", "%s should be %s but is %s (%s)", f.Path, f.Type, actual, short(render(v)))
		return
	}
	if n, isNum := v.(json.Number); isNum {
		x, _ := n.Float64()
		if f.Min != nil && x < *f.Min {
			add("min", "%s is %v, below the minimum %v", f.Path, x, *f.Min)
		}
		if f.Max != nil && x > *f.Max {
			add("max", "%s is %v, above the maximum %v", f.Path, x, *f.Max)
		}
		if math.IsNaN(x) || math.IsInf(x, 0) {
			add("type", "%s is not a finite number", f.Path)
		}
	}
	s, isStr := v.(string)
	if isStr {
		l := len([]rune(s))
		if f.MinLength != nil && l < *f.MinLength {
			add("min_length", "%s has %d characters, minimum is %d", f.Path, l, *f.MinLength)
		}
		if f.MaxLength != nil && l > *f.MaxLength {
			add("max_length", "%s has %d characters, maximum is %d", f.Path, l, *f.MaxLength)
		}
		if re := c.patterns["field:"+f.Path]; re != nil && !re.MatchString(s) {
			add("pattern", "%s %q doesn't match %s", f.Path, short(s), re.String())
		}
		if f.Format != "" && !formatOK(f.Format, s) {
			add("format", "%s %q is not a valid %s", f.Path, short(s), f.Format)
		}
	}
	if len(f.Enum) > 0 && !contains(f.Enum, render(v)) {
		add("enum", "%s is %s, not one of %v", f.Path, short(render(v)), f.Enum)
	}
}

func formatOK(format, s string) bool {
	switch format {
	case "email":
		a, err := mail.ParseAddress(s)
		return err == nil && a.Address == s
	case "uuid":
		return uuidRe.MatchString(s)
	case "date-time":
		_, err := time.Parse(time.RFC3339, s)
		return err == nil
	case "date":
		_, err := time.Parse("2006-01-02", s)
		return err == nil
	case "url":
		u, err := url.Parse(s)
		return err == nil && u.Scheme != "" && u.Host != ""
	case "ipv4":
		ip := net.ParseIP(s)
		return ip != nil && ip.To4() != nil
	}
	return true
}

func typeMatches(want string, v any) bool {
	got := typeOf(v)
	if want == "integer" {
		n, ok := v.(json.Number)
		if !ok {
			return false
		}
		_, err := strconv.ParseInt(n.String(), 10, 64)
		return err == nil
	}
	return got == want
}

func typeOf(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case json.Number, float64:
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

func render(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case json.Number:
		return t.String()
	case nil:
		return "null"
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func short(s string) string {
	if len([]rune(s)) > 60 {
		return string([]rune(s)[:60]) + "..."
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

// Summary joins violations into one reason line for headers and events.
func Summary(vs []Violation) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = v.Message
	}
	return "data rules: " + strings.Join(parts, "; ")
}
