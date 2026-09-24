package mcpserver

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

// interpret_request is the "understand before acting" pass. It reads the
// user's own words (Thai or English), works out what they want, ties every
// name they mention to something that actually exists in ARK, and says what
// is still ambiguous, so the model plans from facts instead of guesses.

type intentRule struct {
	intent   string
	keywords []string
	meaning  string
}

var intentRules = []intentRule{
	{"greeting", []string{"hello", "hi ", "hey", "สวัสดี", "หวัดดี", "ดีครับ", "ดีค่ะ", "yo "}, "The user is greeting; answer briefly and offer what you can do."},
	{"capabilities", []string{"what can you", "help", "ทำอะไรได้", "ช่วยอะไรได้", "ใช้ยังไง", "คุณคือ", "who are you", "ความสามารถ"}, "The user wants to know what this assistant can do."},
	{"overview", []string{"overview", "how is everything", "status", "ภาพรวม", "เป็นยังไงบ้าง", "เป็นไงบ้าง", "สถานะ", "ทั้งหมด", "ทุก pipeline", "summary", "สรุป", "ok ไหม", "ปกติไหม", "health"}, "The user wants the current state of ARK."},
	{"diagnose", []string{"why", "slow", "stuck", "not working", "broken", "down", "lag", "failing", "problem", "issue", "wrong", "ทำไม", "ช้า", "ค้าง", "ไม่ทำงาน", "พัง", "ล่ม", "ปัญหา", "ผิดปกติ", "หยุด", "ไม่ไหล", "ไม่วิ่ง", "เกิดอะไร", "เป็นอะไร"}, "The user wants to know what is wrong with something and why."},
	{"explain_error", []string{"error", "exception", "failed", "failure", "panic", "refused", "timeout", "unknown topic", "coordinator", "status 5", "status 4", "แปลว่า", "หมายความว่า", "มาจากไหน", "เออเร่อ", "เออเร่อร์", "ข้อผิดพลาด"}, "The user has an error or log line and wants it explained."},
	{"events", []string{"what happened", "history", "recently", "last night", "yesterday", "timeline", "log", "เกิดอะไรขึ้น", "ที่ผ่านมา", "ล่าสุด", "เมื่อคืน", "เมื่อวาน", "ประวัติ", "เมื่อกี้", "เมื่อเช้า"}, "The user wants to know what happened over some period."},
	{"tuning", []string{"tune", "tuning", "throughput", "capacity", "scale", "faster", "performance", "sizing", "spec", "how many workers", "msg/s", "msgs/s", "per second", "ปรับ", "เร็วขึ้น", "รองรับ", "สเปค", "สเปก", "ขนาด", "ต่อวินาที", "ประสิทธิภาพ", "แนะนำการตั้งค่า", "ควรตั้ง"}, "The user wants capacity/performance advice or recommended settings."},
	{"dlq", []string{"dlq", "dead letter", "dead-letter", "reject", "failed messages", "ตก dlq", "ข้อความที่ล้ม", "ข้อความเสีย", "ถูก reject", "ค้างใน dlq"}, "The user is asking about dead-lettered or rejected messages."},
	{"create", []string{"create", "new pipeline", "add a pipeline", "add pipeline", "set up", "setup", "สร้าง", "เพิ่ม pipeline", "ทำ pipeline", "เพิ่มท่อ", "สร้างท่อ"}, "The user wants a new pipeline."},
	{"change", []string{"change", "update", "modify", "edit", "set ", "increase", "decrease", "rename", "disable", "enable", "แก้", "เปลี่ยน", "เพิ่ม worker", "ลด", "ตั้งค่า", "ปิดใช้", "เปิดใช้"}, "The user wants to change an existing pipeline's config."},
	{"pause", []string{"pause", "stop", "halt", "หยุด", "พัก", "ปิดชั่วคราว", "stop ไว้"}, "The user wants to pause a pipeline."},
	{"resume", []string{"resume", "start again", "continue", "unpause", "เริ่มใหม่", "ทำต่อ", "เปิดต่อ", "กลับมาทำงาน", "resume"}, "The user wants to resume a pipeline."},
	{"retry", []string{"retry", "reprocess", "resend", "redrive", "ลองใหม่", "ส่งใหม่", "ประมวลผลใหม่", "รีไทร"}, "The user wants dead-lettered messages processed again."},
	{"config_view", []string{"config", "configuration", "settings", "yaml", "การตั้งค่า", "คอนฟิก", "ตั้งไว้ยังไง"}, "The user wants to see how something is configured."},
	{"topics", []string{"topic", "topics", "partition", "ท็อปปิก", "หัวข้อ"}, "The user is asking about Kafka topics."},
	{"check_data", []string{"weird data", "bad data", "strange", "odd", "malformed", "invalid", "format", "schema", "validate data", "data quality", "anomal", "parameter", "ข้อมูลแปลก", "ข้อมูลผิด", "ข้อมูลเสีย", "รูปแบบ", "ฟอร์แมต", "พารามิเตอร์", "ตรวจข้อมูล", "คุณภาพข้อมูล", "ผิดปกติ", "ไม่ตรง format"}, "The user wants the actual messages checked for odd formats, fields or values."},
	{"data_rules", []string{"data rule", "rule", "catch", "block bad", "reject if", "กฎ", "ดักจับ", "ดัก", "กรอง", "ห้าม", "บังคับ field"}, "The user wants rules that catch or block bad data."},
}

var planFor = map[string][]string{
	"greeting":      {"get_help", "get_overview"},
	"capabilities":  {"get_help"},
	"overview":      {"get_overview"},
	"diagnose":      {"diagnose_pipeline (for each pipeline mentioned, or get_overview first if none)", "get_recent_events if the cause isn't clear yet"},
	"explain_error": {"explain_error with the error text", "diagnose_pipeline for the pipeline where it occurred"},
	"events":        {"get_recent_events with the time window below"},
	"tuning":        {"recommend_tuning (pass target_msgs_per_sec if a rate was mentioned)"},
	"dlq":           {"list_dlq_messages", "diagnose_pipeline to see the most common reasons"},
	"create":        {"get_pipeline_schema", "list_topics", "ask for anything still missing (see clarifications)", "validate_pipeline_config", "create_pipeline (preview, then confirm with the user)"},
	"change":        {"get_pipeline_config", "validate_pipeline_config with the change", "apply_pipeline_config (preview, then confirm with the user)"},
	"pause":         {"confirm with the user which pipeline, then pause_pipeline"},
	"resume":        {"resume_pipeline"},
	"retry":         {"list_dlq_messages", "retry_dlq_message for the entries the user means (confirm if several)"},
	"config_view":   {"get_pipeline_config"},
	"topics":        {"list_topics"},
	"check_data":    {"check_data (from: source; also dlq if failures are the concern)", "test_message with an example the user gave, if any"},
	"data_rules":    {"check_data to see real data and get suggested_data_rules_yaml", "get_pipeline_config", "apply_pipeline_config with data_rules added (preview, then confirm; start with on_violation: tag)"},
}

type nameMatch struct {
	Mentioned  string  `json:"mentioned"`
	Pipeline   string  `json:"pipeline"`
	Confidence float64 `json:"confidence"`
}

type interpretOut struct {
	Intents        []intentOut `json:"intents"`
	Pipelines      []nameMatch `json:"pipelines"`
	Topics         []string    `json:"topics,omitempty"`
	URLs           []string    `json:"urls,omitempty"`
	RatePerSec     float64     `json:"rate_per_sec,omitempty"`
	SinceMinutes   int         `json:"since_minutes,omitempty"`
	ErrorText      string      `json:"error_text,omitempty"`
	Clarifications []string    `json:"ask_the_user,omitempty"`
	Plan           []string    `json:"plan"`
	Restated       string      `json:"restated_request"`
}

type intentOut struct {
	Intent  string `json:"intent"`
	Meaning string `json:"meaning"`
}

var (
	urlRe    = regexp.MustCompile(`https?://[^\s"'<>]+`)
	rateRe   = regexp.MustCompile(`(?i)(\d[\d,\.]*)\s*(k)?\s*(msg|msgs|messages|ข้อความ|รายการ|events|req|requests|ครั้ง)?\s*(/|per|ต่อ)\s*(s|sec|second|วิ|วินาที)`)
	rateZhRe = regexp.MustCompile(`每秒\s*(\d[\d,\.]*)\s*(千|万|萬|k)?|(\d[\d,\.]*)\s*(千|万|萬|k)?\s*(条|條|个|個|笔|筆)?\s*/\s*秒`)
	errLike  = regexp.MustCompile(`(?i)(error|exception|failed|refused|timeout|unknown|not available|status \d{3}|\[\d+\])`)
)

var timeWindows = []struct {
	words   []string
	minutes int
}{
	{[]string{"last hour", "past hour", "ชั่วโมงที่แล้ว", "ชั่วโมงก่อน", "1 ชั่วโมง"}, 60},
	{[]string{"last night", "เมื่อคืน", "overnight"}, 14 * 60},
	{[]string{"yesterday", "เมื่อวาน"}, 36 * 60},
	{[]string{"today", "วันนี้"}, 24 * 60},
	{[]string{"this morning", "เมื่อเช้า"}, 12 * 60},
	{[]string{"just now", "a moment ago", "เมื่อกี้", "เมื่อกี๊", "ตะกี้"}, 15},
	{[]string{"last 5 minutes", "5 นาที"}, 5},
	{[]string{"last 30 minutes", "30 นาที", "ครึ่งชั่วโมง"}, 30},
}

func interpret(d Deps, text string) interpretOut {
	lower := strings.ToLower(" " + text + " ")
	out := interpretOut{}

	seen := map[string]bool{}
	for _, r := range intentRules {
		for _, k := range r.keywords {
			if strings.Contains(lower, strings.ToLower(k)) && !seen[r.intent] {
				seen[r.intent] = true
				out.Intents = append(out.Intents, intentOut{Intent: r.intent, Meaning: r.meaning})
			}
		}
	}

	out.URLs = urlRe.FindAllString(text, -1)
	if m := rateRe.FindStringSubmatch(text); m != nil {
		if v, err := strconv.ParseFloat(strings.ReplaceAll(m[1], ",", ""), 64); err == nil {
			if m[2] != "" {
				v *= 1000
			}
			out.RatePerSec = v
			if !seen["tuning"] && !seen["create"] {
				seen["tuning"] = true
				out.Intents = append(out.Intents, intentOut{Intent: "tuning", Meaning: "A rate was mentioned, so the user likely cares about throughput."})
			}
		}
	}
	if out.RatePerSec == 0 {
		if m := rateZhRe.FindStringSubmatch(text); m != nil {
			num, unit := m[1], m[2]
			if num == "" {
				num, unit = m[3], m[4]
			}
			if v, err := strconv.ParseFloat(strings.ReplaceAll(num, ",", ""), 64); err == nil {
				switch unit {
				case "千", "k":
					v *= 1000
				case "万", "萬":
					v *= 10000
				}
				out.RatePerSec = v
				if !seen["tuning"] && !seen["create"] {
					seen["tuning"] = true
					out.Intents = append(out.Intents, intentOut{Intent: "tuning", Meaning: "A rate was mentioned, so the user likely cares about throughput."})
				}
			}
		}
	}
	for _, tw := range timeWindows {
		for _, w := range tw.words {
			if strings.Contains(lower, w) {
				out.SinceMinutes = tw.minutes
			}
		}
		if out.SinceMinutes != 0 {
			break
		}
	}
	if seen["explain_error"] || errLike.MatchString(text) {
		if loc := errLike.FindStringIndex(text); loc != nil {
			out.ErrorText = strings.TrimSpace(text)
			if !seen["explain_error"] {
				seen["explain_error"] = true
				out.Intents = append(out.Intents, intentOut{Intent: "explain_error", Meaning: "The message contains what looks like an error."})
			}
		}
	}

	pipelines := d.visiblePipelines()
	out.Pipelines = matchPipelines(text, pipelines)
	out.Topics = mentionedTopics(text, pipelines)

	if len(out.Intents) == 0 {
		out.Intents = append(out.Intents, intentOut{Intent: "unclear", Meaning: "No clear request was recognized."})
		out.Clarifications = append(out.Clarifications, "Ask what they'd like to do: see how things are running, investigate a problem, explain an error, or create/change a pipeline.")
	}

	ambiguous := map[string][]string{}
	for _, m := range out.Pipelines {
		ambiguous[m.Mentioned] = append(ambiguous[m.Mentioned], m.Pipeline)
	}
	for mentioned, names := range ambiguous {
		if len(names) > 1 {
			out.Clarifications = append(out.Clarifications, fmt.Sprintf("%q could mean %s; ask which one.", mentioned, strings.Join(names, " or ")))
		}
	}
	needsPipeline := seen["diagnose"] || seen["pause"] || seen["resume"] || seen["change"] || seen["tuning"] || seen["retry"] || seen["config_view"]
	if needsPipeline && len(out.Pipelines) == 0 {
		switch {
		case len(pipelines) == 1:
			out.Pipelines = append(out.Pipelines, nameMatch{Mentioned: "(only pipeline)", Pipeline: pipelines[0].Name, Confidence: 0.6})
		case seen["diagnose"]:
			out.Clarifications = append(out.Clarifications, "No pipeline was named; call get_overview and diagnose the ones that aren't healthy, or ask which one they mean.")
		default:
			out.Clarifications = append(out.Clarifications, "Ask which pipeline they mean (list_pipelines has the names).")
		}
	}
	if seen["create"] {
		if len(out.URLs) == 0 {
			out.Clarifications = append(out.Clarifications, "Ask for the HTTP endpoint ARK should call (target.url).")
		}
		if len(out.Topics) == 0 {
			out.Clarifications = append(out.Clarifications, "Ask which topic to read from and where results should go (or propose names and confirm).")
		}
	}
	if seen["pause"] {
		out.Clarifications = append(out.Clarifications, "Pausing stops processing; confirm with the user before calling pause_pipeline.")
	}

	for _, it := range out.Intents {
		out.Plan = append(out.Plan, planFor[it.Intent]...)
	}
	out.Plan = dedupe(out.Plan)
	out.Restated = restate(out)
	return out
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func restate(o interpretOut) string {
	var parts []string
	for _, it := range o.Intents {
		parts = append(parts, it.Intent)
	}
	s := "The user wants: " + strings.Join(parts, ", ")
	if len(o.Pipelines) > 0 {
		var names []string
		for _, p := range o.Pipelines {
			names = append(names, p.Pipeline)
		}
		s += "; about pipeline(s) " + strings.Join(dedupe(names), ", ")
	}
	if o.SinceMinutes > 0 {
		s += fmt.Sprintf("; time window about the last %s", (time.Duration(o.SinceMinutes) * time.Minute).String())
	}
	if o.RatePerSec > 0 {
		s += fmt.Sprintf("; target rate %.0f msgs/s", o.RatePerSec)
	}
	return s + "."
}

// words splits text into lowercase alphanumeric tokens, keeping Thai runs.
func words(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !(unicode.IsLetter(r) || unicode.IsDigit(r))
	})
}

// matchPipelines links what the user wrote to real pipeline names: exact
// names, then names whose parts appear in the text ("orders" matches
// "order-processor"), then close spellings.
func matchPipelines(text string, pipelines []config.Pipeline) []nameMatch {
	lower := strings.ToLower(text)
	tokens := words(text)
	var out []nameMatch
	for _, p := range pipelines {
		name := strings.ToLower(p.Name)
		if strings.Contains(lower, name) {
			out = append(out, nameMatch{Mentioned: p.Name, Pipeline: p.Name, Confidence: 1})
			continue
		}
		best, bestTok := 0.0, ""
		for _, part := range words(p.Name) {
			if len([]rune(part)) < 3 {
				continue
			}
			for _, t := range tokens {
				if len([]rune(t)) < 3 {
					continue
				}
				score := 0.0
				switch {
				case t == part:
					score = 0.85
				case strings.HasPrefix(t, part) || strings.HasPrefix(part, t):
					score = 0.75
				case similarity(t, part) >= 0.75:
					score = 0.6
				}
				if score > best {
					best, bestTok = score, t
				}
			}
		}
		if best > 0 {
			out = append(out, nameMatch{Mentioned: bestTok, Pipeline: p.Name, Confidence: best})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Confidence > out[j].Confidence })
	// Keep exact matches alone if there are any.
	if len(out) > 0 && out[0].Confidence == 1 {
		var exact []nameMatch
		for _, m := range out {
			if m.Confidence == 1 {
				exact = append(exact, m)
			}
		}
		return exact
	}
	return out
}

func mentionedTopics(text string, pipelines []config.Pipeline) []string {
	lower := strings.ToLower(text)
	seen := map[string]bool{}
	var out []string
	for _, p := range pipelines {
		for _, t := range []string{p.SourceTopic, p.DestinationTopic, p.DeadLetterTopic, p.RejectTopic} {
			if t != "" && !seen[t] && strings.Contains(lower, strings.ToLower(t)) {
				seen[t] = true
				out = append(out, t)
			}
		}
	}
	for _, w := range strings.Fields(text) {
		w = strings.Trim(w, ".,;:()'\"")
		if strings.Count(w, ".") >= 1 && !strings.Contains(w, "://") && !seen[w] && len(w) > 3 && !strings.ContainsAny(w, "/") {
			if _, err := strconv.ParseFloat(w, 64); err != nil {
				seen[w] = true
				out = append(out, w)
			}
		}
	}
	return out
}

// similarity is 1 - normalized Levenshtein distance.
func similarity(a, b string) float64 {
	ra, rb := []rune(a), []rune(b)
	if len(ra) == 0 || len(rb) == 0 {
		return 0
	}
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return 1 - float64(prev[len(rb)])/float64(max(len(ra), len(rb)))
}
