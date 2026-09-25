package mcpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/authz"
	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/consumer"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/tap"
)

// Console serves what the ARK console needs over REST: the same overview,
// diagnosis, tuning, data checks and config preview/confirm flow the MCP
// assistant uses, plus topology, live tail, restart and scale. Access is
// decided by the REST API tokens (see api.Guard), not by mcp_access.
type Console struct {
	d  Deps
	cf *confirmations
	// Tap is where live tail records come from; nil means tap.Default.
	Tap *tap.Hub
}

// Restarter is implemented by registries that can restart a pipeline's
// workers on this node.
type Restarter interface {
	Restart(name string) (bool, error)
}

func NewConsole(d Deps) *Console {
	d.AllPipelines = true
	return &Console{d: d, cf: newConfirmations()}
}

// Handler returns the console routes. Mount it behind api.Guard.
func (c *Console) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	d := c.d

	mux.HandleFunc("GET /api/v1/overview", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, overview(d))
	})

	mux.HandleFunc("GET /api/v1/events", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		in := eventsIn{Pipeline: q.Get("pipeline")}
		in.SinceMinutes, _ = strconv.Atoi(q.Get("since_minutes"))
		in.Limit, _ = strconv.Atoi(q.Get("limit"))
		if k := q.Get("kinds"); k != "" {
			in.Kinds = strings.Split(k, ",")
		}
		out, err := recentEvents(d, in)
		if err != nil {
			writeJSON(w, http.StatusNotFound, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("GET /api/v1/pipelines/{name}/diagnosis", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		writeJSON(w, http.StatusOK, diagnose(d, p))
	}))

	mux.HandleFunc("GET /api/v1/pipelines/{name}/tuning", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		target, _ := strconv.ParseFloat(r.URL.Query().Get("target_msgs_per_sec"), 64)
		writeJSON(w, http.StatusOK, recommendTuning(r.Context(), d, p, target))
	}))

	mux.HandleFunc("GET /api/v1/pipelines/{name}/data-check", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		n, _ := strconv.Atoi(r.URL.Query().Get("sample"))
		out, err := checkData(r.Context(), d, p, n, r.URL.Query().Get("from"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	}))

	mux.HandleFunc("POST /api/v1/pipelines/{name}/test-message", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		var in testMessageIn
		if !readJSON(w, r, &in) {
			return
		}
		out, err := testMessage(p, in)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	}))

	mux.HandleFunc("GET /api/v1/pipelines/{name}/tail", c.withPipeline(c.tail))

	mux.HandleFunc("POST /api/v1/pipelines/{name}/restart", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		rs, ok := d.Registry.(Restarter)
		if !ok {
			writeJSON(w, http.StatusNotImplemented, map[string]string{"error": "restart is not available"})
			return
		}
		running, err := rs.Restart(p.Name)
		switch {
		case err != nil:
			writeJSON(w, http.StatusInternalServerError, errBody(err))
		case !running:
			writeJSON(w, http.StatusConflict, map[string]string{"error": "pipeline " + p.Name + " has no workers on this node; restart it through a node that runs it"})
		default:
			c.audit(r, "restart", p.Name)
			writeJSON(w, http.StatusOK, map[string]string{"pipeline": p.Name, "state": "restarted"})
		}
	}))

	mux.HandleFunc("POST /api/v1/pipelines/{name}/scale", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		var in struct {
			Workers int `json:"workers"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		if in.Workers < 1 || in.Workers > 1024 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "workers must be between 1 and 1024"})
			return
		}
		from := p.Workers
		p.Workers = in.Workers
		updated := d.Config.Pipelines()
		for i := range updated {
			if updated[i].Name == p.Name {
				updated[i] = p
			}
		}
		if err := config.ValidatePipelines(updated); err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		if err := d.Config.Apply(r.Context(), updated); err != nil {
			writeJSON(w, http.StatusInternalServerError, errBody(err))
			return
		}
		c.audit(r, "scale", p.Name)
		events.Record(p.Name, events.ConfigApplied, fmt.Sprintf("workers changed from %d to %d through the console", from, in.Workers), nil)
		writeJSON(w, http.StatusOK, map[string]any{"pipeline": p.Name, "workers": in.Workers, "applies_to": d.Config.Mode()})
	}))

	mux.HandleFunc("GET /api/v1/topics", func(w http.ResponseWriter, r *http.Request) {
		topics, err := listTopics(r.Context(), d)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, topics)
	})

	mux.HandleFunc("GET /api/v1/topology", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, topology(d))
	})

	mux.HandleFunc("GET /api/v1/config/schema", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, schemaOut{Fields: pipelineSchema, Example: exampleYAML})
	})

	mux.HandleFunc("GET /api/v1/config/pipelines", func(w http.ResponseWriter, r *http.Request) {
		out := []consoleConfigOut{}
		for _, p := range d.Config.Pipelines() {
			out = append(out, consoleConfigOut{Name: p.Name, YAML: toYAML(p), AppliesTo: d.Config.Mode()})
		}
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("GET /api/v1/config/pipelines/{name}", c.withPipeline(func(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
		writeJSON(w, http.StatusOK, consoleConfigOut{Name: p.Name, YAML: toYAML(p), AppliesTo: d.Config.Mode()})
	}))

	mux.HandleFunc("POST /api/v1/config/validate", func(w http.ResponseWriter, r *http.Request) {
		var in yamlIn
		if !readJSON(w, r, &in) {
			return
		}
		_, _, out := validate(r.Context(), d, in.YAML)
		writeJSON(w, http.StatusOK, out)
	})

	// Changes are two steps, like the MCP config tools: preview returns a
	// diff and a confirm token, and only confirm with that token applies
	// it. The token is bound to the caller that previewed it.
	mux.HandleFunc("POST /api/v1/config/preview", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			YAML   string `json:"yaml"`
			Delete string `json:"delete"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		var out applyOut
		var err error
		switch {
		case in.Delete != "":
			out, err = previewDelete(d, c.cf, callerOf(r).ID, in.Delete)
		case in.YAML != "":
			out, err = previewChange(r.Context(), d, c.cf, "console_apply", callerOf(r).ID, in.YAML)
		default:
			err = fmt.Errorf("send yaml (create or update) or delete (a pipeline name)")
		}
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	mux.HandleFunc("POST /api/v1/config/confirm", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			ConfirmToken string `json:"confirm_token"`
		}
		if !readJSON(w, r, &in) {
			return
		}
		caller := callerOf(r)
		accept := func(a string) bool { return a == "console_apply" || a == actionDelete }
		out, err := confirmChange(r.Context(), d, c.cf, caller.ID, in.ConfirmToken, accept, "the console", string(caller.Scope))
		if err != nil {
			writeJSON(w, http.StatusConflict, errBody(err))
			return
		}
		writeJSON(w, http.StatusOK, out)
	})

	return mux
}

func (c *Console) withPipeline(h func(http.ResponseWriter, *http.Request, config.Pipeline)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, ok := c.d.pipeline(r.PathValue("name"))
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found: " + r.PathValue("name")})
			return
		}
		h(w, r, p)
	}
}

func (c *Console) audit(r *http.Request, action, pipeline string) {
	if c.d.Audit != nil {
		caller := callerOf(r)
		c.d.Audit.Info("console write", "scope", caller.Scope, "action", action, "pipeline", pipeline)
	}
}

// tail streams what crosses a pipeline on this node as Server-Sent Events.
// Filters: stage (in, callback, out), to (destination, reject, dlq, ...),
// key, correlation_id, and max_per_sec (default 50) to keep a busy
// pipeline readable. In cluster mode each node only sees its own workers.
func (c *Console) tail(w http.ResponseWriter, r *http.Request, p config.Pipeline) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "streaming is not supported"})
		return
	}
	q := r.URL.Query()
	stage, to, key, corr := q.Get("stage"), q.Get("to"), q.Get("key"), q.Get("correlation_id")
	perSec, err := strconv.Atoi(q.Get("max_per_sec"))
	if err != nil || perSec <= 0 || perSec > 1000 {
		perSec = 50
	}
	hub := c.Tap
	if hub == nil {
		hub = tap.Default
	}
	sub, stop := hub.Subscribe(p.Name, 512)
	defer stop()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	local := len(c.d.Registry.PipelineRunners(p.Name))
	fmt.Fprintf(w, "event: hello\ndata: {\"pipeline\":%q,\"local_workers\":%d}\n\n", p.Name, local)
	flusher.Flush()

	ping := time.NewTicker(15 * time.Second)
	defer ping.Stop()
	window, sent, skipped := time.Now(), 0, 0
	var lastDropped int64
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			dropped := sub.Dropped() - lastDropped
			lastDropped = sub.Dropped()
			fmt.Fprintf(w, "event: ping\ndata: {\"skipped\":%d,\"dropped\":%d}\n\n", skipped, dropped)
			skipped = 0
			flusher.Flush()
		case rec := <-sub.C:
			if (stage != "" && rec.Stage != stage) || (to != "" && rec.To != to) || (key != "" && rec.Key != key) || (corr != "" && rec.CorrelationID != corr) {
				continue
			}
			if time.Since(window) >= time.Second {
				window, sent = time.Now(), 0
			}
			if sent >= perSec {
				skipped++
				continue
			}
			sent++
			rec.Time = c.d.localize(rec.Time)
			b, _ := json.Marshal(rec)
			fmt.Fprintf(w, "event: record\ndata: %s\n\n", b)
			flusher.Flush()
		}
	}
}

// Topology describes how pipelines connect through topics and targets, so
// the console can draw chained pipelines as one graph.
type Topology struct {
	Nodes []TopologyNode `json:"nodes"`
	Edges []TopologyEdge `json:"edges"`
}

type TopologyNode struct {
	ID    string         `json:"id"`
	Kind  string         `json:"kind"` // topic, pipeline, target, webhook
	Label string         `json:"label"`
	Stats *PipelineStats `json:"stats,omitempty"`
}

type TopologyEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Role string `json:"role"` // consume, call, destination, reject, dead_letter, override, webhook
	Rule string `json:"rule,omitempty"`
}

// PipelineStats sums a pipeline's workers on this node. The console turns
// the counters into rates by polling.
type PipelineStats struct {
	Enabled       bool    `json:"enabled"`
	LocalWorkers  int     `json:"local_workers"`
	Running       int     `json:"running"`
	Paused        bool    `json:"paused"`
	BreakerState  string  `json:"breaker_state,omitempty"`
	Processed     int64   `json:"processed"`
	Rejected      int64   `json:"rejected"`
	DeadLettered  int64   `json:"dead_lettered"`
	Failed        int64   `json:"failed"`
	Lag           int64   `json:"lag"`
	AvgCallbackMs float64 `json:"avg_callback_ms"`
}

func pipelineStats(p config.Pipeline, statuses []consumer.Status) *PipelineStats {
	s := &PipelineStats{Enabled: p.IsEnabled(), LocalWorkers: len(statuses)}
	var ms float64
	var calls int64
	for _, st := range statuses {
		if st.Running {
			s.Running++
		}
		s.Paused = s.Paused || st.Paused
		if st.BreakerState != "" && st.BreakerState != "closed" || s.BreakerState == "" {
			s.BreakerState = st.BreakerState
		}
		s.Processed += st.Processed
		s.Rejected += st.Rejected
		s.DeadLettered += st.DeadLettered
		s.Failed += st.Failed
		s.Lag += st.Lag
		ms += st.AvgCallbackMs * float64(st.CallbackCalls)
		calls += st.CallbackCalls
	}
	if calls > 0 {
		s.AvgCallbackMs = ms / float64(calls)
	}
	return s
}

func topology(d Deps) Topology {
	var t Topology
	seen := map[string]bool{}
	node := func(id, kind, label string) {
		if !seen[id] {
			seen[id] = true
			t.Nodes = append(t.Nodes, TopologyNode{ID: id, Kind: kind, Label: label})
		}
	}
	edge := func(from, to, role, rule string) {
		t.Edges = append(t.Edges, TopologyEdge{From: from, To: to, Role: role, Rule: rule})
	}
	topic := func(name string) string {
		id := "topic:" + name
		node(id, "topic", name)
		return id
	}
	pipelines := d.visiblePipelines()
	sort.Slice(pipelines, func(i, j int) bool { return pipelines[i].Name < pipelines[j].Name })
	for _, p := range pipelines {
		pid := "pipeline:" + p.Name
		seen[pid] = true
		t.Nodes = append(t.Nodes, TopologyNode{ID: pid, Kind: "pipeline", Label: p.Name, Stats: pipelineStats(p, pipelineStatuses(d.Registry, p.Name))})
		edge(topic(p.SourceTopic), pid, "consume", "")
		urls := p.Target.URLs
		if p.Target.URL != "" {
			urls = append([]string{p.Target.URL}, urls...)
		}
		for _, u := range urls {
			node("target:"+u, "target", u)
			edge(pid, "target:"+u, "call", "")
		}
		if p.DestinationTopic != "" {
			edge(pid, topic(p.DestinationTopic), "destination", "")
		}
		if p.RejectTopic != "" {
			edge(pid, topic(p.RejectTopic), "reject", "")
		}
		if p.DeadLetterTopic != "" {
			edge(pid, topic(p.DeadLetterTopic), "dead_letter", "")
		}
		rules := append(append([]config.FastPathRule{}, p.FastPathRules...), p.PostCallbackRules...)
		for _, rule := range rules {
			if rule.DestinationOverride != "" {
				edge(pid, topic(rule.DestinationOverride), "override", rule.Name)
			}
			if rule.WebhookOverride != "" {
				node("webhook:"+rule.WebhookOverride, "webhook", rule.WebhookOverride)
				edge(pid, "webhook:"+rule.WebhookOverride, "webhook", rule.Name)
			}
		}
	}
	return t
}

func callerOf(r *http.Request) authz.Caller {
	c, _ := authz.CallerFrom(r.Context())
	return c
}

func errBody(err error) map[string]string { return map[string]string{"error": err.Error()} }

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err == nil {
		err = json.Unmarshal(body, v)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "request body must be JSON: " + err.Error()})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

type consoleConfigOut struct {
	Name      string `json:"name"`
	YAML      string `json:"yaml"`
	AppliesTo string `json:"applies_to"`
}
