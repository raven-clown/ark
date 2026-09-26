package mcpserver

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
	"github.com/raven-clown/ark/bridge-engine/internal/dlq"
	"github.com/raven-clown/ark/bridge-engine/internal/events"
	"github.com/raven-clown/ark/bridge-engine/internal/tuning"
)

// Severity levels, worst last.
const (
	SeverityOK       = "ok"
	SeverityInfo     = "info"
	SeverityWarning  = "warning"
	SeverityCritical = "critical"
)

var severityRank = map[string]int{SeverityOK: 0, SeverityInfo: 1, SeverityWarning: 2, SeverityCritical: 3}

// Finding is one thing worth telling an operator: what is happening, why
// (with the evidence), and what they can do about it.
type Finding struct {
	Severity string   `json:"severity"`
	What     string   `json:"what"`
	Why      string   `json:"why,omitempty"`
	Actions  []string `json:"suggested_actions,omitempty"`
}

type Numbers struct {
	LocalWorkers             int     `json:"local_workers"`
	ClusterWorkers           int     `json:"cluster_workers,omitempty"`
	Processed                int64   `json:"processed"`
	Rejected                 int64   `json:"rejected"`
	DeadLettered             int64   `json:"dead_lettered"`
	FailedAttempts           int64   `json:"failed_attempts"`
	Lag                      int64   `json:"lag"`
	OldestUncommittedSeconds float64 `json:"oldest_uncommitted_seconds"`
	AvgCallbackMs            float64 `json:"avg_callback_ms"`
	BreakerState             string  `json:"breaker_state,omitempty"`
	Paused                   bool    `json:"paused"`
	LastActivityAt           string  `json:"last_activity_at,omitempty"`
	PendingDLQ               int     `json:"pending_dlq_entries"`
	PendingRejects           int     `json:"pending_reject_entries"`
}

type Diagnosis struct {
	Pipeline     string         `json:"pipeline"`
	Health       string         `json:"health"`
	Summary      string         `json:"summary"`
	Findings     []Finding      `json:"findings"`
	Numbers      Numbers        `json:"numbers"`
	RecentEvents []events.Event `json:"recent_events,omitempty"`
	RecentDLQ    []dlq.Entry    `json:"recent_dlq_entries,omitempty"`
}

const (
	maxEventsInAnswer = 15
)

func diagnose(d Deps, p config.Pipeline) Diagnosis {
	diag := Diagnosis{Pipeline: p.Name}
	add := func(f Finding) { diag.Findings = append(diag.Findings, f) }

	statuses := pipelineStatuses(d.Registry, p.Name)
	n := &diag.Numbers
	n.LocalWorkers = len(statuses)
	var lastActivity time.Time
	var latencySum float64
	var latencyCalls int64
	for _, st := range statuses {
		n.Processed += st.Processed
		n.Rejected += st.Rejected
		n.DeadLettered += st.DeadLettered
		n.FailedAttempts += st.Failed
		n.Lag += st.Lag
		n.OldestUncommittedSeconds = max(n.OldestUncommittedSeconds, st.OldestUncommittedSeconds)
		n.Paused = n.Paused || st.Paused
		n.BreakerState = st.BreakerState
		latencySum += st.AvgCallbackMs * float64(st.CallbackCalls)
		latencyCalls += st.CallbackCalls
		if st.LastActivityAt != nil {
			if t, err := time.Parse(time.RFC3339, *st.LastActivityAt); err == nil && t.After(lastActivity) {
				lastActivity = t
			}
		}
	}
	if latencyCalls > 0 {
		n.AvgCallbackMs = latencySum / float64(latencyCalls)
	}
	if !lastActivity.IsZero() {
		n.LastActivityAt = d.localize(lastActivity).Format(time.RFC3339)
	}
	if d.Cluster != nil {
		if v, ok := d.Cluster.ClusterPipelines()[p.Name]; ok {
			n.ClusterWorkers = v.Total.Workers
			n.Processed = max(n.Processed, v.Total.Processed)
			n.Rejected = max(n.Rejected, v.Total.Rejected)
			n.DeadLettered = max(n.DeadLettered, v.Total.DeadLettered)
			n.FailedAttempts = max(n.FailedAttempts, v.Total.Failed)
			n.Lag = max(n.Lag, v.Total.Lag)
			if v.Total.CallbackCalls > 0 {
				n.AvgCallbackMs = v.Total.AvgCallbackMs
			}
			n.Paused = n.Paused || v.Total.Paused
		}
	}

	var dlqEntries, rejectEntries []dlq.Entry
	if b, _, err := findDLQBrowser(d.Registry, p.Name, "dlq"); err == nil {
		dlqEntries = b.List()
	}
	if b, _, err := findDLQBrowser(d.Registry, p.Name, "reject"); err == nil {
		rejectEntries = b.List()
	}
	n.PendingDLQ, n.PendingRejects = len(dlqEntries), len(rejectEntries)
	if len(dlqEntries) > 0 {
		diag.RecentDLQ = d.localizeEntries(dlqEntries[max(0, len(dlqEntries)-3):])
	}

	log := d.events()
	since := time.Now().Add(-tuning.DiagnosisWindow())
	diag.RecentEvents = d.localizeEvents(log.Recent(p.Name, time.Time{}, maxEventsInAnswer))
	latest := func(kinds ...events.Kind) (events.Event, bool) {
		ev := log.Recent(p.Name, since, 1, kinds...)
		if len(ev) == 0 {
			return events.Event{}, false
		}
		return ev[0], true
	}
	ago := func(t time.Time) string { return time.Since(t).Round(time.Second).String() + " ago" }

	if !p.IsEnabled() {
		add(Finding{Severity: SeverityInfo, What: "This pipeline is disabled in the config (enabled: false), so nothing consumes its source topic.",
			Actions: []string{"Set enabled: true with apply_pipeline_config when it should run."}})
	}

	if p.IsEnabled() && n.LocalWorkers == 0 {
		switch {
		case n.ClusterWorkers > 0:
			add(Finding{Severity: SeverityInfo, What: fmt.Sprintf("Runs on other cluster nodes (%d workers there), none on this node.", n.ClusterWorkers),
				Why: "The cluster leader placed this pipeline's workers elsewhere (node_selector, partition count, or an even split)."})
		default:
			f := Finding{Severity: SeverityCritical, What: "The pipeline is configured and enabled but has no running workers."}
			if ev, ok := latest(events.PipelineStartFailed); ok {
				f.Why = fmt.Sprintf("It failed to start %s: %s. ARK keeps retrying in the background.", ago(ev.Time), ev.Message)
				f.Actions = []string{"Check that Kafka is reachable from ARK and the brokers setting is right.", "Use explain_error on the error text for the likely cause."}
			} else {
				f.Why = "No start failure was recorded recently; the process may have just started, or the pipeline was removed from the config."
			}
			add(f)
		}
	}

	if n.Paused {
		f := Finding{Severity: SeverityWarning, What: "The pipeline is paused. Nothing is lost: messages wait in the source topic.",
			Actions: []string{"resume_pipeline when the reason for pausing is resolved."}}
		if ev, ok := latest(events.Paused); ok {
			f.Why = "An operator paused it " + ago(ev.Time) + "."
		}
		if n.Lag > 0 {
			f.What += fmt.Sprintf(" %d messages are waiting.", n.Lag)
		}
		add(f)
	}

	if n.BreakerState == "open" {
		f := Finding{Severity: SeverityCritical, What: "The circuit breaker is open: the target has been failing, so ARK stopped calling it and messages wait in place (not lost, not dead-lettered).",
			Actions: []string{
				fmt.Sprintf("Check the target at %s is up and answering 2xx.", targetDescription(p)),
				"ARK closes the breaker by itself once the target recovers" + healthCheckHint(p) + ".",
			}}
		if ev, ok := latest(events.BreakerOpened); ok {
			f.Why = fmt.Sprintf("It opened %s. Last failure: %s", ago(ev.Time), ev.Details["last_failure"])
		}
		add(f)
	}

	if n.OldestUncommittedSeconds >= tuning.DiagnosisStuck().Seconds() {
		f := Finding{Severity: SeverityWarning, What: fmt.Sprintf("A message has been in progress for %s and is holding back commits of every later message on its partition.", time.Duration(n.OldestUncommittedSeconds*float64(time.Second)).Round(time.Second))}
		if ev, ok := latest(events.MessageRetrying); ok {
			f.Why = fmt.Sprintf("It is being retried in place (partition %s, offset %s): %s", ev.Details["partition"], ev.Details["offset"], ev.Message)
			f.Actions = []string{"Fix the underlying cause shown above; ARK retries it automatically and then continues.", "If it must be skipped, it needs a dead_letter_topic (on_exhausted: block keeps retrying by design)."}
		} else if n.BreakerState == "open" {
			f.Why = "The circuit breaker is open, so the message is waiting for the target to recover."
		} else {
			f.Why = "The callback may be slow; average callback latency is " + fmt.Sprintf("%.0fms", n.AvgCallbackMs) + "."
		}
		if n.OldestUncommittedSeconds >= 5*tuning.DiagnosisStuck().Seconds() {
			f.Severity = SeverityCritical
		}
		add(f)
	}

	if p.IsEnabled() && !n.Paused && n.BreakerState != "open" && n.Lag > 0 && !lastActivity.IsZero() && time.Since(lastActivity) > tuning.DiagnosisStalled() {
		add(Finding{Severity: SeverityWarning,
			What:    fmt.Sprintf("%d messages are waiting but nothing has completed for %s.", n.Lag, time.Since(lastActivity).Round(time.Second)),
			Why:     "Workers are running and not paused, so they are either blocked on one message or not getting partitions assigned.",
			Actions: []string{"Look at recent_events for retries or worker restarts.", "Check the consumer group with your Kafka tools to confirm partitions are assigned."}})
	}

	if ev, ok := latest(events.TargetRateLimited); ok {
		count := len(log.Recent(p.Name, since, 1000, events.TargetRateLimited))
		add(Finding{Severity: SeverityWarning,
			What:    fmt.Sprintf("The target asked ARK to slow down %d time(s) in the last %s (HTTP 429/425/408).", count, tuning.DiagnosisWindow()),
			Why:     "Latest: " + ev.Message,
			Actions: []string{"Lower concurrency.max_in_flight or workers so ARK sends less at once, or raise the target's capacity/limits.", "Nothing is lost: ARK waits and retries without using up retry attempts."}})
	}

	if n.PendingDLQ > 0 {
		add(Finding{Severity: SeverityWarning,
			What: fmt.Sprintf("%d message(s) are sitting in the dead-letter topic.", n.PendingDLQ),
			Why:  "Most common reasons: " + topReasons(dlqEntries, 3),
			Actions: []string{"list_dlq_messages to look at them, fix the cause, then retry_dlq_message.",
				"If failures are usually temporary, set dead_letter_redrive to retry them on a schedule."}})
	}
	if n.PendingRejects > 0 {
		add(Finding{Severity: SeverityInfo,
			What:    fmt.Sprintf("%d message(s) were rejected as invalid (by data rules, reject rules or the target).", n.PendingRejects),
			Why:     "Most common reasons: " + topReasons(rejectEntries, 3),
			Actions: []string{"These usually need the producer of the data fixed, not a retry.", "check_data on the reject topic shows which fields and values are the problem."}})
	}

	if restarts := log.Recent(p.Name, since, 100, events.WorkerRestarted); len(restarts) > 0 {
		add(Finding{Severity: SeverityWarning,
			What:    fmt.Sprintf("Workers restarted on their own %d time(s) in the last %s.", len(restarts), tuning.DiagnosisWindow()),
			Why:     restarts[0].Message,
			Actions: []string{"Usually a Kafka connectivity problem; use explain_error on the message."}})
	}

	if n.AvgCallbackMs > 0 && n.AvgCallbackMs > 0.5*float64(p.Target.TimeoutMs) {
		add(Finding{Severity: SeverityWarning,
			What:    fmt.Sprintf("Callbacks average %.0fms, more than half of target.timeout_ms (%dms).", n.AvgCallbackMs, p.Target.TimeoutMs),
			Why:     "Slow callbacks at this level risk timing out, which counts as a failure and can open the breaker.",
			Actions: []string{"Raise target.timeout_ms or make the target faster; see recommend_tuning."}})
	}

	if len(diag.Findings) == 0 {
		add(Finding{Severity: SeverityOK, What: fmt.Sprintf("Healthy: %d processed, %d waiting, callbacks averaging %.0fms.", n.Processed, n.Lag, n.AvgCallbackMs)})
	}

	sort.SliceStable(diag.Findings, func(i, j int) bool {
		return severityRank[diag.Findings[i].Severity] > severityRank[diag.Findings[j].Severity]
	})
	diag.Health = healthOf(diag.Findings, n.Paused)
	diag.Summary = diag.Findings[0].What
	return diag
}

func healthOf(findings []Finding, paused bool) string {
	worst := SeverityOK
	for _, f := range findings {
		if severityRank[f.Severity] > severityRank[worst] {
			worst = f.Severity
		}
	}
	switch {
	case worst == SeverityCritical:
		return "down"
	case paused:
		return "paused"
	case worst == SeverityWarning:
		return "degraded"
	default:
		return "healthy"
	}
}

func topReasons(entries []dlq.Entry, n int) string {
	counts := map[string]int{}
	for _, e := range entries {
		r := e.Reason
		if r == "" {
			r = "no reason recorded (routed before reasons were recorded)"
		}
		if len(r) > 140 {
			r = r[:140] + "..."
		}
		counts[r]++
	}
	type rc struct {
		r string
		c int
	}
	var list []rc
	for r, c := range counts {
		list = append(list, rc{r, c})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].c > list[j].c })
	var parts []string
	for i := 0; i < len(list) && i < n; i++ {
		parts = append(parts, fmt.Sprintf("%dx %q", list[i].c, list[i].r))
	}
	return strings.Join(parts, "; ")
}

func targetDescription(p config.Pipeline) string {
	if p.Target.Mode == config.TargetModeMultiURL {
		return strings.Join(p.Target.URLs, ", ")
	}
	return p.Target.URL
}

func healthCheckHint(p config.Pipeline) string {
	if p.Target.HealthCheckURL != "" || len(p.Target.HealthCheckURLs) > 0 {
		return " (it probes the health check URL, so no new traffic is needed)"
	}
	return "; setting target.health_check_url lets it notice recovery without waiting for new traffic"
}
