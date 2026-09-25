// Package tuning holds the engine-wide knobs from the config's tuning
// section. Code reads them through the getters, so a config reload takes
// effect the next time a value is used; each getter falls back to the
// built-in default when the config leaves it at zero.
package tuning

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Values is the config's tuning section. Zero means "use the default".
// Values marked (restart) are read once at startup; the rest apply on the
// next use after a reload.
type Values struct {
	DLQBrowserEntries         int `yaml:"dlq_browser_entries,omitempty"`
	EventLogEntries           int `yaml:"event_log_entries,omitempty"`
	RedriveCheckSeconds       int `yaml:"redrive_check_seconds,omitempty"`
	DLQPruneMinutes           int `yaml:"dlq_prune_minutes,omitempty"`
	RetryBackoffCapSeconds    int `yaml:"retry_backoff_cap_seconds,omitempty"`
	RetryAfterCapSeconds      int `yaml:"retry_after_cap_seconds,omitempty"`
	FinalCommitTimeoutSeconds int `yaml:"final_commit_timeout_seconds,omitempty"`
	ProducerBatchTimeoutMs    int `yaml:"producer_batch_timeout_ms,omitempty"`
	DiagnosisStuckSeconds     int `yaml:"diagnosis_stuck_seconds,omitempty"`
	DiagnosisStalledSeconds   int `yaml:"diagnosis_stalled_seconds,omitempty"`
	DiagnosisWindowMinutes    int `yaml:"diagnosis_window_minutes,omitempty"`
	HistorySampleSeconds      int `yaml:"history_sample_seconds,omitempty"`
	HistoryKeepMinutes        int `yaml:"history_keep_minutes,omitempty"`
	AssistantSessionMinutes   int `yaml:"assistant_session_minutes,omitempty"`
	ConfirmTokenMinutes       int `yaml:"confirm_token_minutes,omitempty"`
	ClusterCatchUpSeconds     int `yaml:"cluster_catch_up_seconds,omitempty"`
	TailValueBytes            int `yaml:"tail_value_bytes,omitempty"`
	CompactedIdleSeconds      int `yaml:"compacted_idle_seconds,omitempty"`
}

var current atomic.Pointer[Values]

// Set installs new values; the config loader calls it on start and reload.
func Set(v Values) { current.Store(&v) }

func get() Values {
	if v := current.Load(); v != nil {
		return *v
	}
	return Values{}
}

func or(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

func secs(v, def int) time.Duration { return time.Duration(or(v, def)) * time.Second }

func DLQBrowserEntries() int            { return or(get().DLQBrowserEntries, 200) }
func EventLogEntries() int              { return or(get().EventLogEntries, 2000) }
func RedriveCheck() time.Duration       { return secs(get().RedriveCheckSeconds, 10) }
func DLQPrune() time.Duration           { return time.Duration(or(get().DLQPruneMinutes, 10)) * time.Minute }
func RetryBackoffCap() time.Duration    { return secs(get().RetryBackoffCapSeconds, 30) }
func RetryAfterCap() time.Duration      { return secs(get().RetryAfterCapSeconds, 300) }
func FinalCommitTimeout() time.Duration { return secs(get().FinalCommitTimeoutSeconds, 10) }
func ProducerBatchTimeout() time.Duration {
	return time.Duration(or(get().ProducerBatchTimeoutMs, 5)) * time.Millisecond
}
func DiagnosisStuck() time.Duration   { return secs(get().DiagnosisStuckSeconds, 30) }
func DiagnosisStalled() time.Duration { return secs(get().DiagnosisStalledSeconds, 120) }
func DiagnosisWindow() time.Duration {
	return time.Duration(or(get().DiagnosisWindowMinutes, 15)) * time.Minute
}
func HistorySample() time.Duration { return secs(get().HistorySampleSeconds, 5) }
func HistoryKeep() time.Duration {
	return time.Duration(or(get().HistoryKeepMinutes, 60)) * time.Minute
}
func AssistantSession() time.Duration {
	return time.Duration(or(get().AssistantSessionMinutes, 120)) * time.Minute
}
func ConfirmToken() time.Duration {
	return time.Duration(or(get().ConfirmTokenMinutes, 10)) * time.Minute
}
func ClusterCatchUp() time.Duration { return secs(get().ClusterCatchUpSeconds, 10) }
func TailValueBytes() int           { return or(get().TailValueBytes, 4096) }
func CompactedIdle() time.Duration  { return secs(get().CompactedIdleSeconds, 2) }

// Validate checks that every value is in a sensible range.
func (v Values) Validate() error {
	limits := []struct {
		name     string
		val, max int
	}{
		{"dlq_browser_entries", v.DLQBrowserEntries, 100000}, {"event_log_entries", v.EventLogEntries, 1000000},
		{"redrive_check_seconds", v.RedriveCheckSeconds, 86400}, {"dlq_prune_minutes", v.DLQPruneMinutes, 10080},
		{"retry_backoff_cap_seconds", v.RetryBackoffCapSeconds, 3600}, {"retry_after_cap_seconds", v.RetryAfterCapSeconds, 86400},
		{"final_commit_timeout_seconds", v.FinalCommitTimeoutSeconds, 600}, {"producer_batch_timeout_ms", v.ProducerBatchTimeoutMs, 10000},
		{"diagnosis_stuck_seconds", v.DiagnosisStuckSeconds, 86400}, {"diagnosis_stalled_seconds", v.DiagnosisStalledSeconds, 86400},
		{"diagnosis_window_minutes", v.DiagnosisWindowMinutes, 10080}, {"history_sample_seconds", v.HistorySampleSeconds, 3600},
		{"history_keep_minutes", v.HistoryKeepMinutes, 10080}, {"assistant_session_minutes", v.AssistantSessionMinutes, 10080},
		{"confirm_token_minutes", v.ConfirmTokenMinutes, 1440}, {"cluster_catch_up_seconds", v.ClusterCatchUpSeconds, 600},
		{"tail_value_bytes", v.TailValueBytes, 1 << 20}, {"compacted_idle_seconds", v.CompactedIdleSeconds, 600},
	}
	for _, l := range limits {
		if l.val < 0 || l.val > l.max {
			return fmt.Errorf("tuning.%s must be between 0 (default) and %d, got %d", l.name, l.max, l.val)
		}
	}
	return nil
}

// Knob describes one setting for the console.
type Knob struct {
	Name    string `json:"name"`
	Value   int    `json:"value"` // 0 = default
	Default int    `json:"default"`
	Unit    string `json:"unit"`
	Restart bool   `json:"restart"` // read once at startup
}

// Describe lists every knob with its current setting and default.
func Describe() []Knob {
	v := get()
	return []Knob{
		{"retry_backoff_cap_seconds", v.RetryBackoffCapSeconds, 30, "s", false},
		{"retry_after_cap_seconds", v.RetryAfterCapSeconds, 300, "s", false},
		{"final_commit_timeout_seconds", v.FinalCommitTimeoutSeconds, 10, "s", false},
		{"producer_batch_timeout_ms", v.ProducerBatchTimeoutMs, 5, "ms", true},
		{"dlq_browser_entries", v.DLQBrowserEntries, 200, "", true},
		{"dlq_prune_minutes", v.DLQPruneMinutes, 10, "min", true},
		{"redrive_check_seconds", v.RedriveCheckSeconds, 10, "s", true},
		{"diagnosis_stuck_seconds", v.DiagnosisStuckSeconds, 30, "s", false},
		{"diagnosis_stalled_seconds", v.DiagnosisStalledSeconds, 120, "s", false},
		{"diagnosis_window_minutes", v.DiagnosisWindowMinutes, 15, "min", false},
		{"history_sample_seconds", v.HistorySampleSeconds, 5, "s", true},
		{"history_keep_minutes", v.HistoryKeepMinutes, 60, "min", false},
		{"event_log_entries", v.EventLogEntries, 2000, "", true},
		{"tail_value_bytes", v.TailValueBytes, 4096, "B", false},
		{"confirm_token_minutes", v.ConfirmTokenMinutes, 10, "min", false},
		{"assistant_session_minutes", v.AssistantSessionMinutes, 120, "min", false},
		{"cluster_catch_up_seconds", v.ClusterCatchUpSeconds, 10, "s", true},
		{"compacted_idle_seconds", v.CompactedIdleSeconds, 2, "s", false},
	}
}
