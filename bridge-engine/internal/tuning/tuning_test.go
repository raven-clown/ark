package tuning

import (
	"testing"
	"time"
)

func TestDefaultsAndOverrides(t *testing.T) {
	Set(Values{})
	if RetryAfterCap() != 5*time.Minute || DLQBrowserEntries() != 200 || ProducerBatchTimeout() != 5*time.Millisecond {
		t.Fatal("defaults changed")
	}
	Set(Values{RetryAfterCapSeconds: 60, DLQBrowserEntries: 50})
	defer Set(Values{})
	if RetryAfterCap() != time.Minute || DLQBrowserEntries() != 50 {
		t.Fatal("overrides not applied")
	}
	if (Values{TailValueBytes: -1}).Validate() == nil || (Values{HistoryKeepMinutes: 99999}).Validate() == nil {
		t.Fatal("out-of-range values must be rejected")
	}
}
