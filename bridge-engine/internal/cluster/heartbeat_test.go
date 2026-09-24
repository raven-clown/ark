package cluster

import (
	"testing"
	"time"
)

func TestLiveNodesExcludesStaleHeartbeats(t *testing.T) {
	v := newHeartbeatView()
	v.seen["fresh"] = time.Now()
	v.seen["stale"] = time.Now().Add(-time.Minute)

	live := v.liveNodes(10 * time.Second)

	if len(live) != 1 || live[0] != "fresh" {
		t.Fatalf("expected only the fresh node to be live, got %v", live)
	}
}

func TestLiveNodesEmptyWhenNoHeartbeatsSeen(t *testing.T) {
	v := newHeartbeatView()

	if live := v.liveNodes(10 * time.Second); len(live) != 0 {
		t.Fatalf("expected no live nodes, got %v", live)
	}
}
