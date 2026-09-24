package cluster

import "testing"

func TestDistributeEvenSplit(t *testing.T) {
	got := distribute(6, []string{"a", "b", "c"})
	want := map[string]int{"a": 2, "b": 2, "c": 2}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("node %s: got %d workers, want %d", id, got[id], w)
		}
	}
}

func TestDistributeRemainderGoesToFirstNodesInSortedOrder(t *testing.T) {
	got := distribute(5, []string{"b", "a", "c"})
	want := map[string]int{"a": 2, "b": 2, "c": 1}
	for id, w := range want {
		if got[id] != w {
			t.Errorf("node %s: got %d workers, want %d", id, got[id], w)
		}
	}
}

func TestDistributeFewerWorkersThanNodes(t *testing.T) {
	got := distribute(2, []string{"a", "b", "c"})
	sum := 0
	for _, w := range got {
		sum += w
	}
	if sum != 2 {
		t.Errorf("expected total distributed workers to equal 2, got %d", sum)
	}
	if got["c"] != 0 {
		t.Errorf("expected the last sorted node to get 0 workers when supply is short, got %d", got["c"])
	}
}

func TestDistributeNoLiveNodes(t *testing.T) {
	got := distribute(3, nil)
	if len(got) != 0 {
		t.Errorf("expected an empty assignment with no live nodes, got %v", got)
	}
}

func TestDistributeZeroOrNegativeTotal(t *testing.T) {
	got := distribute(0, []string{"a", "b"})
	if len(got) != 0 {
		t.Errorf("expected an empty assignment for a zero total, got %v", got)
	}
}

func TestEligibleFiltersBySelector(t *testing.T) {
	live := map[string]heartbeatRecord{
		"a": {Labels: map[string]string{"zone": "dmz", "tier": "gold"}},
		"b": {Labels: map[string]string{"zone": "core"}},
		"c": {},
	}
	if got := eligible(live, map[string]string{"zone": "dmz"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("zone=dmz: got %v, want [a]", got)
	}
	if got := eligible(live, nil); len(got) != 3 {
		t.Errorf("no selector should match every live node, got %v", got)
	}
	if got := eligible(live, map[string]string{"zone": "edge"}); len(got) != 0 {
		t.Errorf("unmatched selector should match nothing, got %v", got)
	}
}
