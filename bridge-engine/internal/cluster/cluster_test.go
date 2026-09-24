package cluster

import (
	"reflect"
	"testing"

	"github.com/raven-clown/ark/bridge-engine/internal/config"
)

func TestReconcileLocalNoPlacementYetUsesFullConfig(t *testing.T) {
	var got []config.Pipeline
	n := &Node{
		id: "node-a",
		reconcile: func(pipelines []config.Pipeline) []error {
			got = pipelines
			return nil
		},
	}

	pipelines := []config.Pipeline{{Name: "orders", Workers: 3}}
	n.reconcileLocal(pipelines)

	if !reflect.DeepEqual(got, pipelines) {
		t.Errorf("expected the full pipeline set unmodified before any placement arrives, got %+v", got)
	}
}

func TestReconcileLocalAppliesAssignedWorkers(t *testing.T) {
	var got []config.Pipeline
	n := &Node{
		id: "node-a",
		reconcile: func(pipelines []config.Pipeline) []error {
			got = pipelines
			return nil
		},
		local: map[string]int{"orders": 2},
	}

	n.reconcileLocal([]config.Pipeline{{Name: "orders", Workers: 5}})

	if len(got) != 1 || got[0].Workers != 2 {
		t.Fatalf("expected the orders pipeline narrowed to 2 workers, got %+v", got)
	}
}

func TestReconcileLocalExcludesPipelinesAssignedElsewhere(t *testing.T) {
	var got []config.Pipeline
	n := &Node{
		id: "node-a",
		reconcile: func(pipelines []config.Pipeline) []error {
			got = pipelines
			return nil
		},
		local: map[string]int{"orders": 0},
	}

	n.reconcileLocal([]config.Pipeline{{Name: "orders", Workers: 5}})

	if len(got) != 0 {
		t.Fatalf("expected the orders pipeline excluded when this node's share is 0, got %+v", got)
	}
}

func TestReconcileLocalKeepsUndecidedPipelinesAtFullWorkers(t *testing.T) {
	var got []config.Pipeline
	n := &Node{
		id: "node-a",
		reconcile: func(pipelines []config.Pipeline) []error {
			got = pipelines
			return nil
		},
		local: map[string]int{"other-pipeline": 1},
	}

	n.reconcileLocal([]config.Pipeline{{Name: "orders", Workers: 4}})

	if len(got) != 1 || got[0].Workers != 4 {
		t.Fatalf("expected the orders pipeline to keep its full configured workers since the leader hasn't decided on it yet, got %+v", got)
	}
}
