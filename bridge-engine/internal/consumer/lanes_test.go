package consumer

import (
	"testing"
	"time"
)

func TestLanesSerializeSameKey(t *testing.T) {
	l := newLanes()
	wait1, release1 := l.acquire("k")
	if wait1 != nil {
		t.Fatal("first job for a key should not wait")
	}
	wait2, release2 := l.acquire("k")
	if wait2 == nil {
		t.Fatal("second job for the same key must wait for the first")
	}

	select {
	case <-wait2:
		t.Fatal("second job was released before the first finished")
	case <-time.After(20 * time.Millisecond):
	}

	release1()
	select {
	case <-wait2:
	case <-time.After(time.Second):
		t.Fatal("second job was never released after the first finished")
	}
	release2()

	if len(l.tails) != 0 {
		t.Errorf("expected lanes to be cleaned up, %d left", len(l.tails))
	}
}

func TestLanesDifferentKeysDontWait(t *testing.T) {
	l := newLanes()
	_, release := l.acquire("a")
	defer release()
	if wait, _ := l.acquire("b"); wait != nil {
		t.Fatal("a different key must not wait")
	}
}

func TestLanesEmptyKeyIsUnordered(t *testing.T) {
	l := newLanes()
	_, r1 := l.acquire("")
	defer r1()
	if wait, _ := l.acquire(""); wait != nil {
		t.Fatal("empty key means no ordering constraint")
	}
}
