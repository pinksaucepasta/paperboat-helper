package session

import (
	"errors"
	"testing"
)

func TestLifecyclePreservesFrozenDistinctions(t *testing.T) {
	l := NewLifecycle()
	if err := l.Transition(Running); err != nil {
		t.Fatal(err)
	}
	if state, generation := l.Snapshot(); state != Running || generation != 1 {
		t.Fatalf("state=%s generation=%d", state, generation)
	}
	if err := l.Transition(Deleted); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("delete running err=%v", err)
	}
	if err := l.Transition(Restarting); err != nil {
		t.Fatal(err)
	}
	if err := l.Transition(Running); err != nil {
		t.Fatal(err)
	}
	if _, generation := l.Snapshot(); generation != 2 {
		t.Fatalf("generation=%d", generation)
	}
}

func TestDetachChangesOnlyAttachment(t *testing.T) {
	l := NewLifecycle()
	l.Transition(Running)
	a := NewAttachment()
	a.Transition(Attached)
	if err := a.Transition(Detached); err != nil {
		t.Fatal(err)
	}
	if state, _ := l.Snapshot(); state != Running {
		t.Fatalf("session state=%s", state)
	}
	if err := a.Transition(Evicted); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal attachment err=%v", err)
	}
}
