//go:build darwin || linux

package session

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
)

func activityManager(t *testing.T, collector *activity.Collector, maxPending int) (*Manager, string, string) {
	t.Helper()
	root := t.TempDir()
	adapter, err := pty.NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(ManagerConfig{Launch: func(command pty.Command) (PTYProcess, error) { return adapter.Start(command) }, Random: bytes.NewReader(make([]byte, 64)), Activity: collector, EnvironmentID: "env_test", MaxPendingActivity: maxPending, TerminationTimeout: 3 * time.Second, TerminationGrace: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return manager, root, "/bin/sh"
}

func TestAcceptedInputCreatesOneTrustedActivityEvent(t *testing.T) {
	collector, err := activity.New(activity.Config{MaxQueued: 10})
	if err != nil {
		t.Fatal(err)
	}
	manager, root, shell := activityManager(t, collector, 10)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "default", Command: shellCommand(shell, root, "read line; read line")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att", 0); err != nil {
		t.Fatal(err)
	}
	key := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "inp_1"}
	if decision, err := manager.Write(created.ID, key, []byte("one\n")); err != nil || decision.Status != InputAccepted {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	if decision, err := manager.Write(created.ID, key, []byte("one\n")); err != nil || decision.Status != InputDuplicate {
		t.Fatalf("duplicate=%#v err=%v", decision, err)
	}
	batch, ok, err := collector.PeekBatch()
	if err != nil || !ok || len(batch.Events) != 1 || batch.Events[0].Source != activity.TerminalInput || batch.Events[0].SessionID != created.ID || batch.Events[0].Sequence != 1 {
		t.Fatalf("batch=%#v ok=%v err=%v", batch, ok, err)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func TestActivityPressureRejectsNextInputBeforePTYWrite(t *testing.T) {
	collector, err := activity.New(activity.Config{MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = collector.Record(activity.Event{EnvironmentID: "other", SourceID: "source", Source: activity.CLIActivity, Sequence: 1, OccurredAt: time.Now().UTC()}, true)
	if err != nil {
		t.Fatal(err)
	}
	manager, root, shell := activityManager(t, collector, 1)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "default", Command: shellCommand(shell, root, "read first; read second")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att", 0); err != nil {
		t.Fatal(err)
	}
	first := InputKey{ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "inp_1"}
	if _, err := manager.Write(created.ID, first, []byte("one\n")); err != nil {
		t.Fatal(err)
	}
	second := first
	second.InputID = "inp_2"
	if _, err := manager.Write(created.ID, second, []byte("two\n")); !errors.Is(err, activity.ErrQueueFull) {
		t.Fatalf("second input err=%v", err)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}
