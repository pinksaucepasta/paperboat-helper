//go:build darwin || linux

package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/history"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
)

func realManager(t *testing.T) (*Manager, string, string) {
	t.Helper()
	root := t.TempDir()
	adapter, err := pty.NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	randomBytes := make([]byte, 128)
	for i := range randomBytes {
		randomBytes[i] = byte(i)
	}
	manager, err := NewManager(ManagerConfig{
		Launch:             func(command pty.Command) (PTYProcess, error) { return adapter.Start(command) },
		Random:             bytes.NewReader(randomBytes),
		HistoryBytes:       1 << 20,
		AttachmentBytes:    1 << 20,
		TerminationTimeout: 3 * time.Second,
		TerminationGrace:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	shell := "/bin/sh"
	if _, err := os.Stat(shell); err != nil {
		shell = "/usr/bin/sh"
	}
	return manager, root, shell
}

func shellCommand(shell, root, script string) pty.Command {
	return pty.Command{Path: shell, Args: []string{"-c", script}, Env: []string{"PATH=/usr/bin:/bin", "TERM=xterm"}, CWD: root, Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}
}

func TestManagerMirrorsReplayAndDeduplicatesInput(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "default", Command: shellCommand(shell, root, "printf ready; read line; printf done; read line")})
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(t, manager, created.ID, 5)
	first, err := manager.Attach(created.ID, "att_1", 0)
	if err != nil || first.Replay.ToSequence < 5 {
		t.Fatalf("attach=%#v err=%v", first, err)
	}
	second, err := manager.Attach(created.ID, "att_2", 0)
	if err != nil || second.Replay.ToSequence < 5 {
		t.Fatalf("attach=%#v err=%v", second, err)
	}
	key := InputKey{ClientID: "cli", AttachmentID: "att_1", Generation: created.Generation, InputID: "inp_0001"}
	decision, err := manager.Write(created.ID, key, []byte("go\n"))
	if err != nil || decision.Status != InputAccepted {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	decision, err = manager.Write(created.ID, key, []byte("go\n"))
	if err != nil || decision.Status != InputDuplicate {
		t.Fatalf("duplicate=%#v err=%v", decision, err)
	}
	for _, attachmentID := range []string{"att_1", "att_2"} {
		output := collectUntil(t, manager, created.ID, attachmentID, "done")
		if !strings.Contains(output, "ready") || !strings.Contains(output, "done") {
			t.Fatalf("%s output=%q", attachmentID, output)
		}
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerStreamInputPreservesBytesWithoutIdempotencyRows(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "stream-input", Command: shellCommand(shell, root, "read line; printf 'got:%s' \"$line\"; read line")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.AttachLive(created.ID, "att_stream")
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.WriteStream(created.ID, "att_stream", created.Generation, []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	output := collectUntil(t, manager, created.ID, "att_stream", "got:hello")
	if !strings.Contains(output, "got:hello") {
		t.Fatalf("output=%q", output)
	}
	session, err := manager.get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	session.inputs.mu.Lock()
	decisionCount := len(session.inputs.decisions)
	session.inputs.mu.Unlock()
	if decisionCount != 0 {
		t.Fatalf("stream input created %d idempotency decisions", decisionCount)
	}
	_, _ = manager.Close(context.Background(), created.ID)
}

func TestManagerStreamInputDoesNotWaitForLifecycleLock(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "unlocked-input", Command: shellCommand(shell, root, "read line; read line")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att", 0); err != nil {
		t.Fatal(err)
	}
	session, err := manager.get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	session.opMu.Lock()
	done := make(chan error, 1)
	go func() {
		done <- manager.WriteStream(created.ID, "att", created.Generation, []byte("one\n"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("stream input waited for lifecycle lock")
	}
	session.opMu.Unlock()
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerAttachUsesLatestWindowWhenReplayExceedsQueue(t *testing.T) {
	manager, root, shell := realManager(t)
	manager.config.AttachmentBytes = 8
	created, err := manager.Create(context.Background(), CreateRequest{
		Name:    "bounded-replay",
		Command: shellCommand(shell, root, "printf 0123456789abcdef; read line"),
	})
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(t, manager, created.ID, 16)

	attached, err := manager.Attach(created.ID, "att_tail", 0)
	if err != nil {
		t.Fatal(err)
	}
	if attached.Replay.FromSequence != attached.Replay.LatestSequence-4 || attached.Replay.ToSequence != attached.Replay.LatestSequence {
		t.Fatalf("attach=%#v", attached)
	}
	session, err := manager.get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if evictions, err := session.fanout.Publish(history.Event{Channel: 1, StartSequence: attached.Replay.LatestSequence, EndSequence: attached.Replay.LatestSequence + 4, Data: []byte("live")}); err != nil || len(evictions) != 0 {
		t.Fatalf("publish evictions=%#v err=%v", evictions, err)
	}
	if state, queued, err := session.fanout.Status("att_tail"); err != nil || state != Attached || queued != 8 {
		t.Fatalf("attachment state=%s queued=%d err=%v", state, queued, err)
	}
}

func TestManagerAttachLiveCannotRaceContinuousCompaction(t *testing.T) {
	retained, err := history.New(64 << 10)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := NewLifecycle()
	if err := lifecycle.Transition(Running); err != nil {
		t.Fatal(err)
	}
	session := &managedSession{id: "ses_hot", name: "hot", lifecycle: lifecycle, history: retained, fanout: NewFanout()}
	manager := &Manager{config: ManagerConfig{AttachmentBytes: 1 << 20, MaxAttachments: 16}, sessions: map[string]*managedSession{"ses_hot": session}, names: map[string]string{"hot": "ses_hot"}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		chunk := bytes.Repeat([]byte("x"), 32<<10)
		for i := 0; i < 500; i++ {
			session.opMu.Lock()
			_, _ = session.history.Append(1, chunk)
			session.opMu.Unlock()
		}
	}()
	for i := 0; i < 200; i++ {
		attachmentID := fmt.Sprintf("att_%d", i)
		attached, err := manager.AttachLive("ses_hot", attachmentID)
		if err != nil {
			t.Fatalf("attach %d: %v", i, err)
		}
		if attached.Replay.FromSequence != attached.Replay.LatestSequence || attached.Replay.ToSequence != attached.Replay.LatestSequence || len(attached.Replay.Events) != 0 {
			t.Fatalf("attach %d replay=%#v", i, attached.Replay)
		}
		if err := manager.Detach("ses_hot", attachmentID); err != nil {
			t.Fatal(err)
		}
	}
	<-done
}

func TestManagerClearRestartCloseAndDeleteRemainDistinct(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "named", Command: shellCommand(shell, root, "printf abc; read line")})
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(t, manager, created.ID, 3)
	before, err := manager.Snapshot(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	next, err := manager.Clear(created.ID)
	if err != nil || next != before.LatestSequence {
		t.Fatalf("clear=%d before=%#v err=%v", next, before, err)
	}
	after, _ := manager.Snapshot(created.ID)
	if after.State != Running || after.EarliestSequence != next || after.LatestSequence != next {
		t.Fatalf("after clear=%#v", after)
	}
	closed, err := manager.Close(context.Background(), created.ID)
	if err != nil || closed.State != Closed {
		t.Fatalf("closed=%#v err=%v", closed, err)
	}
	restarted, err := manager.Restart(created.ID)
	if err != nil || restarted.ID != created.ID || restarted.Generation != created.Generation+1 || restarted.State != Running {
		t.Fatalf("restarted=%#v err=%v", restarted, err)
	}
	if err := manager.Delete(created.ID); !errors.Is(err, ErrSessionRunning) {
		t.Fatalf("delete running err=%v", err)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	if err := manager.Delete(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Snapshot(created.ID); !errors.Is(err, ErrSessionUnknown) {
		t.Fatalf("deleted lookup err=%v", err)
	}
}

func TestManagerReportsNaturalExit(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "exit", Command: shellCommand(shell, root, "exit 7")})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := waitState(t, manager, created.ID, Exited)
	if snapshot.Exit == nil || snapshot.Exit.Code != 7 {
		t.Fatalf("snapshot=%#v", snapshot)
	}
}

func TestCloseHonorsCanceledOperationBeforeMutation(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "cancel-close", Command: shellCommand(shell, root, "read line")})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Close(ctx, created.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("close err=%v", err)
	}
	snapshot, err := manager.Snapshot(created.ID)
	if err != nil || snapshot.State != Running {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func TestCloseDeadlineRetainsProcessOwnershipUntilCleanup(t *testing.T) {
	manager, root, shell := realManager(t)
	created, err := manager.Create(context.Background(), CreateRequest{Name: "deadline-close", Command: shellCommand(shell, root, "trap '' TERM; echo ready; while :; do sleep 1; done")})
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(t, manager, created.ID, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	snapshot, err := manager.Close(ctx, created.ID)
	if !errors.Is(err, context.DeadlineExceeded) || snapshot.State != Closing {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	closed := waitState(t, manager, created.ID, Closed)
	if closed.Exit == nil {
		t.Fatalf("cleanup lost exit result: %#v", closed)
	}
}

func TestManagerListsDeterministicallyAndShutdownStopsAdmission(t *testing.T) {
	manager, root, shell := realManager(t)
	for _, name := range []string{"zeta", "alpha"} {
		if _, err := manager.Create(context.Background(), CreateRequest{Name: name, Command: shellCommand(shell, root, "read line")}); err != nil {
			t.Fatal(err)
		}
	}
	listed := manager.List()
	if len(listed) != 2 || listed[0].Name != "alpha" || listed[1].Name != "zeta" {
		t.Fatalf("listed=%#v", listed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, snapshot := range manager.List() {
		if snapshot.State != Closed {
			t.Fatalf("snapshot=%#v", snapshot)
		}
	}
	if _, err := manager.Create(context.Background(), CreateRequest{Name: "later", Command: shellCommand(shell, root, "exit")}); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("create after shutdown err=%v", err)
	}
}

func TestManagerAttachmentLimitRejectsAndDetachReleasesCapacity(t *testing.T) {
	root := t.TempDir()
	adapter, err := pty.NewAdapter(root)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewManager(ManagerConfig{Launch: func(command pty.Command) (PTYProcess, error) { return adapter.Start(command) }, MaxAttachments: 1, MaxInputDecisions: 1})
	if err != nil {
		t.Fatal(err)
	}
	created, err := manager.Create(context.Background(), CreateRequest{Name: "bounded", Command: shellCommand("/bin/sh", root, "read line")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att_1", 0); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att_2", 0); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("limit err=%v", err)
	}
	if err := manager.Detach(created.ID, "att_1"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Attach(created.ID, "att_2", 0); err != nil {
		t.Fatalf("post-detach attach=%v", err)
	}
	if _, err := manager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func waitLatest(t *testing.T, manager *Manager, sessionID string, minimum uint64) Snapshot {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := manager.Snapshot(sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.LatestSequence >= minimum {
			return snapshot
		}
		select {
		case <-deadline.C:
			t.Fatalf("latest sequence remained below %d", minimum)
		case <-ticker.C:
		}
	}
}

func waitState(t *testing.T, manager *Manager, sessionID string, wanted State) Snapshot {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshot, err := manager.Snapshot(sessionID)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.State == wanted {
			return snapshot
		}
		select {
		case <-deadline.C:
			t.Fatalf("state=%s want=%s", snapshot.State, wanted)
		case <-ticker.C:
		}
	}
}

func collectUntil(t *testing.T, manager *Manager, sessionID, attachmentID, marker string) string {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	var output strings.Builder
	for {
		event, ok, err := manager.Next(sessionID, attachmentID)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			output.Write(event.Data)
			if strings.Contains(output.String(), marker) {
				return output.String()
			}
			continue
		}
		select {
		case <-deadline.C:
			t.Fatalf("output=%q missing %q", output.String(), marker)
		case <-ticker.C:
		}
	}
}
