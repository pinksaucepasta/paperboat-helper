//go:build darwin || linux

package session

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

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
