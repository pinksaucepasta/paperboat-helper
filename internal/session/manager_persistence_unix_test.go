//go:build darwin || linux

package session

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

func TestManagerRecoversHistoryInputAndRestartGeneration(t *testing.T) {
	workspace := t.TempDir()
	stateRoot := filepath.Join(t.TempDir(), "state")
	adapter, err := pty.NewAdapter(workspace)
	if err != nil {
		t.Fatal(err)
	}
	open := func() *store.Store {
		state, err := store.Open(context.Background(), store.Config{Root: stateRoot})
		if err != nil {
			t.Fatal(err)
		}
		return state
	}
	newManager := func(state *store.Store, random []byte) *Manager {
		manager, err := NewManager(ManagerConfig{Store: state, Launch: func(command pty.Command) (PTYProcess, error) { return adapter.Start(command) }, Random: bytes.NewReader(random), HistoryBytes: 1 << 20, AttachmentBytes: 1 << 20, TerminationTimeout: 3 * time.Second, TerminationGrace: 100 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return manager
	}
	random := make([]byte, 64)
	for i := range random {
		random[i] = byte(i)
	}
	state := open()
	manager := newManager(state, random)
	command := shellCommand("/bin/sh", workspace, "printf abc; read line; printf done; exit 7")
	created, err := manager.Create(context.Background(), CreateRequest{Name: "default", Command: command})
	if err != nil {
		t.Fatal(err)
	}
	waitLatest(t, manager, created.ID, 3)
	if _, err := manager.Attach(created.ID, "att_1", 0); err != nil {
		t.Fatal(err)
	}
	key := InputKey{ClientID: "cli", AttachmentID: "att_1", Generation: created.Generation, InputID: "inp_0001"}
	if decision, err := manager.Write(created.ID, key, []byte("go\n")); err != nil || decision.Status != InputAccepted {
		t.Fatalf("decision=%#v err=%v", decision, err)
	}
	waitState(t, manager, created.ID, Exited)
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	state = open()
	defer state.Close()
	recoveredManager := newManager(state, random)
	recovered, err := recoveredManager.Snapshot(created.ID)
	if err != nil || recovered.State != Exited || recovered.Generation != 1 || recovered.Exit == nil || recovered.Exit.Code != 7 {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	attached, err := recoveredManager.Attach(created.ID, "att_2", 0)
	if err != nil || attached.Replay.ToSequence < 7 {
		t.Fatalf("attach=%#v err=%v", attached, err)
	}
	if decision, err := recoveredManager.QueryInput(created.ID, key); err != nil || decision.Status != InputAccepted {
		t.Fatalf("query=%#v err=%v", decision, err)
	}
	restarted, err := recoveredManager.Restart(created.ID)
	if err != nil || restarted.ID != created.ID || restarted.Generation != 2 {
		t.Fatalf("restart=%#v err=%v", restarted, err)
	}
	if _, err := recoveredManager.Close(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
}

func TestManagerMarksUnverifiedRunningGenerationLostOnRecovery(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "state")
	state, err := store.Open(context.Background(), store.Config{Root: stateRoot})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	record := store.Session{ID: "ses_lost", Name: "lost", CWD: t.TempDir(), CommandPath: "/bin/sh", CommandArgs: []string{"-c", "exit"}, CommandEnv: []string{"PATH=/bin"}, Columns: 80, Rows: 24, State: "running", Generation: 3, CreatedAt: now, UpdatedAt: now}
	if err := state.CreateSession(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = store.Open(context.Background(), store.Config{Root: stateRoot})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	manager, err := NewManager(ManagerConfig{Store: state, Launch: func(pty.Command) (PTYProcess, error) { t.Fatal("recovery must not launch"); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := manager.Snapshot("ses_lost")
	if err != nil || snapshot.State != Exited || snapshot.Generation != 3 || snapshot.Exit == nil || snapshot.Exit.Signal != "helper_restart" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}
