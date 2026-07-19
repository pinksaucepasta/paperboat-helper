package store

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func openStore(t *testing.T, hook func(string) error) (*Store, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "state")
	store, err := Open(context.Background(), Config{Root: root, FailureHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	return store, root
}

func testSession() Session {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Session{ID: "ses_1", Name: "default", CWD: "/workspace", Columns: 80, Rows: 24, State: "running", Generation: 1, CreatedAt: now, UpdatedAt: now}
}

func TestStorePersistsSessionAndOutputAcrossReopen(t *testing.T) {
	store, root := openStore(t, nil)
	if err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	if _, earliest, err := store.AppendOutput(context.Background(), "ses_1", 1, 0, []byte("abc"), 64); err != nil || earliest != 0 {
		t.Fatalf("earliest=%d err=%v", earliest, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sessions, err := store.Sessions(context.Background())
	if err != nil || len(sessions) != 1 || sessions[0].LatestSequence != 3 {
		t.Fatalf("sessions=%#v err=%v", sessions, err)
	}
	events, earliest, latest, err := store.Replay(context.Background(), "ses_1", 1, 2)
	if err != nil || earliest != 0 || latest != 3 || len(events) != 1 || string(events[0].Data) != "bc" {
		t.Fatalf("events=%#v bounds=(%d,%d) err=%v", events, earliest, latest, err)
	}
	info, err := os.Stat(filepath.Join(root, "state.db"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestAppendCompactsWholeEventsAndRollsBackInjectedFailure(t *testing.T) {
	fail := false
	store, _ := openStore(t, func(point string) error {
		if fail && point == "append_before_commit" {
			return errors.New("injected")
		}
		return nil
	})
	defer store.Close()
	if err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	store.AppendOutput(context.Background(), "ses_1", 1, 0, []byte("abc"), 5)
	_, earliest, err := store.AppendOutput(context.Background(), "ses_1", 2, 3, []byte("def"), 5)
	if err != nil || earliest != 3 {
		t.Fatalf("earliest=%d err=%v", earliest, err)
	}
	if _, _, _, err := store.Replay(context.Background(), "ses_1", 2, 0); !errors.Is(err, ErrReplayGap) {
		t.Fatalf("gap err=%v", err)
	}
	fail = true
	if _, _, err := store.AppendOutput(context.Background(), "ses_1", 1, 6, []byte("x"), 5); err == nil {
		t.Fatal("expected injected failure")
	}
	fail = false
	sessions, _ := store.Sessions(context.Background())
	if sessions[0].LatestSequence != 6 {
		t.Fatalf("latest=%d", sessions[0].LatestSequence)
	}
}

func TestInputAndOperationConflictsSurviveUniqueness(t *testing.T) {
	store, _ := openStore(t, nil)
	defer store.Close()
	if err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	decision := InputDecision{SessionID: "ses_1", ClientID: "cli", AttachmentID: "att", Generation: 1, InputID: "inp", Hash: []byte("a"), Status: "accepted", BytesWritten: 1, CreatedAt: time.Now()}
	if inserted, err := store.PutInputDecision(context.Background(), decision); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if inserted, err := store.PutInputDecision(context.Background(), decision); err != nil || inserted {
		t.Fatalf("duplicate inserted=%v err=%v", inserted, err)
	}
	decision.Hash = []byte("b")
	if _, err := store.PutInputDecision(context.Background(), decision); !errors.Is(err, ErrConflict) {
		t.Fatalf("input conflict=%v", err)
	}
	now := time.Now()
	operation := OperationResult{OperationID: "op_00000001", RequestHash: []byte("a"), Result: []byte(`{"ok":true}`), CompletedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := store.PutOperation(context.Background(), operation); err != nil {
		t.Fatal(err)
	}
	operation.RequestHash = []byte("b")
	if err := store.PutOperation(context.Background(), operation); !errors.Is(err, ErrConflict) {
		t.Fatalf("operation conflict=%v", err)
	}
}

func TestStoreRejectsNewerVersionAndSymlink(t *testing.T) {
	store, root := openStore(t, nil)
	if _, err := store.db.Exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Config{Root: root}); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("version err=%v", err)
	}
	symlinkRoot := filepath.Join(t.TempDir(), "linked")
	if err := os.Symlink(t.TempDir(), symlinkRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Config{Root: symlinkRoot}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("symlink err=%v", err)
	}
}

func TestStoreRejectsCorruptDatabaseWithTypedError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "state.db"), []byte("not a sqlite database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Config{Root: root}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt err=%v", err)
	}
}

func TestInterruptedInitialMigrationRollsBackAndReopensCleanly(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	injected := errors.New("migration interrupted")
	if _, err := Open(context.Background(), Config{Root: root, FailureHook: func(point string) error {
		if point == "migration_before_commit" {
			return injected
		}
		return nil
	}}); !errors.Is(err, injected) {
		t.Fatalf("err=%v", err)
	}
	store, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != CurrentVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	if err := store.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	sessions, err := store.Sessions(context.Background())
	if err != nil || len(sessions) != 1 {
		t.Fatalf("sessions=%#v err=%v", sessions, err)
	}
}

func TestProcessCrashBeforeMigrationCommitReopensCleanly(t *testing.T) {
	if os.Getenv("PAPERBOAT_STORE_CRASH_MODE") == "migration" {
		crashStoreAt(t, "migration_before_commit")
	}
	root := filepath.Join(t.TempDir(), "state")
	runCrashChild(t, "TestProcessCrashBeforeMigrationCommitReopensCleanly", "migration", root)

	state, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var version int
	if err := state.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != CurrentVersion {
		t.Fatalf("version=%d err=%v", version, err)
	}
	if err := state.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
}

func TestProcessCrashBeforeAppendCommitPreservesAcknowledgedSequence(t *testing.T) {
	if os.Getenv("PAPERBOAT_STORE_CRASH_MODE") == "append" {
		crashStoreAt(t, "append_before_commit")
	}
	state, root := openStore(t, nil)
	if err := state.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.AppendOutput(context.Background(), "ses_1", 1, 0, []byte("old"), 64); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}

	runCrashChild(t, "TestProcessCrashBeforeAppendCommitPreservesAcknowledgedSequence", "append", root)

	state, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	sessions, err := state.Sessions(context.Background())
	if err != nil || len(sessions) != 1 || sessions[0].LatestSequence != 3 {
		t.Fatalf("sessions=%#v err=%v", sessions, err)
	}
	events, earliest, latest, err := state.Replay(context.Background(), "ses_1", 0, 0)
	if err != nil || earliest != 0 || latest != 3 || len(events) != 1 || string(events[0].Data) != "old" {
		t.Fatalf("events=%#v bounds=(%d,%d) err=%v", events, earliest, latest, err)
	}
}

func runCrashChild(t *testing.T, testName, mode, root string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^"+testName+"$")
	command.Env = append(os.Environ(), "PAPERBOAT_STORE_CRASH_MODE="+mode, "PAPERBOAT_STORE_CRASH_ROOT="+root)
	err := command.Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 91 {
		t.Fatalf("crash child mode=%s err=%v", mode, err)
	}
}

func crashStoreAt(t *testing.T, crashPoint string) {
	t.Helper()
	root := os.Getenv("PAPERBOAT_STORE_CRASH_ROOT")
	hook := func(point string) error {
		if point == crashPoint {
			os.Exit(91)
		}
		return nil
	}
	state, err := Open(context.Background(), Config{Root: root, FailureHook: hook})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	if crashPoint == "append_before_commit" {
		if _, _, err := state.AppendOutput(context.Background(), "ses_1", 1, 3, []byte("new"), 64); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("crash point %q was not reached", crashPoint)
}
