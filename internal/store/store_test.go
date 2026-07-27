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

func openStore(t testing.TB, hook func(string) error) (*Store, string) {
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

func BenchmarkPersistenceFlush32KiB(b *testing.B) {
	state, _ := openStore(b, nil)
	b.Cleanup(func() { _ = state.Close() })
	if err := state.CreateSession(context.Background(), testSession()); err != nil {
		b.Fatal(err)
	}
	payload := make([]byte, 32<<10)
	var sequence uint64
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, _, err := state.AppendOutput(context.Background(), "ses_1", 1, sequence, payload, 1<<20); err != nil {
			b.Fatal(err)
		}
		sequence += uint64(len(payload))
	}
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

func TestTrimOutputCompactsExistingHistory(t *testing.T) {
	state, _ := openStore(t, nil)
	defer state.Close()
	if err := state.CreateSession(context.Background(), testSession()); err != nil {
		t.Fatal(err)
	}
	for sequence, data := range [][]byte{[]byte("abc"), []byte("def"), []byte("ghi")} {
		if _, _, err := state.AppendOutput(context.Background(), "ses_1", 1, uint64(sequence*3), data, 64); err != nil {
			t.Fatal(err)
		}
	}
	earliest, err := state.TrimOutput(context.Background(), "ses_1", 4)
	if err != nil || earliest != 6 {
		t.Fatalf("earliest=%d err=%v", earliest, err)
	}
	events, gotEarliest, latest, err := state.Replay(context.Background(), "ses_1", 6, 0)
	if err != nil || gotEarliest != 6 || latest != 9 || len(events) != 1 || string(events[0].Data) != "ghi" {
		t.Fatalf("events=%#v bounds=(%d,%d) err=%v", events, gotEarliest, latest, err)
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

func TestOperationResultsAreBoundedAndExpiredRowsAreDeleted(t *testing.T) {
	state, _ := openStore(t, nil)
	defer state.Close()
	now := time.Now().UTC()
	expired := OperationResult{OperationID: "op_expired_0001", RequestHash: []byte("a"), Result: []byte(`{"ok":true}`), CompletedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)}
	if err := state.PutOperation(context.Background(), expired); err != nil {
		t.Fatal(err)
	}
	if _, inserted, err := state.ReserveOperation(context.Background(), "op_pending_0001", []byte("b"), now.Add(-time.Minute)); err != nil || !inserted {
		t.Fatalf("pending inserted=%v err=%v", inserted, err)
	}
	if err := state.DeleteExpiredOperations(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := state.db.QueryRow(`SELECT count(*) FROM operation_results`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	large := OperationResult{OperationID: "op_large_0001", RequestHash: []byte("c"), Result: make([]byte, MaxOperationResultBytes+1), CompletedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := state.PutOperation(context.Background(), large); !errors.Is(err, ErrResultTooLarge) {
		t.Fatalf("large result err=%v", err)
	}
}

func TestOperationsStripLegacyOversizedResultsWithoutReleasingReservation(t *testing.T) {
	state, _ := openStore(t, nil)
	defer state.Close()
	now := time.Now().UTC()
	result := make([]byte, MaxOperationResultBytes+1)
	if _, err := state.db.Exec(`INSERT INTO operation_results(operation_id,request_hash,state,result,completed_at,expires_at) VALUES(?,?,'completed',?,?,?)`, "op_legacy_large", []byte("hash"), result, now.UnixNano(), now.Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	records, err := state.Operations(context.Background(), now, 8)
	if err != nil || len(records) != 1 || records[0].State != "pending" || len(records[0].Result) != 0 || !records[0].CompletedAt.IsZero() {
		t.Fatalf("records=%#v err=%v", records, err)
	}
	var count int
	if err := state.db.QueryRow(`SELECT count(*) FROM operation_results WHERE operation_id='op_legacy_large' AND state='pending' AND result IS NULL`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
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

func TestProcessCrashAfterAppendCommitRecoversCommittedSequence(t *testing.T) {
	if os.Getenv("PAPERBOAT_STORE_CRASH_MODE") == "append_after_commit" {
		crashStoreAt(t, "append_after_commit")
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

	runCrashChild(t, "TestProcessCrashAfterAppendCommitRecoversCommittedSequence", "append_after_commit", root)

	state, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	events, earliest, latest, err := state.Replay(context.Background(), "ses_1", 0, 0)
	if err != nil || earliest != 0 || latest != 6 || len(events) != 2 || string(events[0].Data) != "old" || string(events[1].Data) != "new" {
		t.Fatalf("events=%#v bounds=(%d,%d) err=%v", events, earliest, latest, err)
	}
}

func TestProcessCrashDuringAppendCompactionRollsBackAtomically(t *testing.T) {
	if os.Getenv("PAPERBOAT_STORE_CRASH_MODE") == "compaction" {
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

	runCrashChild(t, "TestProcessCrashDuringAppendCompactionRollsBackAtomically", "compaction", root)

	state, err := Open(context.Background(), Config{Root: root})
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
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
	if crashPoint == "append_before_commit" || crashPoint == "append_after_commit" {
		maxRetained := uint64(64)
		if os.Getenv("PAPERBOAT_STORE_CRASH_MODE") == "compaction" {
			maxRetained = 3
		}
		if _, _, err := state.AppendOutput(context.Background(), "ses_1", 1, 3, []byte("new"), maxRetained); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("crash point %q was not reached", crashPoint)
}
