package hosted

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type recordingRunner struct{ commands []Command }

func (r *recordingRunner) Run(_ context.Context, command Command) ([]byte, error) {
	r.commands = append(r.commands, command)
	if len(command.Args) > 0 && command.Args[0] == "clone" {
		return nil, os.MkdirAll(filepath.Join(command.Args[len(command.Args)-1], ".git"), 0o700)
	}
	if len(command.Args) > 1 && command.Args[0] == "remote" && command.Args[1] == "get-url" {
		return []byte("https://github.com/paperboat/example.git\n"), nil
	}
	return nil, nil
}

func testConfig(root string) Config {
	return Config{
		VolumeRoot: root, CheckoutRoot: filepath.Join(root, "project"), ProjectID: "prj_1",
		RepositoryURL: "https://github.com/paperboat/example.git", Branch: "main", AllowedRepositoryHosts: []string{"github.com"},
		GitPath: "/usr/bin/git", ShellPath: "/bin/sh", Presets: []Script{{Name: "codex", Body: "echo preset"}}, SetupScript: "echo setup",
		OperationTimeout: time.Second, FlushTimeout: time.Second, MaxScriptBytes: 4096, MaxOutputBytes: 4096,
	}
}

func TestLifecyclePreparesCheckoutRunsStagesAndFlushes(t *testing.T) {
	root := filepath.Join(t.TempDir(), "volume")
	runner := &recordingRunner{}
	var restored, flushed bool
	lifecycle, err := New(testConfig(root), Hooks{
		Restore: func(_ context.Context, checkout string) error {
			restored = checkout == filepath.Join(root, "project")
			return nil
		},
		Flush: func(_ context.Context, checkout string) error {
			flushed = checkout == filepath.Join(root, "project")
			return nil
		},
	}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snapshot := lifecycle.Snapshot(); !snapshot.Ready || snapshot.Stage != StageReady {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	if !restored || len(runner.commands) != 3 {
		t.Fatalf("restored=%v commands=%#v", restored, runner.commands)
	}
	if runner.commands[0].Args[0] != "clone" || runner.commands[1].Args[0] != "-eu" || runner.commands[2].Args[0] != "-eu" {
		t.Fatalf("commands=%#v", runner.commands)
	}
	if err := lifecycle.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !flushed {
		t.Fatal("pre-stop flush hook was not called")
	}
}

func TestLifecycleConfigRestoreFailureIsDegradedNotFatal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "volume")
	runner := &recordingRunner{}
	lifecycle, err := New(testConfig(root), Hooks{Restore: func(context.Context, string) error {
		return errors.New("restore unavailable")
	}}, runner)
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := lifecycle.Snapshot()
	if !snapshot.Ready || snapshot.Stage != StageReady || snapshot.ErrorCode != "stage_failed" {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	if len(runner.commands) != 3 {
		t.Fatalf("commands=%#v", runner.commands)
	}
}

func TestConfigSyncHooksRetainTokenForShutdownSubprocess(t *testing.T) {
	root := t.TempDir()
	checkout := filepath.Join(root, "project")
	if err := os.Mkdir(checkout, 0o700); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(root, "config-sync")
	if err := os.WriteFile(command, []byte("#!/bin/sh\n[ \"$PB_TEST_GITHUB_TOKEN\" = expected-secret ] && [ \"$1\" = save ]\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"PAPERBOAT_CONFIG_SYNC_COMMAND": command,
		"PAPERBOAT_GITHUB_TOKEN_ENV":    "PB_TEST_GITHUB_TOKEN",
		"PB_TEST_GITHUB_TOKEN":          "expected-secret",
	}
	config := testConfig(root)
	config.CheckoutRoot = checkout
	hooks := ConfigSyncHooks(config, func(name string) string { return values[name] })
	delete(values, "PB_TEST_GITHUB_TOKEN")
	if err := hooks.Flush(context.Background(), checkout); err != nil {
		t.Fatalf("shutdown config sync lost captured token: %v", err)
	}
}

func TestLifecycleRejectsWorkspaceIdentityMismatch(t *testing.T) {
	root := filepath.Join(t.TempDir(), "volume")
	first, err := New(testConfig(root), Hooks{}, &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	config := testConfig(root)
	config.ProjectID = "prj_other"
	second, err := New(config, Hooks{}, &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatalf("error=%v, want identity mismatch", err)
	}
}

func TestLifecycleRejectsRepositoryAndSymlinkVolume(t *testing.T) {
	config := testConfig(filepath.Join(t.TempDir(), "volume"))
	config.RepositoryURL = "https://github.example/owner/repo?token=secret"
	if _, err := New(config, Hooks{}, nil); !errors.Is(err, ErrRepository) {
		t.Fatalf("repository error=%v", err)
	}
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "volume")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := New(testConfig(link), Hooks{}, &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Start(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatalf("symlink error=%v", err)
	}
}

func TestLifecycleRejectsUnsafeBranchAndTamperedIdentity(t *testing.T) {
	config := testConfig(filepath.Join(t.TempDir(), "volume"))
	config.Branch = "--upload-pack=evil"
	if _, err := New(config, Hooks{}, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("branch error=%v", err)
	}
	root := filepath.Join(t.TempDir(), "volume")
	first, err := New(testConfig(root), Hooks{}, &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	identityPath := filepath.Join(root, ".paperboat", "workspace.json")
	if err := os.Chmod(identityPath, 0o644); err != nil {
		t.Fatal(err)
	}
	second, err := New(testConfig(root), Hooks{}, &recordingRunner{})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(context.Background()); !errors.Is(err, ErrIdentity) {
		t.Fatalf("identity permission error=%v", err)
	}
}

func TestExecRunnerBoundsOutputAndCancellation(t *testing.T) {
	runner := ExecRunner{}
	_, err := runner.Run(context.Background(), Command{Path: "/bin/sh", Args: []string{"-c", "printf 123456789"}, Dir: t.TempDir(), Env: []string{"PATH=" + os.Getenv("PATH")}, OutputLimit: 4})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("output error=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = runner.Run(ctx, Command{Path: "/bin/sh", Args: []string{"-c", "sleep 5"}, Dir: t.TempDir(), Env: []string{"PATH=" + os.Getenv("PATH")}, OutputLimit: 100})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("cancel error=%v", err)
	}
}
