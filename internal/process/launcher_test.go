package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type recordingLauncher struct{ calls []LaunchRequest }

func (l *recordingLauncher) Launch(_ context.Context, request LaunchRequest) (session.Snapshot, error) {
	l.calls = append(l.calls, request)
	return session.Snapshot{ID: request.ID}, nil
}

func TestModeLauncherSelectsValidatedShell(t *testing.T) {
	shellPath := filepath.Join(t.TempDir(), "shell")
	if err := os.WriteFile(shellPath, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedShellPath, err := filepath.EvalSymlinks(shellPath)
	if err != nil {
		t.Fatal(err)
	}
	herdr := &recordingLauncher{}
	runtime := &sessions{}
	launcher, err := NewModeLauncher(herdr, shellPath, []string{"PATH=/usr/bin:/bin", "SHELL=" + shellPath, "TERM=xterm"}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	dimensions := pty.Dimensions{Columns: 100, Rows: 40}
	_, err = launcher.Launch(context.Background(), LaunchRequest{ID: "ses_shell", Name: "benchmark", CWD: "/tmp", Dimensions: dimensions, Mode: "shell", Environment: map[string]string{"TERM": "xterm-ghostty", "COLORTERM": "truecolor"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(herdr.calls) != 0 || len(runtime.created) != 1 {
		t.Fatalf("herdr calls=%d shell creates=%d", len(herdr.calls), len(runtime.created))
	}
	command := runtime.created[0].Command
	if command.Path != resolvedShellPath || len(command.Args) != 1 || command.Args[0] != "-l" || command.CWD != "/tmp" || command.Dimensions != dimensions {
		t.Fatalf("command = %#v", command)
	}
	if environmentValue(command.Env, "TERM") != "xterm-ghostty" || environmentValue(command.Env, "COLORTERM") != "truecolor" {
		t.Fatalf("environment = %#v", command.Env)
	}
	if _, err := launcher.Launch(context.Background(), LaunchRequest{ID: "ses_env", Mode: "shell", Environment: map[string]string{"LD_PRELOAD": "/tmp/injected.so"}}); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("unsafe environment err = %v", err)
	}
	if _, err := launcher.Launch(context.Background(), LaunchRequest{ID: "ses_bad", Mode: "other"}); !errors.Is(err, ErrTerminalModeInvalid) {
		t.Fatalf("err = %v", err)
	}
}
