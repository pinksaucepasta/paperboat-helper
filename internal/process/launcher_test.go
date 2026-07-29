package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type sessions struct {
	created []session.CreateRequest
}

func (s *sessions) Create(_ context.Context, request session.CreateRequest) (session.Snapshot, error) {
	s.created = append(s.created, request)
	return session.Snapshot{ID: request.ID}, nil
}

func (*sessions) Close(_ context.Context, id string) (session.Snapshot, error) {
	return session.Snapshot{ID: id}, nil
}

func environmentValue(environment []string, key string) string {
	for _, entry := range environment {
		if name, value, ok := strings.Cut(entry, "="); ok && name == key {
			return value
		}
	}
	return ""
}

func TestShellLauncherStartsValidatedLoginShell(t *testing.T) {
	shellPath := filepath.Join(t.TempDir(), "shell")
	if err := os.WriteFile(shellPath, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	resolvedShellPath, err := filepath.EvalSymlinks(shellPath)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &sessions{}
	launcher, err := NewShellLauncher(shellPath, []string{"PATH=/usr/bin:/bin", "SHELL=" + shellPath, "TERM=xterm"}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	dimensions := pty.Dimensions{Columns: 100, Rows: 40}
	_, err = launcher.Launch(context.Background(), LaunchRequest{ID: "ses_shell", Name: "benchmark", CWD: "/tmp", Dimensions: dimensions, Environment: map[string]string{"TERM": "xterm-ghostty", "COLORTERM": "truecolor"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(runtime.created) != 1 {
		t.Fatalf("shell creates=%d", len(runtime.created))
	}
	command := runtime.created[0].Command
	if command.Path != resolvedShellPath || len(command.Args) != 1 || command.Args[0] != "-l" || command.CWD != "/tmp" || command.Dimensions != dimensions {
		t.Fatalf("command = %#v", command)
	}
	if environmentValue(command.Env, "TERM") != "xterm-ghostty" || environmentValue(command.Env, "COLORTERM") != "truecolor" {
		t.Fatalf("environment = %#v", command.Env)
	}
	if _, err := launcher.Launch(context.Background(), LaunchRequest{ID: "ses_env", Environment: map[string]string{"LD_PRELOAD": "/tmp/injected.so"}}); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("unsafe environment err = %v", err)
	}
}
