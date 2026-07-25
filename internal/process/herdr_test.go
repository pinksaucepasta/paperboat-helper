package process

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type runner struct {
	output []byte
	err    error
	path   string
	args   []string
}

func (r *runner) Output(_ context.Context, path string, args ...string) ([]byte, error) {
	r.path = path
	r.args = append([]string(nil), args...)
	return r.output, r.err
}

type sessions struct {
	created  []session.CreateRequest
	closed   []string
	snapshot session.Snapshot
	err      error
}

func (s *sessions) Create(_ context.Context, request session.CreateRequest) (session.Snapshot, error) {
	s.created = append(s.created, request)
	if s.err != nil {
		return session.Snapshot{}, s.err
	}
	snapshot := s.snapshot
	snapshot.Name = request.Name
	return snapshot, nil
}
func (s *sessions) Close(_ context.Context, id string) (session.Snapshot, error) {
	s.closed = append(s.closed, id)
	return session.Snapshot{}, s.err
}

func TestSupervisorPinsVersionAndFixedInvocation(t *testing.T) {
	executable := testExecutable(t)
	runtime := &sessions{snapshot: session.Snapshot{ID: "ses_1"}}
	check := &runner{output: []byte("herdr 0.7.4\n")}
	stateRoot := filepath.Join(t.TempDir(), "herdr")
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", Environment: []string{"PATH=/bin", "TERM=xterm"}, StateRoot: stateRoot, Sessions: runtime, Runner: check})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := supervisor.Launch(context.Background(), LaunchRequest{ID: "ses_server_1", Name: "default", CWD: t.TempDir(), Dimensions: pty.Dimensions{Columns: 80, Rows: 24}})
	if err != nil || snapshot.ID != "ses_1" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	command := runtime.created[0].Command
	if runtime.created[0].ID != "ses_server_1" {
		t.Fatalf("session id=%q", runtime.created[0].ID)
	}
	if command.Path != executable || len(command.Args) != 1 || command.Args[0] != "--no-session" {
		t.Fatalf("command=%#v", command)
	}
	if got := environmentValue(command.Env, "XDG_CONFIG_HOME"); got != filepath.Join(stateRoot, "ses_server_1") {
		t.Fatalf("XDG_CONFIG_HOME=%q", got)
	}
	socketDirectory := herdrSocketDirectory(stateRoot, "ses_server_1")
	if got := environmentValue(command.Env, "HERDR_SOCKET_PATH"); got != filepath.Join(socketDirectory, "herdr.sock") {
		t.Fatalf("HERDR_SOCKET_PATH=%q", got)
	}
	if got := environmentValue(command.Env, "HERDR_CLIENT_SOCKET_PATH"); got != filepath.Join(socketDirectory, "client.sock") {
		t.Fatalf("HERDR_CLIENT_SOCKET_PATH=%q", got)
	}
	if err := supervisor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorRejectsVersionAndDuplicateLaunch(t *testing.T) {
	executable := testExecutable(t)
	runtime := &sessions{snapshot: session.Snapshot{ID: "ses_1"}}
	stateRoot := filepath.Join(t.TempDir(), "herdr")
	if _, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", StateRoot: stateRoot, Sessions: runtime, Runner: &runner{output: []byte("herdr 0.8.0\n")}}); !errors.Is(err, ErrVersionIncompatible) {
		t.Fatalf("version err=%v", err)
	}
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", StateRoot: stateRoot, Sessions: runtime, Runner: &runner{output: []byte("herdr 0.7.4\n")}})
	if err != nil {
		t.Fatal(err)
	}
	request := LaunchRequest{ID: "ses_1", Name: "default", CWD: t.TempDir(), Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}
	if _, err := supervisor.Launch(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	runtime.err = session.ErrSessionExists
	if _, err := supervisor.Launch(context.Background(), request); !errors.Is(err, session.ErrSessionExists) {
		t.Fatalf("session error=%v", err)
	}
}

func TestSupervisorRejectsLoaderEnvironment(t *testing.T) {
	_, err := NewSupervisor(context.Background(), Config{Executable: testExecutable(t), ExpectedVersion: "0.7.4", Environment: []string{"LD_PRELOAD=/tmp/injected.so"}, StateRoot: filepath.Join(t.TempDir(), "herdr"), Sessions: &sessions{}, Runner: &runner{output: []byte("herdr 0.7.4\n")}})
	if !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("err=%v", err)
	}
}

func TestSupervisorUsesBoundedSocketPathForLongStateRoot(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), strings.Repeat("long", 30))
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: testExecutable(t), ExpectedVersion: "0.7.4", StateRoot: stateRoot, Sessions: &sessions{}, Runner: &runner{output: []byte("herdr 0.7.4\n")}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = supervisor.Launch(context.Background(), LaunchRequest{ID: "ses_1", CWD: t.TempDir()})
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	socketPath := environmentValue(supervisor.config.Sessions.(*sessions).created[0].Command.Env, "HERDR_SOCKET_PATH")
	if len(socketPath) > maxUnixSocketPathBytes || !strings.HasPrefix(socketPath, "/tmp/paperboat-herdr-") {
		t.Fatalf("socket path=%q", socketPath)
	}
}

func herdrSocketDirectory(stateRoot, sessionID string) string {
	digest := sha256.Sum256([]byte(stateRoot + "\x00" + sessionID))
	return filepath.Join("/tmp", fmt.Sprintf("paperboat-herdr-%d", os.Getuid()), fmt.Sprintf("%x", digest[:12]))
}

func TestSupervisorAllowsOnlyExplicitAgentIntegrationEnvironment(t *testing.T) {
	_, err := NewSupervisor(context.Background(), Config{
		Executable: testExecutable(t), ExpectedVersion: "0.7.4", Sessions: &sessions{},
		Environment: []string{
			"PAPERBOAT_PREVIEW_REGISTRATION_ENDPOINT=http://127.0.0.1:8080/v1/preview-registrations",
			"PAPERBOAT_HELPER_AGENT_TOKEN_FILE=/state/agent/token",
		},
		StateRoot: filepath.Join(t.TempDir(), "herdr"),
		Runner:    &runner{output: []byte("herdr 0.7.4\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorIsolatesSessionState(t *testing.T) {
	stateRoot := filepath.Join(t.TempDir(), "herdr")
	runtime := &sessions{}
	supervisor, err := NewSupervisor(context.Background(), Config{
		Executable: testExecutable(t), ExpectedVersion: "0.7.4", StateRoot: stateRoot,
		Environment: []string{"PATH=/bin", "XDG_CONFIG_HOME=/shared"}, Sessions: runtime,
		Runner: &runner{output: []byte("herdr 0.7.4\n")},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"ses_one", "ses_two"} {
		if _, err := supervisor.Launch(context.Background(), LaunchRequest{ID: id, Name: id, CWD: t.TempDir(), Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}); err != nil {
			t.Fatal(err)
		}
	}
	for index, id := range []string{"ses_one", "ses_two"} {
		command := runtime.created[index].Command
		if got := environmentValue(command.Env, "XDG_CONFIG_HOME"); got != filepath.Join(stateRoot, id) {
			t.Fatalf("session %s XDG_CONFIG_HOME=%q", id, got)
		}
		if countEnvironment(command.Env, "XDG_CONFIG_HOME") != 1 {
			t.Fatalf("session %s environment=%#v", id, command.Env)
		}
		if info, err := os.Stat(filepath.Join(stateRoot, id)); err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("session %s state info=%#v err=%v", id, info, err)
		}
	}
	if _, err := supervisor.Launch(context.Background(), LaunchRequest{ID: "../escape"}); !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("traversal err=%v", err)
	}
}

func environmentValue(environment []string, key string) string {
	for _, entry := range environment {
		entryKey, value, _ := strings.Cut(entry, "=")
		if entryKey == key {
			return value
		}
	}
	return ""
}

func countEnvironment(environment []string, key string) int {
	count := 0
	for _, entry := range environment {
		entryKey, _, _ := strings.Cut(entry, "=")
		if entryKey == key {
			count++
		}
	}
	return count
}

func testExecutable(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "herdr-test")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o500); err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}
