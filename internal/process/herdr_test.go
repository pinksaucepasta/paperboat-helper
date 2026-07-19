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
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", Environment: []string{"PATH=/bin", "TERM=xterm"}, Sessions: runtime, Runner: check})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := supervisor.Launch(context.Background(), LaunchRequest{Name: "default", CWD: t.TempDir(), Dimensions: pty.Dimensions{Columns: 80, Rows: 24}})
	if err != nil || snapshot.ID != "ses_1" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
	command := runtime.created[0].Command
	if command.Path != executable || len(command.Args) != 1 || command.Args[0] != "--no-session" {
		t.Fatalf("command=%#v", command)
	}
	if err := supervisor.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(runtime.closed) != 1 || runtime.closed[0] != "ses_1" {
		t.Fatalf("closed=%v", runtime.closed)
	}
}

func TestSupervisorRejectsVersionAndDuplicateLaunch(t *testing.T) {
	executable := testExecutable(t)
	runtime := &sessions{snapshot: session.Snapshot{ID: "ses_1"}}
	if _, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", Sessions: runtime, Runner: &runner{output: []byte("herdr 0.8.0\n")}}); !errors.Is(err, ErrVersionIncompatible) {
		t.Fatalf("version err=%v", err)
	}
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", Sessions: runtime, Runner: &runner{output: []byte("herdr 0.7.4\n")}})
	if err != nil {
		t.Fatal(err)
	}
	request := LaunchRequest{Name: "default", CWD: t.TempDir(), Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}
	if _, err := supervisor.Launch(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := supervisor.Launch(context.Background(), request); !errors.Is(err, session.ErrSessionExists) {
		t.Fatalf("duplicate err=%v", err)
	}
}

func TestSupervisorRejectsLoaderEnvironment(t *testing.T) {
	_, err := NewSupervisor(context.Background(), Config{Executable: testExecutable(t), ExpectedVersion: "0.7.4", Environment: []string{"LD_PRELOAD=/tmp/injected.so"}, Sessions: &sessions{}, Runner: &runner{output: []byte("herdr 0.7.4\n")}})
	if !errors.Is(err, ErrLaunchRejected) {
		t.Fatalf("err=%v", err)
	}
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
