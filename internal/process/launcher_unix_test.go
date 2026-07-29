//go:build darwin || linux

package process

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type realShellSessions struct {
	adapter *pty.Adapter
	output  string
	exit    pty.ExitResult
}

func (s *realShellSessions) Create(ctx context.Context, request session.CreateRequest) (session.Snapshot, error) {
	process, err := s.adapter.Start(request.Command)
	if err != nil {
		return session.Snapshot{}, err
	}
	defer process.CloseIO()
	if _, err := process.Write([]byte("printf PAPERBOAT_LOGIN_SHELL_OK; exit 7\n")); err != nil {
		return session.Snapshot{}, err
	}
	output, err := io.ReadAll(process)
	if err != nil {
		return session.Snapshot{}, err
	}
	s.output = string(output)
	s.exit, err = process.Wait(ctx)
	return session.Snapshot{ID: request.ID}, err
}

func (*realShellSessions) Close(_ context.Context, id string) (session.Snapshot, error) {
	return session.Snapshot{ID: id}, nil
}

func TestRealShellLauncherStartsLoginShellInPTY(t *testing.T) {
	var shell string
	for _, candidate := range []string{"/bin/bash", "/usr/bin/bash", "/bin/zsh", "/bin/sh"} {
		info, err := os.Lstat(candidate)
		if err == nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 {
			shell = candidate
			break
		}
	}
	if shell == "" {
		t.Skip("requires a canonical login shell executable")
	}
	workspace := t.TempDir()
	adapter, err := pty.NewAdapter(workspace)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &realShellSessions{adapter: adapter}
	launcher, err := NewShellLauncher(shell, []string{"HOME=" + workspace, "PATH=/usr/bin:/bin", "SHELL=" + shell, "TERM=xterm"}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := launcher.Launch(ctx, LaunchRequest{ID: "ses_real_shell", Name: "real-shell", CWD: workspace, Dimensions: pty.Dimensions{Columns: 80, Rows: 24}}); err != nil {
		t.Fatal(err)
	}
	if runtime.exit.Code != 7 || !strings.Contains(runtime.output, "PAPERBOAT_LOGIN_SHELL_OK") {
		t.Fatalf("exit=%#v output=%q", runtime.exit, runtime.output)
	}
}
