//go:build darwin || linux

package process

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

func TestRealHerdrStartsInsideHelperPTY(t *testing.T) {
	executable, err := exec.LookPath("herdr")
	if err != nil {
		t.Skip("requires Herdr 0.7.4 on PATH")
	}
	workspace := t.TempDir()
	adapter, err := pty.NewAdapter(workspace)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := session.NewManager(session.ManagerConfig{Launch: func(command pty.Command) (session.PTYProcess, error) { return adapter.Start(command) }, Random: bytes.NewReader(make([]byte, 32)), TerminationTimeout: 5 * time.Second, TerminationGrace: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	supervisor, err := NewSupervisor(context.Background(), Config{Executable: executable, ExpectedVersion: "0.7.4", Environment: []string{"HOME=" + home, "XDG_CONFIG_HOME=" + home, "PATH=/usr/bin:/bin", "SHELL=/bin/sh", "TERM=xterm-256color"}, StateRoot: filepath.Join(home, "herdr"), Sessions: manager})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := supervisor.Launch(context.Background(), LaunchRequest{ID: "ses_herdr_smoke", Name: "herdr-smoke", CWD: workspace, Dimensions: pty.Dimensions{Columns: 100, Rows: 30}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		current, err := manager.Snapshot(snapshot.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.LatestSequence > 0 || current.State == session.Exited {
			break
		}
		select {
		case <-deadline.C:
			t.Fatal("Herdr produced no PTY output")
		case <-ticker.C:
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := supervisor.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
