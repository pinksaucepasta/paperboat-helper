//go:build darwin || linux

package hosted

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"sync"
	"syscall"
)

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, command Command) ([]byte, error) {
	writer := &limitedWriter{remaining: command.OutputLimit}
	cmd := exec.CommandContext(ctx, command.Path, command.Args...)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = command.Dir, command.Env, writer, writer
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	err := cmd.Run()
	if writer.exceeded {
		return nil, errors.Join(ErrOutputLimit, err)
	}
	return writer.Bytes(), err
}

type limitedWriter struct {
	mu        sync.Mutex
	remaining int
	exceeded  bool
	output    bytes.Buffer
}

func (w *limitedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(data) > w.remaining {
		_, _ = w.output.Write(data[:w.remaining])
		w.exceeded = true
		w.remaining = 0
		return len(data), nil
	}
	w.remaining -= len(data)
	_, _ = w.output.Write(data)
	return len(data), nil
}
func (w *limitedWriter) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.output.Bytes()...)
}

var _ io.Writer = (*limitedWriter)(nil)
