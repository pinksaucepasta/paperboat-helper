//go:build linux

package hostservice

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type platformApplier struct {
	mu      sync.Mutex
	command *exec.Cmd
	done    <-chan error
}

func NewPlatformApplier(string) Applier { return &platformApplier{} }

func (a *platformApplier) Apply(ctx context.Context, mode string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if mode == AllowSleep {
		return a.releaseLocked(ctx)
	}
	if a.command != nil && a.command.Process != nil {
		return nil
	}
	command := exec.Command("/usr/bin/systemd-inhibit",
		"--what=sleep:idle:handle-lid-switch", "--who=Paperboat",
		"--why=Paperboat keep_awake availability policy", "--mode=block",
		"/usr/bin/sleep", "infinity")
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		return err
	}
	a.command = command
	a.done = waitCommand(command)
	select {
	case err := <-a.done:
		a.command = nil
		a.done = nil
		if err == nil {
			return errors.New("systemd inhibitor exited unexpectedly")
		}
		return err
	case <-time.After(150 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return errors.Join(ctx.Err(), a.releaseLocked(context.Background()))
	}
}

func (a *platformApplier) Close(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.releaseLocked(ctx)
}

func (a *platformApplier) releaseLocked(ctx context.Context) error {
	if a.command == nil || a.command.Process == nil {
		a.command = nil
		return nil
	}
	command := a.command
	done := a.done
	a.command = nil
	a.done = nil
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		<-done
		return ctx.Err()
	}
}

func waitCommand(command *exec.Cmd) <-chan error {
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	return done
}
