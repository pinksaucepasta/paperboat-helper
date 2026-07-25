package service

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type Runner interface {
	Run(context.Context, string, ...string) error
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, name string, arguments ...string) error {
	output := &boundedCommandOutput{limit: 8 << 10}
	command := exec.CommandContext(ctx, name, arguments...)
	command.Stdout, command.Stderr = output, output
	if err := command.Run(); err != nil {
		return &CommandError{Tool: name, Output: output.String(), Cause: err}
	}
	return nil
}

type CommandError struct {
	Tool   string
	Output string
	Cause  error
}

func (e *CommandError) Error() string {
	if e.Output == "" {
		return fmt.Sprintf("%s: %v", e.Tool, e.Cause)
	}
	return fmt.Sprintf("%s: %v: %s", e.Tool, e.Cause, e.Output)
}
func (e *CommandError) Unwrap() error { return e.Cause }

type boundedCommandOutput struct {
	bytes []byte
	limit int
}

func (w *boundedCommandOutput) Write(data []byte) (int, error) {
	consumed := len(data)
	remaining := w.limit - len(w.bytes)
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		w.bytes = append(w.bytes, data...)
	}
	return consumed, nil
}
func (w *boundedCommandOutput) String() string { return string(w.bytes) }

type SystemdController struct{ Runner Runner }

func (c SystemdController) Apply(ctx context.Context, _ string, upgrading bool) error {
	if c.Runner == nil {
		return ErrInvalidDefinition
	}
	if err := c.Runner.Run(ctx, "systemctl", "--user", "daemon-reload"); err != nil {
		return err
	}
	if err := c.Runner.Run(ctx, "systemctl", "--user", "enable", "--now", "paperboat-helper.service"); err != nil {
		return err
	}
	if upgrading {
		if err := c.Runner.Run(ctx, "systemctl", "--user", "restart", "paperboat-helper.service"); err != nil {
			return err
		}
	}
	return c.Runner.Run(ctx, "systemctl", "--user", "is-active", "--quiet", "paperboat-helper.service")
}

func (c SystemdController) Remove(ctx context.Context, _ string) error {
	if c.Runner == nil {
		return ErrInvalidDefinition
	}
	if err := c.Runner.Run(ctx, "systemctl", "--user", "disable", "--now", "paperboat-helper.service"); err != nil {
		return err
	}
	return c.Runner.Run(ctx, "systemctl", "--user", "daemon-reload")
}

type LaunchdController struct {
	Runner Runner
	UID    int
}

func (c LaunchdController) Apply(ctx context.Context, path string, upgrading bool) error {
	if c.Runner == nil || c.UID < 0 {
		return ErrInvalidDefinition
	}
	domain := fmt.Sprintf("gui/%d", c.UID)
	service := domain + "/" + Label
	if upgrading {
		if err := c.Runner.Run(ctx, "launchctl", "bootout", service); err != nil && !strings.Contains(err.Error(), "No such process") {
			return err
		}
	}
	for {
		err := c.Runner.Run(ctx, "launchctl", "bootstrap", domain, path)
		if err == nil {
			break
		}
		// launchd can keep a recently booted-out label reserved briefly. It can
		// also return an error after loading the job, so verify state before retrying.
		if c.Runner.Run(ctx, "launchctl", "print", service) == nil {
			break
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
	if err := c.Runner.Run(ctx, "launchctl", "kickstart", "-k", service); err != nil {
		return err
	}
	return c.Runner.Run(ctx, "launchctl", "print", service)
}

func (c LaunchdController) Remove(ctx context.Context, _ string) error {
	if c.Runner == nil || c.UID < 0 {
		return ErrInvalidDefinition
	}
	err := c.Runner.Run(ctx, "launchctl", "bootout", fmt.Sprintf("gui/%d/%s", c.UID, Label))
	if err != nil && strings.Contains(err.Error(), "No such process") {
		return nil
	}
	return err
}
