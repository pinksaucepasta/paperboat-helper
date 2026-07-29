package process

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

var ErrTerminalModeInvalid = errors.New("terminal mode invalid")

type Launcher interface {
	Launch(context.Context, LaunchRequest) (session.Snapshot, error)
}

type ModeLauncher struct {
	Herdr       Launcher
	ShellPath   string
	Environment []string
	Sessions    SessionRuntime
}

func NewModeLauncher(herdr Launcher, shellPath string, environment []string, sessions SessionRuntime) (*ModeLauncher, error) {
	resolved, err := validateExecutable(shellPath)
	if err != nil || herdr == nil || sessions == nil || !validEnvironment(environment) || !filepath.IsAbs(resolved) {
		return nil, ErrLaunchRejected
	}
	return &ModeLauncher{Herdr: herdr, ShellPath: resolved, Environment: append([]string(nil), environment...), Sessions: sessions}, nil
}

func (l *ModeLauncher) Launch(ctx context.Context, request LaunchRequest) (session.Snapshot, error) {
	switch request.Mode {
	case "", "herdr":
		return l.Herdr.Launch(ctx, request)
	case "shell":
		if !validSessionID(request.ID) {
			return session.Snapshot{}, ErrLaunchRejected
		}
		environment, ok := mergeClientEnvironment(l.Environment, request.Environment)
		if !ok {
			return session.Snapshot{}, ErrLaunchRejected
		}
		environment = replaceEnvironment(environment, "PAPERBOAT_TERMINAL_SESSION_ID", request.ID)
		command := pty.Command{Path: l.ShellPath, Args: []string{"-l"}, Env: environment, CWD: request.CWD, Dimensions: request.Dimensions}
		return l.Sessions.Create(ctx, session.CreateRequest{ID: request.ID, Name: request.Name, Command: command})
	default:
		return session.Snapshot{}, ErrTerminalModeInvalid
	}
}
