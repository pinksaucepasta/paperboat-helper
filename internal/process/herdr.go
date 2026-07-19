package process

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

var (
	ErrExecutableInvalid   = errors.New("Herdr executable invalid")
	ErrVersionIncompatible = errors.New("Herdr version incompatible")
	ErrLaunchRejected      = errors.New("Herdr launch rejected")
)

type CommandRunner interface {
	Output(context.Context, string, ...string) ([]byte, error)
}
type SessionRuntime interface {
	Create(context.Context, session.CreateRequest) (session.Snapshot, error)
	Close(context.Context, string) (session.Snapshot, error)
}
type Config struct {
	Executable      string
	ExpectedVersion string
	Environment     []string
	Sessions        SessionRuntime
	Runner          CommandRunner
	VersionTimeout  time.Duration
}
type LaunchRequest struct {
	Name       string
	CWD        string
	Dimensions pty.Dimensions
}
type Supervisor struct {
	mu       sync.Mutex
	config   Config
	sessions map[string]string
	stopping bool
}

func NewSupervisor(ctx context.Context, config Config) (*Supervisor, error) {
	if config.Runner == nil {
		config.Runner = ExecRunner{MaxOutput: 4 << 10}
	}
	if config.VersionTimeout == 0 {
		config.VersionTimeout = 3 * time.Second
	}
	if config.Sessions == nil || config.ExpectedVersion == "" || config.VersionTimeout <= 0 || !validEnvironment(config.Environment) {
		return nil, ErrLaunchRejected
	}
	executable, err := validateExecutable(config.Executable)
	if err != nil {
		return nil, err
	}
	config.Executable = executable
	versionCtx, cancel := context.WithTimeout(ctx, config.VersionTimeout)
	output, err := config.Runner.Output(versionCtx, executable, "--version")
	cancel()
	if err != nil {
		return nil, fmt.Errorf("verify Herdr version: %w", err)
	}
	if strings.TrimSpace(string(output)) != "herdr "+config.ExpectedVersion {
		return nil, fmt.Errorf("%w: expected %s", ErrVersionIncompatible, config.ExpectedVersion)
	}
	return &Supervisor{config: config, sessions: make(map[string]string)}, nil
}

var allowedEnvironment = map[string]bool{"HOME": true, "XDG_CONFIG_HOME": true, "PATH": true, "SHELL": true, "TERM": true, "COLORTERM": true, "LANG": true, "LC_ALL": true, "NO_COLOR": true}

func validEnvironment(environment []string) bool {
	seen := make(map[string]bool, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || !allowedEnvironment[key] || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

func (s *Supervisor) Launch(ctx context.Context, request LaunchRequest) (session.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping {
		return session.Snapshot{}, ErrLaunchRejected
	}
	if _, exists := s.sessions[request.Name]; exists {
		return session.Snapshot{}, session.ErrSessionExists
	}
	command := pty.Command{Path: s.config.Executable, Args: []string{"--no-session"}, Env: append([]string(nil), s.config.Environment...), CWD: request.CWD, Dimensions: request.Dimensions}
	snapshot, err := s.config.Sessions.Create(ctx, session.CreateRequest{Name: request.Name, Command: command})
	if err != nil {
		return session.Snapshot{}, err
	}
	s.sessions[request.Name] = snapshot.ID
	return snapshot, nil
}

func (s *Supervisor) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	s.stopping = true
	ids := make([]string, 0, len(s.sessions))
	for _, id := range s.sessions {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	var result error
	for _, id := range ids {
		if _, err := s.config.Sessions.Close(ctx, id); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

type ExecRunner struct{ MaxOutput int }

func (r ExecRunner) Output(ctx context.Context, path string, args ...string) ([]byte, error) {
	if r.MaxOutput <= 0 {
		r.MaxOutput = 4 << 10
	}
	buffer := &boundedBuffer{remaining: r.MaxOutput}
	command := exec.CommandContext(ctx, path, args...)
	command.Stdout = buffer
	command.Stderr = buffer
	if err := command.Run(); err != nil {
		return nil, err
	}
	return append([]byte(nil), buffer.data...), nil
}

type boundedBuffer struct {
	data      []byte
	remaining int
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	if len(data) > b.remaining {
		return 0, errors.New("command output limit exceeded")
	}
	b.data = append(b.data, data...)
	b.remaining -= len(data)
	return len(data), nil
}

func validateExecutable(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrExecutableInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return "", ErrExecutableInvalid
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", ErrExecutableInvalid
	}
	return resolved, nil
}
