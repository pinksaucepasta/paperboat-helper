package process

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

var (
	ErrExecutableInvalid   = errors.New("Herdr executable invalid")
	ErrVersionIncompatible = errors.New("Herdr version incompatible")
	ErrLaunchRejected      = errors.New("Herdr launch rejected")
)

const maxUnixSocketPathBytes = 100

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
	StateRoot       string
	Sessions        SessionRuntime
	Runner          CommandRunner
	VersionTimeout  time.Duration
}
type LaunchRequest struct {
	ID         string
	Name       string
	CWD        string
	Dimensions pty.Dimensions
}
type Supervisor struct {
	config Config
}

func NewSupervisor(ctx context.Context, config Config) (*Supervisor, error) {
	if config.Runner == nil {
		config.Runner = ExecRunner{MaxOutput: 4 << 10}
	}
	if config.VersionTimeout == 0 {
		config.VersionTimeout = 3 * time.Second
	}
	if config.Sessions == nil || config.ExpectedVersion == "" || config.VersionTimeout <= 0 || !validEnvironment(config.Environment) || !filepath.IsAbs(config.StateRoot) {
		return nil, ErrLaunchRejected
	}
	if err := privateDirectory(config.StateRoot); err != nil {
		return nil, fmt.Errorf("prepare Herdr state: %w", ErrLaunchRejected)
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
	return &Supervisor{config: config}, nil
}

var allowedEnvironment = map[string]bool{
	"HOME": true, "XDG_CONFIG_HOME": true, "PATH": true, "SHELL": true,
	"TERM": true, "COLORTERM": true, "LANG": true, "LC_ALL": true, "NO_COLOR": true,
	"PAPERBOAT_PREVIEW_REGISTRATION_ENDPOINT": true, "PAPERBOAT_HELPER_AGENT_TOKEN_FILE": true,
}

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
	if !validSessionID(request.ID) {
		return session.Snapshot{}, ErrLaunchRejected
	}
	stateRoot := filepath.Join(s.config.StateRoot, request.ID)
	if err := privateDirectory(stateRoot); err != nil {
		return session.Snapshot{}, fmt.Errorf("prepare Herdr session state: %w", err)
	}
	socketRoot := filepath.Join("/tmp", fmt.Sprintf("paperboat-herdr-%d", os.Getuid()))
	digest := sha256.Sum256([]byte(s.config.StateRoot + "\x00" + request.ID))
	socketDirectory := filepath.Join(socketRoot, fmt.Sprintf("%x", digest[:12]))
	if err := privateDirectory(socketDirectory); err != nil {
		return session.Snapshot{}, fmt.Errorf("prepare Herdr session sockets: %w", err)
	}
	serverSocket := filepath.Join(socketDirectory, "herdr.sock")
	clientSocket := filepath.Join(socketDirectory, "client.sock")
	if len(serverSocket) > maxUnixSocketPathBytes || len(clientSocket) > maxUnixSocketPathBytes {
		return session.Snapshot{}, fmt.Errorf("Herdr session socket path is too long: %w", ErrLaunchRejected)
	}
	environment := replaceEnvironment(s.config.Environment, "XDG_CONFIG_HOME", stateRoot)
	environment = replaceEnvironment(environment, "HERDR_SOCKET_PATH", serverSocket)
	environment = replaceEnvironment(environment, "HERDR_CLIENT_SOCKET_PATH", clientSocket)
	command := pty.Command{Path: s.config.Executable, Args: []string{"--no-session"}, Env: environment, CWD: request.CWD, Dimensions: request.Dimensions}
	return s.config.Sessions.Create(ctx, session.CreateRequest{ID: request.ID, Name: request.Name, Command: command})
}

func validSessionID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character != '-' && character != '_' && (character < 'a' || character > 'z') && (character < 'A' || character > 'Z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func replaceEnvironment(environment []string, key, value string) []string {
	replaced := false
	result := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		entryKey, _, _ := strings.Cut(entry, "=")
		if entryKey == key {
			result = append(result, key+"="+value)
			replaced = true
		} else {
			result = append(result, entry)
		}
	}
	if !replaced {
		result = append(result, key+"="+value)
	}
	return result
}

func privateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrLaunchRejected
	}
	return os.Chmod(path, 0o700)
}

func (*Supervisor) Shutdown(context.Context) error { return nil }

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
