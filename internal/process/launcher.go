package process

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

var ErrLaunchRejected = errors.New("shell launch rejected")

type Launcher interface {
	Launch(context.Context, LaunchRequest) (session.Snapshot, error)
}

type SessionRuntime interface {
	Create(context.Context, session.CreateRequest) (session.Snapshot, error)
	Close(context.Context, string) (session.Snapshot, error)
}

type LaunchRequest struct {
	ID          string
	Name        string
	CWD         string
	Dimensions  pty.Dimensions
	Environment map[string]string
}

type ShellLauncher struct {
	path        string
	environment []string
	sessions    SessionRuntime
}

func NewShellLauncher(shellPath string, environment []string, sessions SessionRuntime) (*ShellLauncher, error) {
	resolved, err := validateExecutable(shellPath)
	if err != nil || sessions == nil || !validEnvironment(environment) || !filepath.IsAbs(resolved) {
		return nil, ErrLaunchRejected
	}
	return &ShellLauncher{path: resolved, environment: append([]string(nil), environment...), sessions: sessions}, nil
}

func (l *ShellLauncher) Launch(ctx context.Context, request LaunchRequest) (session.Snapshot, error) {
	if !validSessionID(request.ID) {
		return session.Snapshot{}, ErrLaunchRejected
	}
	environment, ok := mergeClientEnvironment(l.environment, request.Environment)
	if !ok {
		return session.Snapshot{}, ErrLaunchRejected
	}
	environment = replaceEnvironment(environment, "PAPERBOAT_TERMINAL_SESSION_ID", request.ID)
	command := pty.Command{Path: l.path, Args: []string{"-l"}, Env: environment, CWD: request.CWD, Dimensions: request.Dimensions}
	return l.sessions.Create(ctx, session.CreateRequest{ID: request.ID, Name: request.Name, Command: command})
}

var allowedEnvironment = map[string]bool{
	"HOME": true, "PATH": true, "SHELL": true,
	"TERM": true, "COLORTERM": true, "TERM_PROGRAM": true, "TERM_PROGRAM_VERSION": true,
	"LANG": true, "LC_ALL": true, "LC_CTYPE": true, "NO_COLOR": true,
	"PAPERBOAT_PREVIEW_REGISTRATION_ENDPOINT": true, "PAPERBOAT_HELPER_AGENT_TOKEN_FILE": true,
	"PAPERBOAT_FILE_TRANSFER_ENDPOINT": true, "PAPERBOAT_WORKSPACE_ROOT": true, "PAPERBOAT_TERMINAL_SESSION_ID": true,
}

var allowedClientEnvironment = map[string]bool{
	"TERM": true, "COLORTERM": true, "TERM_PROGRAM": true, "TERM_PROGRAM_VERSION": true,
	"LANG": true, "LC_ALL": true, "LC_CTYPE": true,
}

func mergeClientEnvironment(base []string, overrides map[string]string) ([]string, bool) {
	result := append([]string(nil), base...)
	for key, value := range overrides {
		if !allowedClientEnvironment[key] || value == "" || len(value) > 8192 || strings.ContainsRune(value, '\x00') {
			return nil, false
		}
		result = replaceEnvironment(result, key, value)
	}
	return result, validEnvironment(result)
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

func validateExecutable(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrLaunchRejected
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return "", ErrLaunchRejected
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", ErrLaunchRejected
	}
	return resolved, nil
}
