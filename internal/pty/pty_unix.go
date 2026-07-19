//go:build darwin || linux

package pty

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	creackpty "github.com/creack/pty"
)

var (
	ErrInvalidCommand    = errors.New("invalid PTY command")
	ErrInvalidCWD        = errors.New("invalid PTY cwd")
	ErrInvalidDimensions = errors.New("invalid PTY dimensions")
	ErrInvalidSignal     = errors.New("invalid PTY signal")
)

type Dimensions struct {
	Columns uint16 `json:"columns"`
	Rows    uint16 `json:"rows"`
}
type Command struct {
	Path       string
	Args       []string
	Env        []string
	CWD        string
	Dimensions Dimensions
}
type ExitResult struct {
	Code     int       `json:"code"`
	Signal   string    `json:"signal,omitempty"`
	ExitedAt time.Time `json:"exited_at"`
}
type Signal string

const (
	Interrupt Signal = "SIGINT"
	Terminate Signal = "SIGTERM"
	Hangup    Signal = "SIGHUP"
	Kill      Signal = "SIGKILL"
)

type Adapter struct{ root string }

func NewAdapter(root string) (*Adapter, error) {
	resolved, err := resolveDirectory(root)
	if err != nil {
		return nil, fmt.Errorf("root: %w", ErrInvalidCWD)
	}
	return &Adapter{root: resolved}, nil
}

func (a *Adapter) Start(command Command) (*Process, error) {
	path, err := ValidateProcessPolicy(command.Path, command.Args, command.Env)
	if err != nil {
		return nil, err
	}
	cwd, err := resolveDirectory(command.CWD)
	if err != nil || !within(a.root, cwd) {
		return nil, ErrInvalidCWD
	}
	if !validDimensions(command.Dimensions) {
		return nil, ErrInvalidCommand
	}
	cmd := exec.Command(path, command.Args...)
	cmd.Dir = cwd
	cmd.Env = append([]string(nil), command.Env...)
	terminal, err := creackpty.StartWithSize(cmd, &creackpty.Winsize{Cols: command.Dimensions.Columns, Rows: command.Dimensions.Rows})
	if err != nil {
		return nil, fmt.Errorf("start PTY: %w", err)
	}
	process := &Process{file: terminal, cmd: cmd, done: make(chan struct{})}
	go process.wait()
	return process, nil
}

type Process struct {
	file      *os.File
	cmd       *exec.Cmd
	done      chan struct{}
	mu        sync.RWMutex
	result    ExitResult
	waitErr   error
	closeOnce sync.Once
}

func (p *Process) Read(buffer []byte) (int, error) {
	n, err := p.file.Read(buffer)
	if errors.Is(err, syscall.EIO) {
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}
	return n, err
}
func (p *Process) Write(data []byte) (int, error) { return p.file.Write(data) }
func (p *Process) Resize(dimensions Dimensions) error {
	if !validDimensions(dimensions) {
		return ErrInvalidDimensions
	}
	return creackpty.Setsize(p.file, &creackpty.Winsize{Cols: dimensions.Columns, Rows: dimensions.Rows})
}
func (p *Process) Signal(signal Signal) error {
	native, ok := nativeSignal(signal)
	if !ok {
		return ErrInvalidSignal
	}
	if p.cmd.Process == nil {
		return os.ErrProcessDone
	}
	if err := syscall.Kill(-p.cmd.Process.Pid, native); err != nil {
		return fmt.Errorf("signal process group: %w", err)
	}
	return nil
}
func (p *Process) Wait(ctx context.Context) (ExitResult, error) {
	select {
	case <-p.done:
		p.mu.RLock()
		defer p.mu.RUnlock()
		return p.result, p.waitErr
	case <-ctx.Done():
		return ExitResult{}, ctx.Err()
	}
}
func (p *Process) CloseIO() error {
	var err error
	p.closeOnce.Do(func() { err = p.file.Close() })
	return err
}
func (p *Process) Terminate(ctx context.Context, grace time.Duration) (ExitResult, error) {
	if grace < 0 {
		return ExitResult{}, ErrInvalidCommand
	}
	select {
	case <-p.done:
		result, err := p.Wait(context.Background())
		_ = p.CloseIO()
		return result, err
	default:
	}
	if err := p.Signal(Terminate); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return ExitResult{}, err
	}
	graceCtx, cancel := context.WithTimeout(ctx, grace)
	result, err := p.Wait(graceCtx)
	cancel()
	if err == nil {
		_ = p.CloseIO()
		return result, nil
	}
	if ctx.Err() != nil {
		return ExitResult{}, ctx.Err()
	}
	if err := p.Signal(Kill); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return ExitResult{}, err
	}
	result, err = p.Wait(ctx)
	_ = p.CloseIO()
	return result, err
}
func (p *Process) wait() {
	err := p.cmd.Wait()
	result := ExitResult{ExitedAt: time.Now().UTC()}
	if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok {
		if status.Signaled() {
			result.Code = 128 + int(status.Signal())
			result.Signal = status.Signal().String()
		} else {
			result.Code = status.ExitStatus()
		}
	} else if p.cmd.ProcessState.Success() {
		result.Code = 0
	} else {
		result.Code = -1
	}
	p.mu.Lock()
	p.result = result
	if _, ok := err.(*exec.ExitError); !ok {
		p.waitErr = err
	}
	p.mu.Unlock()
	close(p.done)
}

func nativeSignal(signal Signal) (syscall.Signal, bool) {
	switch signal {
	case Interrupt:
		return syscall.SIGINT, true
	case Terminate:
		return syscall.SIGTERM, true
	case Hangup:
		return syscall.SIGHUP, true
	case Kill:
		return syscall.SIGKILL, true
	default:
		return 0, false
	}
}
func validDimensions(dimensions Dimensions) bool {
	return dimensions.Columns >= 1 && dimensions.Columns <= 1000 && dimensions.Rows >= 1 && dimensions.Rows <= 1000
}
func validEnvironment(environment []string) bool {
	if len(environment) > 128 {
		return false
	}
	seen := make(map[string]bool, len(environment))
	total := 0
	for _, entry := range environment {
		total += len(entry)
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" || len(entry) > 4096 || total > 64<<10 || strings.ContainsAny(key, "\x00\r\n") || strings.ContainsRune(value, '\x00') || seen[key] {
			return false
		}
		seen[key] = true
	}
	return true
}

// ValidateProcessPolicy resolves and validates the static executable, arguments,
// and environment before a runtime advertises terminal readiness.
func ValidateProcessPolicy(path string, arguments, environment []string) (string, error) {
	resolved, err := validateExecutable(path)
	if err != nil || !validArguments(arguments) || !validEnvironment(environment) {
		return "", ErrInvalidCommand
	}
	return resolved, nil
}

func validArguments(arguments []string) bool {
	if len(arguments) > 64 {
		return false
	}
	total := 0
	for _, argument := range arguments {
		total += len(argument)
		if len(argument) > 4096 || total > 64<<10 || strings.ContainsRune(argument, '\x00') {
			return false
		}
	}
	return true
}

func validateExecutable(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrInvalidCommand
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", ErrInvalidCommand
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", ErrInvalidCommand
	}
	return resolved, nil
}
func resolveDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrInvalidCWD
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", ErrInvalidCWD
	}
	return filepath.Clean(resolved), nil
}
func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
