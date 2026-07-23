package hosted

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrInvalid     = errors.New("invalid hosted lifecycle configuration")
	ErrIdentity    = errors.New("hosted workspace identity mismatch")
	ErrRepository  = errors.New("hosted repository is not permitted")
	ErrOutputLimit = errors.New("hosted command output limit exceeded")
	ErrNotStarted  = errors.New("hosted lifecycle is not started")
)

type Stage string

const (
	StageWorkspace     Stage = "workspace"
	StageCheckout      Stage = "checkout"
	StageConfigRestore Stage = "config_restore"
	StagePresets       Stage = "presets"
	StageSetup         Stage = "setup"
	StageReady         Stage = "ready"
	StageFlush         Stage = "pre_stop_flush"
)

type Script struct{ Name, Body string }

type Config struct {
	VolumeRoot, CheckoutRoot         string
	GitToken                         string
	ProjectID, RepositoryURL, Branch string
	AllowedRepositoryHosts           []string
	GitPath, ShellPath               string
	Presets                          []Script
	SetupScript                      string
	OperationTimeout                 time.Duration
	FlushTimeout                     time.Duration
	MaxScriptBytes                   int
	MaxOutputBytes                   int
}

type Hooks struct {
	Restore func(context.Context, string) error
	Flush   func(context.Context, string) error
}

type Runner interface {
	Run(context.Context, Command) ([]byte, error)
}

type Command struct {
	Path        string
	Args        []string
	Dir         string
	Env         []string
	OutputLimit int
}

type Snapshot struct {
	Stage     Stage  `json:"stage"`
	Ready     bool   `json:"ready"`
	ErrorCode string `json:"error_code,omitempty"`
}

type Lifecycle struct {
	config   Config
	hooks    Hooks
	runner   Runner
	mu       sync.RWMutex
	snapshot Snapshot
	started  bool
}

func New(config Config, hooks Hooks, runner Runner) (*Lifecycle, error) {
	if err := validate(config); err != nil {
		return nil, err
	}
	if runner == nil {
		runner = ExecRunner{}
	}
	return &Lifecycle{config: config, hooks: hooks, runner: runner, snapshot: Snapshot{Stage: StageWorkspace}}, nil
}

func (*Lifecycle) Capabilities() []string { return []string{"hosted.lifecycle.v1"} }

func (l *Lifecycle) Snapshot() Snapshot { l.mu.RLock(); defer l.mu.RUnlock(); return l.snapshot }

func (l *Lifecycle) Start(ctx context.Context) error {
	l.mu.Lock()
	if l.started {
		l.mu.Unlock()
		return nil
	}
	l.mu.Unlock()
	steps := []struct {
		stage Stage
		run   func(context.Context) error
	}{
		{StageWorkspace, l.prepareWorkspace}, {StageCheckout, l.checkout},
		{StageConfigRestore, func(ctx context.Context) error {
			if l.hooks.Restore == nil {
				return nil
			}
			return l.hooks.Restore(ctx, l.config.CheckoutRoot)
		}},
		{StagePresets, l.applyPresets}, {StageSetup, l.applySetup},
	}
	var degradedCode string
	for _, step := range steps {
		l.set(step.stage, false, "")
		stepCtx, cancel := context.WithTimeout(ctx, l.config.OperationTimeout)
		err := step.run(stepCtx)
		cancel()
		if err != nil {
			code := errorCode(err)
			l.set(step.stage, false, code)
			// Config sync is an optional hosted capability. A restore failure
			// must not make terminal, upload, preview, or connector startup
			// unavailable; retain the typed degradation for health/diagnostics.
			if step.stage == StageConfigRestore {
				degradedCode = code
				continue
			}
			return fmt.Errorf("hosted %s: %w", step.stage, err)
		}
	}
	l.mu.Lock()
	l.started = true
	l.snapshot = Snapshot{Stage: StageReady, Ready: true, ErrorCode: degradedCode}
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) Shutdown(ctx context.Context) error {
	l.mu.RLock()
	started := l.started
	l.mu.RUnlock()
	if !started {
		return ErrNotStarted
	}
	l.set(StageFlush, false, "")
	if l.hooks.Flush != nil {
		flushCtx, cancel := context.WithTimeout(ctx, l.config.FlushTimeout)
		err := l.hooks.Flush(flushCtx, l.config.CheckoutRoot)
		cancel()
		if err != nil {
			l.set(StageFlush, false, errorCode(err))
			return fmt.Errorf("hosted pre-stop flush: %w", err)
		}
	}
	l.mu.Lock()
	l.started = false
	l.snapshot = Snapshot{Stage: StageFlush}
	l.mu.Unlock()
	return nil
}

func (l *Lifecycle) set(stage Stage, ready bool, code string) {
	l.mu.Lock()
	l.snapshot = Snapshot{Stage: stage, Ready: ready, ErrorCode: code}
	l.mu.Unlock()
}

type workspaceIdentity struct {
	Version       int    `json:"version"`
	ProjectID     string `json:"project_id"`
	RepositoryURL string `json:"repository_url"`
}

func (l *Lifecycle) prepareWorkspace(context.Context) error {
	if err := secureDirectory(l.config.VolumeRoot); err != nil {
		return err
	}
	meta := filepath.Join(l.config.VolumeRoot, ".paperboat")
	if err := secureDirectory(meta); err != nil {
		return err
	}
	path := filepath.Join(meta, "workspace.json")
	want := workspaceIdentity{Version: 1, ProjectID: l.config.ProjectID, RepositoryURL: l.config.RepositoryURL}
	info, err := os.Lstat(path)
	if err == nil {
		stat, statOK := info.Sys().(*syscall.Stat_t)
		if !statOK || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || stat.Nlink != 1 || info.Size() > 16<<10 {
			return ErrIdentity
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var got workspaceIdentity
		decoder := json.NewDecoder(strings.NewReader(string(data)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&got) != nil || got != want {
			return ErrIdentity
		}
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			return ErrIdentity
		}
		return writePrivateFile(filepath.Join(meta, "project-dir"), []byte(l.config.CheckoutRoot+"\n"))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	encoded, _ := json.Marshal(want)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(encoded, '\n')); err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return err
	}
	return writePrivateFile(filepath.Join(meta, "project-dir"), []byte(l.config.CheckoutRoot+"\n"))
}

func writePrivateFile(path string, data []byte) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".hosted-state-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (l *Lifecycle) checkout(ctx context.Context) error {
	gitDir := filepath.Join(l.config.CheckoutRoot, ".git")
	info, err := os.Lstat(gitDir)
	if errors.Is(err, os.ErrNotExist) {
		if entries, readErr := os.ReadDir(l.config.CheckoutRoot); readErr == nil && len(entries) != 0 {
			return ErrRepository
		}
		_, err := l.runner.Run(ctx, Command{Path: l.config.GitPath, Args: []string{"clone", "--single-branch", "--branch", l.config.Branch, "--", l.config.RepositoryURL, l.config.CheckoutRoot}, Dir: l.config.VolumeRoot, Env: l.gitEnvironment(), OutputLimit: l.config.MaxOutputBytes})
		return err
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return ErrRepository
	}
	checkoutInfo, err := os.Lstat(l.config.CheckoutRoot)
	if err != nil || checkoutInfo.Mode()&os.ModeSymlink != 0 || !checkoutInfo.IsDir() {
		return ErrRepository
	}
	remote, err := l.runner.Run(ctx, Command{Path: l.config.GitPath, Args: []string{"remote", "get-url", "origin"}, Dir: l.config.CheckoutRoot, Env: l.gitEnvironment(), OutputLimit: l.config.MaxOutputBytes})
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(remote)) != l.config.RepositoryURL {
		return ErrRepository
	}
	_, err = l.runner.Run(ctx, Command{Path: l.config.GitPath, Args: []string{"fetch", "--prune", "origin", l.config.Branch}, Dir: l.config.CheckoutRoot, Env: l.gitEnvironment(), OutputLimit: l.config.MaxOutputBytes})
	return err
}

func (l *Lifecycle) applyPresets(ctx context.Context) error {
	for _, preset := range l.config.Presets {
		if err := l.runScript(ctx, preset.Body); err != nil {
			return fmt.Errorf("preset %s: %w", preset.Name, err)
		}
	}
	return nil
}
func (l *Lifecycle) applySetup(ctx context.Context) error {
	if strings.TrimSpace(l.config.SetupScript) == "" {
		return nil
	}
	return l.runScript(ctx, l.config.SetupScript)
}
func (l *Lifecycle) runScript(ctx context.Context, body string) error {
	_, err := l.runner.Run(ctx, Command{Path: l.config.ShellPath, Args: []string{"-eu", "-c", body}, Dir: l.config.CheckoutRoot, Env: []string{"HOME=" + l.config.VolumeRoot, "PATH=" + os.Getenv("PATH")}, OutputLimit: l.config.MaxOutputBytes})
	return err
}

func validate(c Config) error {
	if !filepath.IsAbs(c.VolumeRoot) || !filepath.IsAbs(c.CheckoutRoot) || c.CheckoutRoot == c.VolumeRoot || !pathWithin(c.VolumeRoot, c.CheckoutRoot) || !safeIdentifier(c.ProjectID) || !safeBranch(c.Branch) || c.OperationTimeout <= 0 || c.FlushTimeout <= 0 || c.MaxScriptBytes <= 0 || c.MaxOutputBytes <= 0 || !filepath.IsAbs(c.GitPath) || !filepath.IsAbs(c.ShellPath) {
		return ErrInvalid
	}
	u, err := url.Parse(c.RepositoryURL)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Hostname() == "" || !slices.Contains(c.AllowedRepositoryHosts, strings.ToLower(u.Hostname())) {
		return ErrRepository
	}
	validName := regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	total := len(c.SetupScript)
	for _, preset := range c.Presets {
		if !validName.MatchString(preset.Name) {
			return ErrInvalid
		}
		total += len(preset.Body)
	}
	if total > c.MaxScriptBytes {
		return ErrInvalid
	}
	return nil
}

func secureDirectory(path string) error {
	clean := filepath.Clean(path)
	if info, err := os.Lstat(clean); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return ErrIdentity
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.MkdirAll(clean, 0o700)
}
func pathWithin(root, child string) bool {
	relative, err := filepath.Rel(filepath.Clean(root), filepath.Clean(child))
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
func safeIdentifier(value string) bool {
	return regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`).MatchString(value)
}
func safeBranch(value string) bool {
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/-]{0,199}$`).MatchString(value) {
		return false
	}
	return !strings.Contains(value, "..") && !strings.Contains(value, "@{") && !strings.Contains(value, "//") && !strings.HasSuffix(value, "/") && !strings.HasSuffix(value, ".") && !strings.HasSuffix(value, ".lock")
}
func (l *Lifecycle) gitEnvironment() []string {
	env := []string{"GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "PATH=" + os.Getenv("PATH")}
	if l.config.GitToken == "" {
		return env
	}
	u, _ := url.Parse(l.config.RepositoryURL)
	auth := base64.StdEncoding.EncodeToString([]byte("x-access-token:" + l.config.GitToken))
	return append(env, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.https://"+u.Hostname()+"/.extraheader", "GIT_CONFIG_VALUE_0=Authorization: Basic "+auth)
}
func errorCode(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, ErrIdentity):
		return "workspace_identity_mismatch"
	case errors.Is(err, ErrRepository):
		return "repository_rejected"
	case errors.Is(err, ErrOutputLimit):
		return "output_limit"
	default:
		return "stage_failed"
	}
}
