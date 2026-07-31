package configsync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var ErrConfigRepositoryInvalid = errors.New("invalid config repository")

type ChezmoiSourceConfig struct {
	Binary      string
	RuntimeRoot string
	SourceRoot  string
	HomeRoot    string
}

type ChezmoiSource struct {
	binary     string
	configPath string
	sourceRoot string
	homeRoot   string
}

func NewChezmoiSource(config ChezmoiSourceConfig) (*ChezmoiSource, error) {
	if strings.TrimSpace(config.Binary) == "" ||
		!canonicalAbsolutePath(config.RuntimeRoot) || !canonicalAbsolutePath(config.SourceRoot) ||
		!canonicalAbsolutePath(config.HomeRoot) {
		return nil, ErrConfigRepositoryInvalid
	}
	return &ChezmoiSource{
		binary: config.Binary, configPath: filepath.Join(config.RuntimeRoot, "chezmoi.toml"),
		sourceRoot: config.SourceRoot, homeRoot: config.HomeRoot,
	}, nil
}

func (s *ChezmoiSource) Apply(ctx context.Context) error {
	return s.ApplyPaths(ctx, nil)
}

func (s *ChezmoiSource) ApplyPaths(ctx context.Context, paths []string) error {
	if err := ValidateConfigRepository(s.sourceRoot); err != nil {
		return err
	}
	if err := s.writeConfig(); err != nil {
		return err
	}
	arguments := []string{"--config", s.configPath, "apply", "--force", "--no-tty"}
	if len(paths) > 0 {
		arguments = append(arguments, "--")
		for _, path := range paths {
			if !safeRelativeStatusPath(path) {
				return ErrConfigRepositoryInvalid
			}
			arguments = append(arguments, filepath.Join(s.homeRoot, filepath.FromSlash(path)))
		}
	}
	return runChezmoi(ctx, s.binary, arguments...)
}

func (s *ChezmoiSource) Add(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	if err := s.writeConfig(); err != nil {
		return err
	}
	arguments := []string{"--config", s.configPath, "add", "--"}
	for _, path := range paths {
		if !safeRelativeStatusPath(path) {
			return ErrConfigRepositoryInvalid
		}
		arguments = append(arguments, filepath.Join(s.homeRoot, filepath.FromSlash(path)))
	}
	return runChezmoi(ctx, s.binary, arguments...)
}

func (s *ChezmoiSource) Forget(ctx context.Context, path string) error {
	if !safeRelativeStatusPath(path) {
		return ErrConfigRepositoryInvalid
	}
	if err := s.writeConfig(); err != nil {
		return err
	}
	return runChezmoi(ctx, s.binary, "--config", s.configPath, "forget", "--", filepath.Join(s.homeRoot, filepath.FromSlash(path)))
}

func (s *ChezmoiSource) writeConfig() error {
	if err := os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
		return err
	}
	var body bytes.Buffer
	_, _ = fmt.Fprintf(&body, "sourceDir = %q\ndestDir = %q\n", s.sourceRoot, s.homeRoot)
	return writePrivateAtomic(s.configPath, body.Bytes())
}

func ValidateConfigRepository(root string) error {
	if !canonicalAbsolutePath(root) {
		return ErrConfigRepositoryInvalid
	}
	return filepath.WalkDir(root, func(full string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(root, full)
		if err != nil || relative == "." {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == ".git" || strings.HasPrefix(relative, ".git/") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: repository symlink at %q", ErrConfigRepositoryInvalid, relative)
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".tmpl") {
			return fmt.Errorf("%w: template at %q", ErrConfigRepositoryInvalid, relative)
		}
		for _, unsafe := range []string{
			"run_", "run_once_", "run_onchange_", "modify_", "external_", "remove_",
			"create_", "exact_", "executable_",
		} {
			if strings.HasPrefix(name, unsafe) || strings.Contains(name, "_"+unsafe) {
				return fmt.Errorf("%w: executable attribute at %q", ErrConfigRepositoryInvalid, relative)
			}
		}
		if !entry.IsDir() {
			info, infoErr := entry.Info()
			if infoErr != nil || !info.Mode().IsRegular() {
				return errors.Join(ErrConfigRepositoryInvalid, infoErr)
			}
		}
		return nil
	})
}

func writePrivateAtomic(path string, data []byte) error {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrConfigRepositoryInvalid, err)
	}
	temporary, err := os.CreateTemp(parent, ".paperboat-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err = temporary.Chmod(0o600); err == nil {
		_, err = temporary.Write(data)
	}
	if err == nil {
		err = temporary.Sync()
	}
	err = errors.Join(err, temporary.Close())
	if err != nil {
		return err
	}
	return os.Rename(temporaryPath, path)
}

func canonicalAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path
}

func runChezmoi(ctx context.Context, binary string, arguments ...string) error {
	command := exec.CommandContext(ctx, binary, arguments...)
	command.Env = append(os.Environ(), "CHEZMOI_NO_PAGER=1")
	if _, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("%w: chezmoi operation failed", ErrConfigRepositoryInvalid)
	}
	return nil
}
