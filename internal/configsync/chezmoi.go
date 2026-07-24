package configsync

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

var ErrEncryptedRepositoryInvalid = errors.New("invalid encrypted config repository")

type EncryptedRepositoryFormat struct {
	Format     string `json:"format"`
	Version    int    `json:"version"`
	KeyVersion int64  `json:"key_version"`
	Recipient  string `json:"recipient"`
}

type ChezmoiSourceConfig struct {
	Binary       string
	RuntimeRoot  string
	SourceRoot   string
	HomeRoot     string
	IdentityPath string
	Recipient    string
}

type ChezmoiSource struct {
	binary       string
	configPath   string
	sourceRoot   string
	homeRoot     string
	identityPath string
	recipient    string
}

func NewChezmoiSource(config ChezmoiSourceConfig) (*ChezmoiSource, error) {
	if strings.TrimSpace(config.Binary) == "" ||
		!canonicalAbsolutePath(config.RuntimeRoot) || !canonicalAbsolutePath(config.SourceRoot) ||
		!canonicalAbsolutePath(config.HomeRoot) || !canonicalAbsolutePath(config.IdentityPath) ||
		strings.TrimSpace(config.Recipient) == "" {
		return nil, ErrEncryptedRepositoryInvalid
	}
	return &ChezmoiSource{
		binary: config.Binary, configPath: filepath.Join(config.RuntimeRoot, "chezmoi.toml"),
		sourceRoot: config.SourceRoot, homeRoot: config.HomeRoot,
		identityPath: config.IdentityPath, recipient: config.Recipient,
	}, nil
}

func (s *ChezmoiSource) Apply(ctx context.Context) error {
	return s.ApplyPaths(ctx, nil)
}

func (s *ChezmoiSource) ApplyPaths(ctx context.Context, paths []string) error {
	if err := ValidateEncryptedRepository(s.sourceRoot); err != nil {
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
				return ErrEncryptedRepositoryInvalid
			}
			arguments = append(arguments, filepath.Join(s.homeRoot, filepath.FromSlash(path)))
		}
	}
	return runChezmoi(ctx, s.binary, arguments...)
}

func (s *ChezmoiSource) AddEncrypted(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	if err := s.writeConfig(); err != nil {
		return err
	}
	arguments := []string{"--config", s.configPath, "add", "--encrypt", "--"}
	for _, path := range paths {
		if !safeRelativeStatusPath(path) {
			return ErrEncryptedRepositoryInvalid
		}
		arguments = append(arguments, filepath.Join(s.homeRoot, filepath.FromSlash(path)))
	}
	return runChezmoi(ctx, s.binary, arguments...)
}

func (s *ChezmoiSource) Forget(ctx context.Context, path string) error {
	if !safeRelativeStatusPath(path) {
		return ErrEncryptedRepositoryInvalid
	}
	if err := s.writeConfig(); err != nil {
		return err
	}
	return runChezmoi(ctx, s.binary, "--config", s.configPath, "forget", "--", filepath.Join(s.homeRoot, filepath.FromSlash(path)))
}

func (s *ChezmoiSource) writeConfig() error {
	identity, err := os.Lstat(s.identityPath)
	if err != nil || !identity.Mode().IsRegular() || identity.Mode()&os.ModeSymlink != 0 || identity.Mode().Perm()&0o077 != 0 {
		return errors.Join(ErrEncryptedRepositoryInvalid, err)
	}
	if err := os.MkdirAll(filepath.Dir(s.configPath), 0o700); err != nil {
		return err
	}
	var body bytes.Buffer
	_, _ = fmt.Fprintf(&body,
		"sourceDir = %q\ndestDir = %q\nencryption = \"age\"\n\n[age]\nidentity = %q\nrecipient = %q\n",
		s.sourceRoot, s.homeRoot, s.identityPath, s.recipient)
	return writePrivateAtomic(s.configPath, body.Bytes())
}

func ReadEncryptedRepositoryFormat(root string) (EncryptedRepositoryFormat, error) {
	if !canonicalAbsolutePath(root) {
		return EncryptedRepositoryFormat{}, ErrEncryptedRepositoryInvalid
	}
	path := filepath.Join(root, ".paperboat", "format.json")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 4096 {
		return EncryptedRepositoryFormat{}, errors.Join(ErrEncryptedRepositoryInvalid, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return EncryptedRepositoryFormat{}, err
	}
	var format EncryptedRepositoryFormat
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&format) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		format.Format != "paperboat-chezmoi-age" || format.Version != 1 ||
		format.KeyVersion < 1 || !strings.HasPrefix(format.Recipient, "age1") {
		return EncryptedRepositoryFormat{}, ErrEncryptedRepositoryInvalid
	}
	return format, nil
}

func WriteEncryptedRepositoryFormat(root string, format EncryptedRepositoryFormat) error {
	if !canonicalAbsolutePath(root) || format.Format != "paperboat-chezmoi-age" ||
		format.Version != 1 || format.KeyVersion < 1 || !strings.HasPrefix(format.Recipient, "age1") {
		return ErrEncryptedRepositoryInvalid
	}
	parent := filepath.Join(root, ".paperboat")
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(format, "", "  ")
	if err != nil {
		return err
	}
	return writePrivateAtomic(filepath.Join(parent, "format.json"), append(data, '\n'))
}

func ValidateEncryptedRepository(root string) error {
	if _, err := ReadEncryptedRepositoryFormat(root); err != nil {
		return err
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
			return fmt.Errorf("%w: repository symlink at %q", ErrEncryptedRepositoryInvalid, relative)
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".tmpl") {
			return fmt.Errorf("%w: template at %q", ErrEncryptedRepositoryInvalid, relative)
		}
		for _, unsafe := range []string{
			"run_", "run_once_", "run_onchange_", "modify_", "external_", "remove_",
			"create_", "exact_", "executable_",
		} {
			if strings.HasPrefix(name, unsafe) || strings.Contains(name, "_"+unsafe) {
				return fmt.Errorf("%w: executable attribute at %q", ErrEncryptedRepositoryInvalid, relative)
			}
		}
		if !entry.IsDir() {
			info, infoErr := entry.Info()
			if infoErr != nil || !info.Mode().IsRegular() {
				return errors.Join(ErrEncryptedRepositoryInvalid, infoErr)
			}
		}
		return nil
	})
}

func WriteAgeIdentity(path, identities string) error {
	if !canonicalAbsolutePath(path) || strings.TrimSpace(identities) == "" {
		return ErrEncryptedRepositoryInvalid
	}
	if _, err := os.Lstat(path); err == nil {
		return ErrEncryptedRepositoryInvalid
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := io.WriteString(file, strings.TrimSpace(identities)+"\n")
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(path)
		return errors.Join(writeErr, syncErr, closeErr)
	}
	return nil
}

func EnsureAgeIdentity(path, identities string) error {
	if !canonicalAbsolutePath(path) || strings.TrimSpace(identities) == "" {
		return ErrEncryptedRepositoryInvalid
	}
	expected := []byte(strings.TrimSpace(identities) + "\n")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return WriteAgeIdentity(path, identities)
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return errors.Join(ErrEncryptedRepositoryInvalid, err)
	}
	current, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(current) == len(expected) && subtle.ConstantTimeCompare(current, expected) == 1 {
		return nil
	}
	return writePrivateAtomic(path, expected)
}

func writePrivateAtomic(path string, data []byte) error {
	parent := filepath.Dir(path)
	info, err := os.Lstat(parent)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrEncryptedRepositoryInvalid, err)
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
		return fmt.Errorf("%w: chezmoi operation failed", ErrEncryptedRepositoryInvalid)
	}
	return nil
}
