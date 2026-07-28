package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type Profile string

const (
	Hosted Profile = "hosted"
	BYOD   Profile = "byod"
)

type Limits struct {
	StructuredFrameBytes uint64
	TerminalFrameBytes   uint64
	PendingOutputBytes   uint64
	HeartbeatInterval    time.Duration
	PeerTimeout          time.Duration
	MutationDeadline     time.Duration
}

var DefaultLimits = Limits{64 << 10, 256 << 10, 1 << 20, 15 * time.Second, 45 * time.Second, 5 * time.Minute}

type Config struct {
	Profile    Profile
	StateRoot  string
	UploadRoot string
	Version    string
	Limits     Limits
	Resources  ResourceLimits
}

type ResourceLimits struct {
	MaxSessions          int
	MaxAttachments       int
	MaxInputDecisions    int
	HistoryBytes         uint64
	MaxConcurrentUploads int
	MaxPreviewTargets    int
	MaxConcurrentProbes  int
	MaxConcurrentOps     int
}

var DefaultResources = ResourceLimits{MaxSessions: 64, MaxAttachments: 16, MaxInputDecisions: 10_000, HistoryBytes: 64 << 10, MaxConcurrentUploads: 2, MaxPreviewTargets: 20, MaxConcurrentProbes: 8, MaxConcurrentOps: 32}

func (c Config) Validate() error {
	if c.Profile != Hosted && c.Profile != BYOD {
		return fmt.Errorf("profile: %w", ErrInvalid)
	}
	if c.StateRoot == "" || !filepath.IsAbs(c.StateRoot) {
		return fmt.Errorf("state root must be an absolute path: %w", ErrInvalid)
	}
	if c.UploadRoot != "" && !filepath.IsAbs(c.UploadRoot) {
		return fmt.Errorf("upload root must be an absolute path: %w", ErrInvalid)
	}
	if c.Version == "" {
		return fmt.Errorf("version: %w", ErrInvalid)
	}
	if c.Limits.StructuredFrameBytes == 0 || c.Limits.StructuredFrameBytes > DefaultLimits.StructuredFrameBytes ||
		c.Limits.TerminalFrameBytes == 0 || c.Limits.TerminalFrameBytes > DefaultLimits.TerminalFrameBytes ||
		c.Limits.PendingOutputBytes == 0 || c.Limits.PendingOutputBytes > DefaultLimits.PendingOutputBytes ||
		c.Limits.HeartbeatInterval <= 0 || c.Limits.PeerTimeout < c.Limits.HeartbeatInterval ||
		c.Limits.MutationDeadline <= 0 || c.Limits.MutationDeadline > DefaultLimits.MutationDeadline {
		return fmt.Errorf("limits exceed protocol bounds: %w", ErrInvalid)
	}
	resources := c.Resources
	if resources == (ResourceLimits{}) {
		resources = DefaultResources
	}
	if resources.MaxSessions < 1 || resources.MaxSessions > 256 || resources.MaxAttachments < 1 || resources.MaxAttachments > 64 || resources.MaxInputDecisions < 1 || resources.MaxInputDecisions > 100_000 || resources.HistoryBytes < 1 || resources.HistoryBytes > 64<<20 || resources.MaxConcurrentUploads < 1 || resources.MaxConcurrentUploads > 16 || resources.MaxPreviewTargets < 1 || resources.MaxPreviewTargets > 20 || resources.MaxConcurrentProbes < 1 || resources.MaxConcurrentProbes > 64 || resources.MaxConcurrentOps < 1 || resources.MaxConcurrentOps > 256 {
		return fmt.Errorf("resource limits exceed runtime bounds: %w", ErrInvalid)
	}
	return nil
}

func FromEnv(version string, environ func(string) string) (Config, error) {
	profile := Profile(environ("PAPERBOAT_HELPER_PROFILE"))
	if profile == "" {
		profile = BYOD
	}
	root := environ("PAPERBOAT_HELPER_STATE_ROOT")
	if root == "" {
		var err error
		root, err = DefaultStateRoot(environ)
		if err != nil {
			return Config{}, err
		}
	}
	uploadRoot := environ("PAPERBOAT_HELPER_UPLOAD_ROOT")
	if uploadRoot == "" {
		var err error
		uploadRoot, err = DefaultUploadRoot(environ)
		if err != nil {
			return Config{}, err
		}
	}
	c := Config{Profile: profile, StateRoot: root, UploadRoot: uploadRoot, Version: version, Limits: DefaultLimits, Resources: DefaultResources}
	return c, c.Validate()
}

// DefaultStateRoot resolves durable runtime state separately from user
// configuration. Linux follows XDG_STATE_HOME; macOS uses Application Support.
func DefaultStateRoot(environ func(string) string) (string, error) {
	if runtime.GOOS == "linux" {
		if base := environ("XDG_STATE_HOME"); base != "" {
			if !filepath.IsAbs(base) {
				return "", fmt.Errorf("XDG_STATE_HOME must be absolute: %w", ErrInvalid)
			}
			return filepath.Join(base, "paperboat", "helper"), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("state root: %w", err)
		}
		return filepath.Join(home, ".local", "state", "paperboat", "helper"), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("state root: %w", err)
	}
	return filepath.Join(base, "Paperboat", "helper"), nil
}

// DefaultUploadRoot resolves short-lived staged images under the OS cache.
func DefaultUploadRoot(environ func(string) string) (string, error) {
	if runtime.GOOS == "linux" {
		if base := environ("XDG_CACHE_HOME"); base != "" {
			if !filepath.IsAbs(base) {
				return "", fmt.Errorf("XDG_CACHE_HOME must be absolute: %w", ErrInvalid)
			}
			return filepath.Join(base, "paperboat", "uploads"), nil
		}
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("upload cache root: %w", err)
	}
	product := "Paperboat"
	if runtime.GOOS == "linux" {
		product = "paperboat"
	}
	return filepath.Join(base, product, "uploads"), nil
}

// EffectiveUploadRoot returns the separately configured cache root. Explicit
// test and embedded configurations retain a deterministic state-root fallback.
func (c Config) EffectiveUploadRoot() string {
	if c.UploadRoot != "" {
		return c.UploadRoot
	}
	return filepath.Join(c.StateRoot, "uploads")
}

var ErrInvalid = errors.New("invalid configuration")
