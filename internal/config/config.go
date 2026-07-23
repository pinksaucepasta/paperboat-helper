package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	Profile   Profile
	StateRoot string
	Version   string
	Limits    Limits
	Resources ResourceLimits
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
	MaxActivityEvents    int
}

var DefaultResources = ResourceLimits{MaxSessions: 64, MaxAttachments: 16, MaxInputDecisions: 10_000, HistoryBytes: 64 << 10, MaxConcurrentUploads: 2, MaxPreviewTargets: 128, MaxConcurrentProbes: 8, MaxConcurrentOps: 32, MaxActivityEvents: 1000}

func (c Config) Validate() error {
	if c.Profile != Hosted && c.Profile != BYOD {
		return fmt.Errorf("profile: %w", ErrInvalid)
	}
	if c.StateRoot == "" || !filepath.IsAbs(c.StateRoot) {
		return fmt.Errorf("state root must be an absolute path: %w", ErrInvalid)
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
	if resources.MaxSessions < 1 || resources.MaxSessions > 256 || resources.MaxAttachments < 1 || resources.MaxAttachments > 64 || resources.MaxInputDecisions < 1 || resources.MaxInputDecisions > 100_000 || resources.HistoryBytes < 1 || resources.HistoryBytes > 64<<20 || resources.MaxConcurrentUploads < 1 || resources.MaxConcurrentUploads > 16 || resources.MaxPreviewTargets < 1 || resources.MaxPreviewTargets > 1024 || resources.MaxConcurrentProbes < 1 || resources.MaxConcurrentProbes > 64 || resources.MaxConcurrentOps < 1 || resources.MaxConcurrentOps > 256 || resources.MaxActivityEvents < 1 || resources.MaxActivityEvents > 10_000 {
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
		base, err := os.UserConfigDir()
		if err != nil {
			return Config{}, fmt.Errorf("state root: %w", err)
		}
		root = filepath.Join(base, "paperboat", "helper")
	}
	c := Config{Profile: profile, StateRoot: root, Version: version, Limits: DefaultLimits, Resources: DefaultResources}
	return c, c.Validate()
}

var ErrInvalid = errors.New("invalid configuration")
