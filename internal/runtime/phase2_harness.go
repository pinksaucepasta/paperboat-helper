//go:build darwin || linux

package runtime

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/process"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/server"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

const maxPhase2HarnessConfigBytes = 64 << 10

type Phase2HarnessConfig struct {
	Profile          helperconfig.Profile `json:"profile"`
	StateRoot        string               `json:"state_root"`
	WorkspaceRoot    string               `json:"workspace_root"`
	ListenAddress    string               `json:"listen_address"`
	ShellPath        string               `json:"shell_path"`
	ShellArgs        []string             `json:"shell_args"`
	ShellEnvironment []string             `json:"shell_environment"`
	OriginPatterns   []string             `json:"origin_patterns"`
	Issuer           string               `json:"issuer"`
	EnvironmentID    string               `json:"environment_id"`
	HelperID         string               `json:"helper_id"`
	PublicKeys       map[string]string    `json:"public_keys"`
	RevokedJTIs      []string             `json:"revoked_jtis"`
	ConfigApplyProof bool                 `json:"config_apply_proof"`
}

func LoadPhase2HarnessConfig(path string) (Phase2HarnessConfig, error) {
	if !filepath.IsAbs(path) {
		return Phase2HarnessConfig{}, ErrHelperInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return Phase2HarnessConfig{}, errors.Join(ErrHelperInvalid, err)
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 || info.Size() < 2 || info.Size() > maxPhase2HarnessConfigBytes {
		return Phase2HarnessConfig{}, ErrHelperInvalid
	}
	file, err := os.Open(path)
	if err != nil {
		return Phase2HarnessConfig{}, err
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, maxPhase2HarnessConfigBytes+1))
	if err != nil || len(encoded) > maxPhase2HarnessConfigBytes {
		return Phase2HarnessConfig{}, errors.Join(ErrHelperInvalid, err)
	}
	var config Phase2HarnessConfig
	if err := decodePhase2HarnessJSON(encoded, &config); err != nil || validatePhase2HarnessConfig(config) != nil {
		return Phase2HarnessConfig{}, ErrHelperInvalid
	}
	return config, nil
}

func NewPhase2Harness(ctx context.Context, config Phase2HarnessConfig, version string) (*Helper, error) {
	if validatePhase2HarnessConfig(config) != nil || version == "" {
		return nil, ErrHelperInvalid
	}
	keys := make(map[string]ed25519.PublicKey, len(config.PublicKeys))
	for keyID, encoded := range config.PublicKeys {
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize || len(keyID) < 1 || len(keyID) > 128 {
			return nil, ErrHelperInvalid
		}
		keys[keyID] = ed25519.PublicKey(key)
	}
	authorizer, err := NewStaticAuthorizer(StaticAuthConfig{Issuer: config.Issuer, EnvironmentID: config.EnvironmentID, HelperID: config.HelperID, Keys: keys, RevokedJTIs: config.RevokedJTIs, Clock: health.RealClock{}})
	if err != nil {
		return nil, err
	}
	previews, err := preview.New(preview.Config{Prober: preview.TCPProber{Dialer: net.Dialer{Timeout: 2 * time.Second}}, MaxTargets: helperconfig.DefaultResources.MaxPreviewTargets, MaxConcurrentProbes: helperconfig.DefaultResources.MaxConcurrentProbes})
	if err != nil {
		return nil, err
	}
	monitor, err := preview.NewMonitor(preview.MonitorConfig{Registry: previews})
	if err != nil {
		return nil, err
	}
	collector, err := activity.New(activity.Config{MaxQueued: helperconfig.DefaultResources.MaxActivityEvents})
	if err != nil {
		return nil, err
	}
	var configHandler configapply.Handler
	if config.ConfigApplyProof {
		configHandler = configapply.ConformanceHandler{}
	}
	static := helperconfig.Config{Profile: config.Profile, StateRoot: config.StateRoot, Version: version, Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
	return NewHelper(ctx, HelperConfig{
		Runtime: static, ListenAddress: config.ListenAddress, WorkspaceRoot: config.WorkspaceRoot,
		OriginPatterns: config.OriginPatterns, EnvironmentID: config.EnvironmentID,
	}, HelperDependencies{
		Authorizer: authorizer, Previews: previews, PreviewService: monitor,
		Activity: collector, ConfigApply: configHandler, ConfigApplyProof: config.ConfigApplyProof,
		SessionLauncherFactory: commandSessionLauncherFactory(config.ShellPath, config.ShellArgs, config.ShellEnvironment),
	})
}

type commandSessionLauncher struct {
	sessions *session.Manager
	path     string
	args     []string
	env      []string
}

func (l commandSessionLauncher) Launch(ctx context.Context, request process.LaunchRequest) (session.Snapshot, error) {
	return l.sessions.Create(ctx, session.CreateRequest{ID: request.ID, Name: request.Name, Command: pty.Command{Path: l.path, Args: append([]string(nil), l.args...), Env: append([]string(nil), l.env...), CWD: request.CWD, Dimensions: request.Dimensions}})
}

func commandSessionLauncherFactory(path string, args, env []string) func(*session.Manager) (server.SessionLauncher, error) {
	return func(sessions *session.Manager) (server.SessionLauncher, error) {
		resolved, err := pty.ValidateProcessPolicy(path, args, env)
		if err != nil {
			return nil, err
		}
		return commandSessionLauncher{sessions: sessions, path: resolved, args: args, env: env}, nil
	}
}

func validatePhase2HarnessConfig(config Phase2HarnessConfig) error {
	issuer, err := url.Parse(config.Issuer)
	if err != nil || issuer.Scheme != "https" || issuer.User != nil || issuer.Hostname() == "" || issuer.Fragment != "" || !validHarnessID(config.EnvironmentID) || !validHarnessID(config.HelperID) || len(config.PublicKeys) == 0 || len(config.PublicKeys) > 64 || len(config.OriginPatterns) > 32 {
		return ErrHelperInvalid
	}
	static := helperconfig.Config{Profile: config.Profile, StateRoot: config.StateRoot, Version: "phase2", Limits: helperconfig.DefaultLimits, Resources: helperconfig.DefaultResources}
	if err := static.Validate(); err != nil || !filepath.IsAbs(config.WorkspaceRoot) || !LoopbackAddress(config.ListenAddress) || config.ShellPath == "" {
		return ErrHelperInvalid
	}
	for _, pattern := range config.OriginPatterns {
		if len(pattern) < 1 || len(pattern) > 253 || bytes.IndexAny([]byte(pattern), "\x00\r\n") >= 0 {
			return ErrHelperInvalid
		}
	}
	for keyID, encoded := range config.PublicKeys {
		key, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize || len(keyID) < 1 || len(keyID) > 128 {
			return ErrHelperInvalid
		}
	}
	revoked := make(map[string]bool, len(config.RevokedJTIs))
	for _, jti := range config.RevokedJTIs {
		if jti == "" || revoked[jti] {
			return ErrHelperInvalid
		}
		revoked[jti] = true
	}
	return nil
}

func validHarnessID(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func decodePhase2HarnessJSON(encoded []byte, target any) error {
	if err := rejectPhase2HarnessDuplicates(encoded); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrHelperInvalid
	}
	return nil
}

func rejectPhase2HarnessDuplicates(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return ErrHelperInvalid
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return ErrHelperInvalid
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrHelperInvalid
	}
	return nil
}
