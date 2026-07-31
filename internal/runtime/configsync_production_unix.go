//go:build darwin || linux

package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/configsync"
)

type productionConfigSyncConfig struct {
	ControlURL      string
	ControlHost     string
	RepositoryHosts []string
	HomeRoot        string
	StateRoot       string
	ChezmoiBinary   string
	Identities      configsync.TokenSource
	Proofs          configsync.ProofSource
	OperationID     configsync.OperationIDSource
	Transport       http.RoundTripper
}

func newProductionConfigSync(config productionConfigSyncConfig) (*configsync.Supervisor, error) {
	client, err := configsync.NewControlClient(configsync.ControlClientConfig{
		BaseURL: config.ControlURL, AllowedHosts: []string{config.ControlHost},
		RepositoryHosts: config.RepositoryHosts, Identities: config.Identities,
		Proofs: config.Proofs, OperationID: config.OperationID, Transport: config.Transport,
	})
	if err != nil {
		return nil, err
	}
	return configsync.NewSupervisor(configsync.SupervisorConfig{
		Credentials: client,
		Factory: func(ctx context.Context, credential configsync.Credential) (configsync.Runtime, error) {
			descriptor, err := client.RuntimeDescriptor(ctx)
			if err != nil {
				return nil, err
			}
			if descriptor.AssignmentID != credential.AssignmentID ||
				descriptor.EnvironmentID != credential.EnvironmentID ||
				descriptor.HelperID != credential.HelperID ||
				descriptor.WarningRevision != credential.WarningRevision {
				return nil, configsync.ErrAuthorization
			}
			descriptor, err = protectConfigSyncRuntimeState(descriptor, config.HomeRoot, config.StateRoot)
			if err != nil {
				return nil, err
			}
			hash := sha256.Sum256([]byte(descriptor.AssignmentID))
			assignmentRoot := filepath.Join(config.StateRoot, "config-sync", hex.EncodeToString(hash[:16]))
			if err := os.MkdirAll(assignmentRoot, 0o700); err != nil {
				return nil, err
			}
			assignmentRoot, err = filepath.EvalSymlinks(assignmentRoot)
			if err != nil {
				return nil, err
			}
			chezmoiBinary, err := ensureChezmoi(
				ctx, config.ChezmoiBinary, assignmentRoot,
				&http.Client{Timeout: 2 * time.Minute},
			)
			if err != nil {
				return nil, err
			}
			repositoryRoot := filepath.Join(assignmentRoot, "repository")
			reconciler, err := configsync.NewPlaintextWorkspaceReconciler(configsync.WorkspaceReconcilerConfig{
				HomeRoot: config.HomeRoot, StateRoot: assignmentRoot, Descriptor: descriptor,
				Resolutions: client, ChezmoiBinary: chezmoiBinary,
			})
			if err != nil {
				return nil, err
			}
			repository, err := configsync.NewGitRepository(configsync.GitRepositoryConfig{
				Root: repositoryRoot, Access: client, Reconciler: reconciler,
			})
			if err != nil {
				return nil, err
			}
			publisher, err := configsync.NewPublisher(configsync.PublisherConfig{
				Authority: client, Repository: repository,
			})
			if err != nil {
				return nil, err
			}
			return configsync.NewEngine(configsync.EngineConfig{
				HomeRoot: config.HomeRoot, Descriptor: descriptor, Syncer: publisher, Statuses: client,
				Diagnostics: reconciler, Manifest: reconciler,
				StatusPath: filepath.Join(assignmentRoot, "status.json"),
			})
		},
	})
}

func protectConfigSyncRuntimeState(
	descriptor configsync.RuntimeDescriptor,
	homeRoot string,
	stateRoot string,
) (configsync.RuntimeDescriptor, error) {
	relative, err := filepath.Rel(homeRoot, stateRoot)
	if err != nil {
		return configsync.RuntimeDescriptor{}, errors.Join(ErrProductionInvalid, err)
	}
	if relative == "." {
		return configsync.RuntimeDescriptor{}, errors.Join(ErrProductionInvalid, errors.New("config sync state root cannot be the managed home"))
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return descriptor, nil
	}
	relative = filepath.ToSlash(relative)
	for _, existing := range descriptor.Policy.RuntimeExclusionRoots {
		if existing == relative {
			return descriptor, nil
		}
	}
	descriptor.Policy.RuntimeExclusionRoots = append(descriptor.Policy.RuntimeExclusionRoots, relative)
	return descriptor, nil
}

func productionConfigHome(profileHosted bool, hostedRoot string) (string, error) {
	root := hostedRoot
	if !profileHosted {
		var err error
		root, err = os.UserHomeDir()
		if err != nil {
			return "", err
		}
	}
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", ErrProductionInvalid
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return "", errors.Join(ErrProductionInvalid, err)
	}
	return root, nil
}

func productionRepositoryHosts(value string) ([]string, error) {
	if strings.TrimSpace(value) == "" {
		value = "github.com"
	}
	seen := make(map[string]bool)
	var hosts []string
	for _, item := range strings.Split(value, ",") {
		host := strings.ToLower(strings.TrimSpace(item))
		if host == "" || strings.ContainsAny(host, "/:@") {
			return nil, ErrProductionInvalid
		}
		if !seen[host] {
			seen[host] = true
			hosts = append(hosts, host)
		}
	}
	if len(hosts) == 0 {
		return nil, ErrProductionInvalid
	}
	return hosts, nil
}
