package configsync

import (
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/bmatcuk/doublestar/v4"
)

var ErrPolicyInvalid = errors.New("invalid config sync policy")

var requiredMandatoryExclusions = []string{
	".git", "**/.git", ".paperboat", "**/.paperboat",
	".config/paperboat", ".config/paperboat/**",
	".local/bin/pbh",
	".config/systemd/user/paperboat-helper.service",
	".config/systemd/user/default.target.wants/paperboat-helper.service",
	"Library/LaunchAgents/com.pinksaucepasta.paperboat-helper.plist",
	".ssh", "**/.ssh", ".gnupg", "**/.gnupg",
	".aws", "**/.aws", ".kube", "**/.kube", ".docker/config.json",
	".git-credentials", "**/.git-credentials", ".netrc", "**/.netrc",
	".env", ".env.*", "**/.env", "**/.env.*", "**/credentials", "**/credentials.*",
	"**/*.db", "**/*.sqlite", "**/*.log", "**/*.tmp", "**/*_history",
}

func validateRuntimeDescriptor(descriptor RuntimeDescriptor, credential Credential) error {
	policy := descriptor.Policy
	if (descriptor.WriteMode != "read_only" && descriptor.WriteMode != "leased_writes") || !descriptor.Mode.Valid() ||
		descriptor.RepositoryID == "" || descriptor.AssignmentID != credential.AssignmentID ||
		descriptor.EnvironmentID != credential.EnvironmentID || descriptor.HelperID != credential.HelperID ||
		descriptor.HelperGeneration < 1 || descriptor.SyncRevisionFloor < 0 ||
		descriptor.WarningRevision != credential.WarningRevision ||
		policy.Format != "paperboat-config-plaintext-v1" || policy.Revision == "" ||
		policy.MaxFileBytes < 1 || policy.MaxFileBytes > 100<<20 ||
		policy.MaxBatchBytes < policy.MaxFileBytes || policy.MaxBatchBytes > 500<<20 ||
		policy.Debounce < time.Second || policy.Debounce > 5*time.Minute ||
		policy.MinimumPushInterval < time.Minute || policy.MinimumPushInterval > 24*time.Hour ||
		policy.MaximumDirtyDelay < policy.Debounce || policy.MaximumDirtyDelay > 24*time.Hour ||
		policy.RemotePollInterval < time.Second || policy.RemotePollInterval > time.Hour ||
		policy.RetryLimit < 1 || policy.RetryLimit > 20 ||
		policy.ShutdownFlushTimeout < time.Second || policy.ShutdownFlushTimeout > 10*time.Minute ||
		policy.SummaryLimit < 1 || policy.SummaryLimit > 1000 ||
		policy.ManifestContract != ManifestContractVersion ||
		!validManifestLimits(policy.ManifestLimits()) {
		return ErrPolicyInvalid
	}
	return nil
}

func mandatoryExcluded(path string, policy RuntimePolicy) bool {
	path = filepath.ToSlash(filepath.Clean(path))
	if path == "." || strings.HasPrefix(path, "../") || strings.HasPrefix(path, "/") {
		return true
	}
	for _, root := range policy.RuntimeExclusionRoots {
		root = filepath.ToSlash(filepath.Clean(root))
		if root != "." && root != ".." && !strings.HasPrefix(root, "../") &&
			(path == root || strings.HasPrefix(path, root+"/")) {
			return true
		}
	}
	for _, pattern := range requiredMandatoryExclusions {
		for candidate := path; candidate != "."; candidate = filepath.ToSlash(filepath.Dir(candidate)) {
			if matched, err := doublestar.Match(pattern, candidate); err == nil && matched {
				return true
			}
		}
	}
	return false
}
