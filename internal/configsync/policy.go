package configsync

import (
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"filippo.io/age"
	"github.com/bmatcuk/doublestar/v4"
)

var ErrPolicyInvalid = errors.New("invalid config sync policy")

var requiredMandatoryExclusions = []string{
	".git", "**/.git", ".paperboat", "**/.paperboat", ".ssh", "**/.ssh", ".gnupg", "**/.gnupg",
	".aws", "**/.aws", ".kube", "**/.kube", ".docker/config.json",
	".git-credentials", "**/.git-credentials", ".netrc", "**/.netrc",
	".env", ".env.*", "**/.env", "**/.env.*", "**/credentials", "**/credentials.*",
	"**/*.db", "**/*.sqlite", "**/*.log", "**/*.tmp", "**/*_history",
}

func validateRuntimeDescriptor(descriptor RuntimeDescriptor, credential Credential) error {
	policy := descriptor.Policy
	if (descriptor.WriteMode != "read_only" && descriptor.WriteMode != "leased_writes") ||
		descriptor.RepositoryID == "" || descriptor.AssignmentID != credential.AssignmentID ||
		descriptor.EnvironmentID != credential.EnvironmentID || descriptor.HelperID != credential.HelperID ||
		descriptor.HelperGeneration < 1 || descriptor.SyncRevisionFloor < 0 ||
		descriptor.WarningRevision != credential.WarningRevision || descriptor.KeyVersion < 1 ||
		policy.Format != "paperboat-chezmoi-age-v1" || policy.Revision == "" ||
		policy.MaxFileBytes < 1 || policy.MaxFileBytes > 100<<20 ||
		policy.MaxBatchBytes < policy.MaxFileBytes || policy.MaxBatchBytes > 500<<20 ||
		policy.Debounce < time.Second || policy.Debounce > 5*time.Minute ||
		policy.MinimumPushInterval < time.Minute || policy.MinimumPushInterval > 24*time.Hour ||
		policy.MaximumDirtyDelay < policy.Debounce || policy.MaximumDirtyDelay > 24*time.Hour ||
		policy.RemotePollInterval < time.Second || policy.RemotePollInterval > time.Hour ||
		policy.RetryLimit < 1 || policy.RetryLimit > 20 ||
		policy.ShutdownFlushTimeout < time.Second || policy.ShutdownFlushTimeout > 10*time.Minute ||
		policy.SummaryLimit < 1 || policy.SummaryLimit > 1000 ||
		len(policy.Includes) > 256 || len(policy.Excludes) > 512 || len(policy.MandatoryExclusions) > 1024 {
		return ErrPolicyInvalid
	}
	for _, required := range requiredMandatoryExclusions {
		if !slices.Contains(policy.MandatoryExclusions, required) {
			return ErrPolicyInvalid
		}
	}
	for _, pattern := range append(append(append([]string{}, policy.Includes...), policy.Excludes...), policy.MandatoryExclusions...) {
		if !safePolicyPattern(pattern) {
			return ErrPolicyInvalid
		}
	}
	recipient, err := age.ParseX25519Recipient(strings.TrimSpace(descriptor.AgeRecipient))
	if err != nil || recipient.String() != strings.TrimSpace(descriptor.AgeRecipient) {
		return ErrPolicyInvalid
	}
	identities, err := age.ParseIdentities(strings.NewReader(descriptor.AgeIdentities))
	if err != nil || len(identities) < 1 || len(identities) > 2 {
		return ErrPolicyInvalid
	}
	current, ok := identities[0].(*age.X25519Identity)
	if !ok || current.Recipient().String() != recipient.String() {
		return ErrPolicyInvalid
	}
	if policy.ClassifierEnabled && (policy.ClassifierRevision == "" || policy.ClassifierModelRevision == "") {
		return ErrPolicyInvalid
	}
	return nil
}

func safePolicyPattern(pattern string) bool {
	pattern = filepath.ToSlash(strings.TrimSpace(pattern))
	if pattern == "" || len(pattern) > 512 || strings.HasPrefix(pattern, "/") ||
		strings.Contains(pattern, "\\") || strings.Contains(pattern, "\x00") ||
		pattern == ".." || strings.HasPrefix(pattern, "../") || strings.Contains(pattern, "/../") {
		return false
	}
	return doublestar.ValidatePattern(pattern)
}

func mandatoryExcluded(path string, policy RuntimePolicy) bool {
	path = filepath.ToSlash(filepath.Clean(path))
	if path == "." || strings.HasPrefix(path, "../") || strings.HasPrefix(path, "/") {
		return true
	}
	for _, pattern := range policy.MandatoryExclusions {
		if matched, err := doublestar.Match(pattern, path); err == nil && matched {
			return true
		}
	}
	return false
}
