package configsync

import (
	"errors"
	"testing"
	"time"
)

func TestValidateRuntimeDescriptorAcceptsEmptyManifestContract(t *testing.T) {
	credential := Credential{
		AssignmentID: "assignment", EnvironmentID: "environment", HelperID: "helper",
		WarningRevision: "warning",
	}
	descriptor := RuntimeDescriptor{
		WriteMode: "read_only", Mode: ModePullOnly, RepositoryID: "repository", AssignmentID: credential.AssignmentID,
		EnvironmentID: credential.EnvironmentID, HelperID: credential.HelperID, HelperGeneration: 1,
		WarningRevision: credential.WarningRevision,
		Policy: RuntimePolicy{
			Format: "paperboat-config-plaintext-v1", Revision: "policy",
			ManifestContract: ManifestContractVersion, ManifestMaxBytes: DefaultManifestMaxBytes,
			ManifestMaxLines: DefaultManifestMaxLines, ManifestMaxPatternBytes: DefaultManifestMaxPatternBytes,
			MaxFileBytes: 1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Minute, RetryLimit: 1,
			ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}

	if err := validateRuntimeDescriptor(descriptor, credential); err != nil {
		t.Fatalf("manifest descriptor rejected: %v", err)
	}
	descriptor.Policy.ManifestMaxLines = 0
	if err := validateRuntimeDescriptor(descriptor, credential); !errors.Is(err, ErrPolicyInvalid) {
		t.Fatalf("invalid manifest limit error = %v", err)
	}
}

func TestMandatoryExcludedHonorsExactRuntimeRoots(t *testing.T) {
	policy := RuntimePolicy{RuntimeExclusionRoots: []string{"Library/Application Support/paperboat/helper[state]"}}
	if !mandatoryExcluded("Library/Application Support/paperboat/helper[state]/identity.json", policy) {
		t.Fatal("runtime identity path was not excluded")
	}
	if mandatoryExcluded("Library/Application Support/paperboat/helper-state/identity.json", policy) {
		t.Fatal("runtime exclusion matched a sibling prefix")
	}
}
