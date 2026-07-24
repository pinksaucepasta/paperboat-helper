package configsync

import (
	"errors"
	"testing"
	"time"

	"filippo.io/age"
)

func TestValidateRuntimeDescriptorRejectsEmptyIncludes(t *testing.T) {
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	credential := Credential{
		AssignmentID: "assignment", EnvironmentID: "environment", HelperID: "helper",
		WarningRevision: "warning",
	}
	descriptor := RuntimeDescriptor{
		WriteMode: "read_only", RepositoryID: "repository", AssignmentID: credential.AssignmentID,
		EnvironmentID: credential.EnvironmentID, HelperID: credential.HelperID, HelperGeneration: 1,
		WarningRevision: credential.WarningRevision, KeyVersion: 1,
		AgeRecipient: identity.Recipient().String(), AgeIdentities: identity.String(),
		Policy: RuntimePolicy{
			Format: "paperboat-chezmoi-age-v1", Revision: "policy",
			MandatoryExclusions: append([]string(nil), requiredMandatoryExclusions...),
			MaxFileBytes:        1 << 20, MaxBatchBytes: 2 << 20, Debounce: time.Second,
			MinimumPushInterval: time.Minute, MaximumDirtyDelay: time.Minute,
			RemotePollInterval: time.Minute, RetryLimit: 1,
			ShutdownFlushTimeout: time.Second, SummaryLimit: 10,
		},
	}

	if err := validateRuntimeDescriptor(descriptor, credential); !errors.Is(err, ErrPolicyInvalid) {
		t.Fatalf("validation error = %v, want %v", err, ErrPolicyInvalid)
	}
	descriptor.Policy.Includes = []string{".bashrc"}
	if err := validateRuntimeDescriptor(descriptor, credential); err != nil {
		t.Fatalf("explicit include rejected: %v", err)
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
