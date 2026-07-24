//go:build darwin || linux

package runtime

import (
	"errors"
	"testing"

	"github.com/pinksaucepasta/paperboat-helper/internal/configsync"
)

func TestProtectConfigSyncRuntimeStateExcludesStateInsideManagedHome(t *testing.T) {
	descriptor, err := protectConfigSyncRuntimeState(
		configsync.RuntimeDescriptor{},
		"/Users/sailor",
		"/Users/sailor/Library/Application Support/paperboat/helper[state]",
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := descriptor.Policy.RuntimeExclusionRoots; len(got) != 1 || got[0] != "Library/Application Support/paperboat/helper[state]" {
		t.Fatalf("runtime exclusions = %#v", got)
	}
}

func TestProtectConfigSyncRuntimeStateLeavesExternalStateOutsideManagedHome(t *testing.T) {
	descriptor, err := protectConfigSyncRuntimeState(
		configsync.RuntimeDescriptor{}, "/Users/sailor", "/var/lib/paperboat/helper",
	)
	if err != nil || len(descriptor.Policy.RuntimeExclusionRoots) != 0 {
		t.Fatalf("descriptor = %#v, error = %v", descriptor, err)
	}
}

func TestProtectConfigSyncRuntimeStateRejectsManagedHome(t *testing.T) {
	if _, err := protectConfigSyncRuntimeState(
		configsync.RuntimeDescriptor{}, "/Users/sailor", "/Users/sailor",
	); !errors.Is(err, ErrProductionInvalid) {
		t.Fatalf("state root equal to home error = %v", err)
	}
}
