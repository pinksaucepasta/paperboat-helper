package config

import (
	"path/filepath"
	"testing"
)

func TestFromEnvDefaultsToBYOD(t *testing.T) {
	c, err := FromEnv("1.0.0", func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if c.Profile != BYOD || c.Limits != DefaultLimits || c.Resources != DefaultResources {
		t.Fatalf("config = %#v", c)
	}
	if c.UploadRoot == "" || c.UploadRoot == filepath.Join(c.StateRoot, "uploads") || filepath.Base(c.UploadRoot) != "uploads" {
		t.Fatalf("upload cache root = %q, state root = %q", c.UploadRoot, c.StateRoot)
	}
}

func TestFromEnvAcceptsIndependentAbsoluteUploadRoot(t *testing.T) {
	c, err := FromEnv("1", func(key string) string {
		switch key {
		case "PAPERBOAT_HELPER_STATE_ROOT":
			return "/srv/paperboat/state"
		case "PAPERBOAT_HELPER_UPLOAD_ROOT":
			return "/srv/paperboat/cache/uploads"
		default:
			return ""
		}
	})
	if err != nil || c.EffectiveUploadRoot() != "/srv/paperboat/cache/uploads" {
		t.Fatalf("config=%#v err=%v", c, err)
	}
}

func TestValidateResourceLimitsAreCompleteAndBounded(t *testing.T) {
	base := Config{Profile: BYOD, StateRoot: "/tmp/helper", Version: "1", Limits: DefaultLimits, Resources: DefaultResources}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []func(*ResourceLimits){
		func(value *ResourceLimits) { value.MaxSessions = 0 },
		func(value *ResourceLimits) { value.MaxAttachments = 65 },
		func(value *ResourceLimits) { value.MaxInputDecisions = 100_001 },
		func(value *ResourceLimits) { value.HistoryBytes = (64 << 20) + 1 },
		func(value *ResourceLimits) { value.MaxConcurrentUploads = 17 },
		func(value *ResourceLimits) { value.MaxPreviewTargets = 1025 },
		func(value *ResourceLimits) { value.MaxConcurrentProbes = 65 },
		func(value *ResourceLimits) { value.MaxConcurrentOps = 257 },
	}
	for index, mutate := range cases {
		candidate := base
		mutate(&candidate.Resources)
		if err := candidate.Validate(); err == nil {
			t.Fatalf("case %d accepted: %#v", index, candidate.Resources)
		}
	}
}

func TestValidateRejectsHostedLifecycleUnsafeLimits(t *testing.T) {
	c := Config{Profile: Hosted, StateRoot: "/tmp/helper", Version: "1", Limits: DefaultLimits}
	c.Limits.MutationDeadline = DefaultLimits.MutationDeadline + 1
	if err := c.Validate(); err == nil {
		t.Fatal("expected invalid limits")
	}
}
