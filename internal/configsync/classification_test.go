package configsync

import (
	"os"
	"testing"
)

func TestClassificationSerializerContainsOnlyAllowlistedMetadata(t *testing.T) {
	snapshot := Snapshot{Files: map[string]FileState{
		".config/tool/settings.json": {Bytes: 12, Mode: 0o600},
		".config/tool/other.toml":    {Bytes: 8, Mode: os.ModeSymlink, Target: "target"},
	}}
	candidates, deletions, err := ClassificationCandidates(snapshot, []string{
		".config/tool/settings.json", ".deleted",
	})
	if err != nil || len(candidates) != 1 || len(deletions) != 1 {
		t.Fatalf("candidates = %#v, deletions = %#v, %v", candidates, deletions, err)
	}
	candidate := candidates[0]
	if candidate.Path != ".config/tool/settings.json" || candidate.LocationClass != "xdg_config" ||
		len(candidate.Siblings) != 1 || candidate.Siblings[0].Name != "other.toml" {
		t.Fatalf("candidate = %#v", candidate)
	}
}

func TestClassificationResponseRequiresExactOrderAndRevisions(t *testing.T) {
	policy := RuntimePolicy{
		Revision: "policy", ClassifierRevision: "classifier", ClassifierModelRevision: "model",
	}
	candidates := []ClassificationCandidate{{Path: ".zshrc"}}
	response := ClassificationResponse{
		Results: []ClassificationResult{{
			Path: ".zshrc", Decision: "portable", Confidence: 1,
			ReasonCode: "catalog_portable", Source: "catalog",
		}},
		PolicyRevision: "policy", ClassifierRevision: "classifier",
		ModelRevision: "model", Health: "healthy",
	}
	if err := ValidateClassificationResponse(response, candidates, policy); err != nil {
		t.Fatal(err)
	}
	response.Results[0].Path = ".other"
	if err := ValidateClassificationResponse(response, candidates, policy); err == nil {
		t.Fatal("mismatched response accepted")
	}
}
