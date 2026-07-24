package contracttest

import (
	"encoding/json"
	"os"
	"testing"
)

func TestConfigSyncLifecycleVectorsFailClosed(t *testing.T) {
	t.Parallel()
	data, err := os.ReadFile("../../testdata/contracts/fixtures/config-sync/lifecycle.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SchemaVersion string `json:"schema_version"`
		Vectors       []struct {
			ID              string         `json:"id"`
			Operation       string         `json:"operation"`
			Current         map[string]any `json:"current"`
			Presented       map[string]any `json:"presented"`
			Accepted        bool           `json:"accepted"`
			MutationAllowed bool           `json:"mutation_allowed"`
			Error           string         `json:"error"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != "paperboat.config-sync-lifecycle/v1" {
		t.Fatalf("schema_version = %q", fixture.SchemaVersion)
	}
	required := map[string]bool{
		"eligible-current": false, "stale-warning": false, "stale-assignment": false,
		"stale-fence": false, "remote-moved": false, "stale-status": false,
		"stale-conflict": false, "offline-key-overlap": false,
		"unknown-contract-version": false, "revoked-helper": false,
	}
	seen := make(map[string]bool, len(fixture.Vectors))
	for _, vector := range fixture.Vectors {
		if vector.ID == "" || seen[vector.ID] || vector.Operation == "" || len(vector.Current) == 0 || len(vector.Presented) == 0 {
			t.Fatalf("invalid lifecycle vector: %#v", vector)
		}
		seen[vector.ID] = true
		if _, ok := required[vector.ID]; ok {
			required[vector.ID] = true
		}
		if !vector.Accepted && (vector.MutationAllowed || vector.Error == "") {
			t.Fatalf("rejection must be typed and pre-mutation: %#v", vector)
		}
	}
	for id, present := range required {
		if !present {
			t.Errorf("missing lifecycle vector %q", id)
		}
	}
}
