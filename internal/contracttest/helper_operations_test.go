package contracttest

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

func TestNonTerminalOperationVectorCoverage(t *testing.T) {
	required := map[string]bool{
		"upload-valid": false, "upload-traversal": false, "upload-mime-mismatch": false,
		"preview-register": false, "preview-private-request": false,
		"config-stale-revision": false, "readiness-degraded": false,
	}
	f, err := os.Open("../../testdata/contracts/fixtures/helper/operations.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var vector struct {
			Case      string          `json:"case"`
			Valid     bool            `json:"valid"`
			Operation string          `json:"operation"`
			Error     string          `json:"error"`
			Input     json.RawMessage `json:"input"`
			Result    json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &vector); err != nil {
			t.Fatal(err)
		}
		if _, ok := required[vector.Case]; !ok {
			t.Fatalf("unknown operation vector %q", vector.Case)
		}
		if vector.Operation == "" {
			t.Errorf("%s: operation is required", vector.Case)
		}
		if !vector.Valid && vector.Error == "" {
			t.Errorf("%s: negative vector requires typed error", vector.Case)
		}
		if vector.Input == nil && vector.Result == nil {
			t.Errorf("%s: input or result is required", vector.Case)
		}
		required[vector.Case] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for name, seen := range required {
		if !seen {
			t.Errorf("missing operation vector %q", name)
		}
	}
}

func TestPreviewStateContract(t *testing.T) {
	b, err := os.ReadFile("../../testdata/contracts/states/preview.json")
	if err != nil {
		t.Fatal(err)
	}
	var state struct {
		Initial string         `json:"initial"`
		HTTP    map[string]int `json:"http"`
	}
	if err := json.Unmarshal(b, &state); err != nil {
		t.Fatal(err)
	}
	if state.Initial != "registering" || state.HTTP["ready"] != 200 || state.HTTP["expired"] != 410 || state.HTTP["removed"] != 404 {
		t.Fatalf("unexpected preview state contract: %#v", state)
	}
}
