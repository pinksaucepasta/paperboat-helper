package contracttest

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

func TestEdgeControlContractCoverage(t *testing.T) {
	f, err := os.Open("../../testdata/contracts/fixtures/edge/control.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	required := map[string]bool{"admit-current-generation": false, "admit-stale-generation": false, "admit-replayed-jti": false, "attach-route": false, "detach-stale-revision": false, "usage-first": false, "usage-increase": false, "usage-lower-duplicate": false, "usage-new-epoch": false}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var vector struct {
			Case          string `json:"case"`
			Valid         bool   `json:"valid"`
			Kind          string `json:"kind"`
			Error         string `json:"error"`
			Mutated       bool   `json:"mutated"`
			PreviousBytes uint64 `json:"previous_bytes"`
			Delta         uint64 `json:"delta"`
			Input         struct {
				Bytes uint64 `json:"bytes"`
			} `json:"input"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &vector); err != nil {
			t.Fatal(err)
		}
		if _, ok := required[vector.Case]; !ok {
			t.Fatalf("unknown edge case %q", vector.Case)
		}
		if !vector.Valid && (vector.Error == "" || vector.Mutated) {
			t.Fatalf("edge rejection is not typed/pre-mutation: %#v", vector)
		}
		if vector.Kind == "usage" {
			want := uint64(0)
			if vector.Input.Bytes > vector.PreviousBytes {
				want = vector.Input.Bytes - vector.PreviousBytes
			}
			if vector.Delta != want {
				t.Errorf("%s: usage delta=%d, want %d", vector.Case, vector.Delta, want)
			}
		}
		required[vector.Case] = true
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	for name, seen := range required {
		if !seen {
			t.Errorf("missing edge case %q", name)
		}
	}
}
