package contracttest

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

type stateTable struct {
	Initial string `json:"initial"`
	States  map[string]struct {
		Allow []string `json:"allow"`
	} `json:"states"`
	Attachment struct {
		States map[string]struct {
			Allow []string `json:"allow"`
		} `json:"states"`
	} `json:"attachment"`
}

func TestSessionStateContract(t *testing.T) {
	b, err := os.ReadFile("../../testdata/contracts/states/session.json")
	if err != nil {
		t.Fatal(err)
	}
	var table stateTable
	if err := json.Unmarshal(b, &table); err != nil {
		t.Fatal(err)
	}
	if table.Initial != "creating" {
		t.Fatalf("initial state = %q", table.Initial)
	}
	assertTransition(t, table.States, "running", "exited", true)
	assertTransition(t, table.States, "running", "deleted", false)
	assertTransition(t, table.States, "closed", "restarting", true)
	assertTransition(t, table.Attachment.States, "attached", "detached", true)
	if _, processState := table.States["detached"]; processState {
		t.Fatal("detach must not be a process state")
	}
}

func assertTransition(t *testing.T, states map[string]struct {
	Allow []string `json:"allow"`
}, from, to string, want bool) {
	t.Helper()
	state, ok := states[from]
	if !ok {
		t.Fatalf("missing state %q", from)
	}
	got := false
	for _, allowed := range state.Allow {
		if allowed == to {
			got = true
		}
	}
	if got != want {
		t.Fatalf("transition %s -> %s allowed=%v, want %v", from, to, got, want)
	}
}

func TestSessionOperationVectorCoverage(t *testing.T) {
	f, err := os.Open("../../testdata/contracts/fixtures/session/operations.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	required := map[string]bool{
		"create": false, "attach-replay": false, "attach-live-boundary": false, "input": false, "resize": false,
		"signal": false, "replay-gap": false, "stale-generation-input": false,
		"duplicate-input": false, "delete-running": false, "slow-consumer": false,
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var vector struct {
			Case  string `json:"case"`
			Valid bool   `json:"valid"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &vector); err != nil {
			t.Fatal(err)
		}
		if _, ok := required[vector.Case]; !ok {
			t.Fatalf("unknown operation vector %q", vector.Case)
		}
		if !vector.Valid && vector.Error == "" {
			t.Fatalf("negative vector %q has no typed error", vector.Case)
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
