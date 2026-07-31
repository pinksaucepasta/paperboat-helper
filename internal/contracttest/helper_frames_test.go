package contracttest

import (
	"bufio"
	"encoding/json"
	"os"
	"testing"
)

type frameCase struct {
	Case  string `json:"case"`
	Valid bool   `json:"valid"`
	Error string `json:"error"`
	Frame struct {
		Type        string          `json:"type"`
		RequestID   string          `json:"request_id"`
		Version     string          `json:"version"`
		OperationID string          `json:"operation_id"`
		Capability  string          `json:"capability"`
		DeadlineMS  int             `json:"deadline_ms"`
		Payload     json.RawMessage `json:"payload"`
	} `json:"frame"`
}

func classifyFrame(c frameCase) string {
	switch c.Frame.Type {
	case "hello", "welcome", "response", "error", "heartbeat", "detach":
	case "request":
		if c.Frame.OperationID == "" {
			return "invalid_frame"
		}
		if c.Frame.Capability == "" {
			return "capability_required"
		}
		if c.Frame.DeadlineMS < 1 || c.Frame.DeadlineMS > 300000 {
			return "invalid_deadline"
		}
		if c.Frame.Payload == nil {
			return "invalid_frame"
		}
	case "cancel":
		if c.Frame.OperationID == "" {
			return "invalid_frame"
		}
	default:
		return "unsupported_message_type"
	}
	if c.Frame.RequestID == "" || c.Frame.Version != "1.0" {
		return "invalid_frame"
	}
	return ""
}

func TestHelperFrameContractVectors(t *testing.T) {
	f, err := os.Open("../../testdata/contracts/fixtures/helper/frames.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	seenValid, seenInvalid := false, false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var c frameCase
		if err := json.Unmarshal(scanner.Bytes(), &c); err != nil {
			t.Fatalf("decode vector: %v", err)
		}
		got := classifyFrame(c)
		if c.Valid {
			seenValid = true
			if got != "" {
				t.Errorf("%s: valid frame rejected as %s", c.Case, got)
			}
		} else {
			seenInvalid = true
			if got != c.Error {
				t.Errorf("%s: got %q, want %q", c.Case, got, c.Error)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if !seenValid || !seenInvalid {
		t.Fatal("contract requires positive and negative frame vectors")
	}
}
