package contracttest

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

type transportCase struct {
	Case       string   `json:"case"`
	Valid      bool     `json:"valid"`
	WireChunks []string `json:"wire_chunks_base64"`
	Error      string   `json:"error"`
	CloseCode  int      `json:"close_code"`
	Expected   any      `json:"expected"`
	Count      int      `json:"expected_count"`
}

func TestTransportContractVectors(t *testing.T) {
	b, err := os.ReadFile("../../testdata/contracts/fixtures/helper/transport.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []transportCase `json:"cases"`
	}
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	required := map[string]bool{"structured-fragmented": false, "structured-coalesced": false, "binary-fragmented": false, "malformed-short-binary-length": false, "invalid-structured-utf8": false, "oversized-structured-frame": false, "unknown-binary-channel": false, "cancel-before-result": false, "half-close-input": false, "slow-consumer-limit": false}
	for _, vector := range fixture.Cases {
		if _, ok := required[vector.Case]; !ok {
			t.Fatalf("unknown transport case %q", vector.Case)
		}
		if !vector.Valid && (vector.Error == "" || vector.CloseCode == 0) {
			t.Errorf("%s: rejected wire case needs typed error and close code", vector.Case)
		}
		switch vector.Case {
		case "structured-fragmented":
			frames, err := decodeStructured(vector.WireChunks)
			if err != nil || len(frames) != 1 || frames[0]["type"] != "heartbeat" {
				t.Fatalf("fragmented frame: frames=%#v err=%v", frames, err)
			}
		case "structured-coalesced":
			frames, err := decodeStructured(vector.WireChunks)
			if err != nil || len(frames) != vector.Count {
				t.Fatalf("coalesced frames: count=%d err=%v", len(frames), err)
			}
		case "binary-fragmented":
			wire := decodeChunks(t, vector.WireChunks)
			if len(wire) != 16 || binary.BigEndian.Uint32(wire[:4]) != 12 || wire[4] != 1 || binary.BigEndian.Uint64(wire[5:13]) != 42 || string(wire[13:]) != "abc" {
				t.Fatalf("unexpected binary frame: %x", wire)
			}
		}
		required[vector.Case] = true
	}
	for name, seen := range required {
		if !seen {
			t.Errorf("missing transport case %q", name)
		}
	}
}

func decodeChunks(t *testing.T, chunks []string) []byte {
	t.Helper()
	var wire []byte
	for _, chunk := range chunks {
		decoded, err := base64.StdEncoding.DecodeString(chunk)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, decoded...)
	}
	return wire
}

func decodeStructured(chunks []string) ([]map[string]any, error) {
	var wire []byte
	for _, chunk := range chunks {
		decoded, err := base64.StdEncoding.DecodeString(chunk)
		if err != nil {
			return nil, err
		}
		wire = append(wire, decoded...)
	}
	var frames []map[string]any
	for len(wire) > 0 {
		if len(wire) < 4 {
			return nil, fmt.Errorf("truncated length")
		}
		length := int(binary.BigEndian.Uint32(wire[:4]))
		wire = wire[4:]
		if length > 65536 || len(wire) < length {
			return nil, fmt.Errorf("invalid length")
		}
		body := wire[:length]
		wire = wire[length:]
		if !json.Valid(body) || bytes.ContainsRune(body, '\ufffd') {
			return nil, fmt.Errorf("invalid JSON")
		}
		var frame map[string]any
		if err := json.Unmarshal(body, &frame); err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	return frames, nil
}
