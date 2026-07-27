package protocol

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func TestProductionFrameValidationMatchesCopiedVectors(t *testing.T) {
	file, err := os.Open("../../testdata/contracts/fixtures/helper/frames.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	requestedCodes := map[string]Code{"invalid_frame": InvalidFrame, "invalid_deadline": InvalidDeadline, "unsupported_message_type": UnsupportedMessage, "capability_required": CapabilityRequired}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var vector struct {
			Case  string `json:"case"`
			Valid bool   `json:"valid"`
			Error string `json:"error"`
			Frame Frame  `json:"frame"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &vector); err != nil {
			t.Fatal(err)
		}
		err := vector.Frame.Validate()
		if vector.Valid {
			if err != nil {
				t.Errorf("%s: %v", vector.Case, err)
			}
			continue
		}
		var protocolError *Error
		if !errors.As(err, &protocolError) || protocolError.Code != requestedCodes[vector.Error] {
			t.Errorf("%s: err=%v want=%s", vector.Case, err, requestedCodes[vector.Error])
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestProductionDecodersMatchCopiedTransportVectors(t *testing.T) {
	data, err := os.ReadFile("../../testdata/contracts/fixtures/helper/terminal-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Case  string `json:"case"`
			Valid bool   `json:"valid"`
			Wire  string `json:"wire_base64"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, vector := range fixture.Cases {
		wire, err := base64.StdEncoding.DecodeString(vector.Wire)
		if vector.Wire == "" {
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", vector.Case, err)
		}
		switch vector.Case {
		case "input":
			frame, err := DecodeTerminalInput(wire)
			if err != nil || frame.StreamID != 7 || frame.Sequence != 9 || !bytes.Equal(frame.Data, []byte{0, 1, 0xff}) {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "output-stderr":
			frame, err := DecodeTerminalOutput(wire)
			if err != nil || frame.Channel != TerminalStderr || frame.StreamID != 8 || frame.StartSequence != 10 || string(frame.Data) != "err" {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "ack":
			frame, err := DecodeTerminalACK(wire)
			if err != nil || frame.StreamID != 9 || frame.NextSequence != 123 {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "resize":
			frame, err := DecodeTerminalResize(wire)
			if err != nil || frame.StreamID != 10 || frame.Columns != 120 || frame.Rows != 40 || frame.Sequence != 2 {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "zero-stream-id":
			if _, err := DecodeTerminalInput(wire); err == nil {
				t.Fatalf("%s accepted", vector.Case)
			}
		case "unknown-opcode":
			assertProtocolCode(t, func() error { _, err := TerminalOpcode(wire); return err }(), UnsupportedChannel)
		}
	}
}

func TestFrameRejectsDuplicateTopLevelAndNestedKeys(t *testing.T) {
	for _, body := range []string{
		`{"type":"heartbeat","type":"heartbeat","request_id":"req","version":"2.0"}`,
		`{"type":"hello","request_id":"req","version":"2.0","payload":{"min_version":"2.0","min_version":"2.0"}}`,
	} {
		wire := makeStructuredWire([]byte(body))
		_, err := ReadFrame(bytes.NewReader(wire))
		assertProtocolCode(t, err, Malformed)
	}
}

func TestCloseCodeUsesFrozenTransportCategories(t *testing.T) {
	if got := CloseCode(&Error{Code: ProtocolIncompatible}); got != CloseIncompatible {
		t.Fatalf("incompatible close=%d", got)
	}
	if got := CloseCode(&Error{Code: Oversized}); got != CloseMalformed {
		t.Fatalf("oversized close=%d", got)
	}
	if got := CloseCode(&Error{Code: CredentialExpired}); got != CloseUnauthorized {
		t.Fatalf("expired credential close=%d", got)
	}
	if got := CloseCode(errors.New("unknown")); got != CloseUnavailable {
		t.Fatalf("unknown close=%d", got)
	}
}

func decodeWire(t *testing.T, chunks []string) []byte {
	t.Helper()
	var wire []byte
	for _, encoded := range chunks {
		chunk, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, chunk...)
	}
	return wire
}

func readBinaryFrameError(wire []byte) error {
	_, err := ReadBinaryFrame(bytes.NewReader(wire))
	return err
}

func assertProtocolCode(t *testing.T, err error, code Code) {
	t.Helper()
	var protocolError *Error
	if !errors.As(err, &protocolError) || protocolError.Code != code {
		t.Fatalf("err=%v want=%s", err, code)
	}
}

func makeStructuredWire(body []byte) []byte {
	var wire bytes.Buffer
	wire.Write([]byte{byte(len(body) >> 24), byte(len(body) >> 16), byte(len(body) >> 8), byte(len(body))})
	wire.Write(body)
	return wire.Bytes()
}
