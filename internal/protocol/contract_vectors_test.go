package protocol

import (
	"bufio"
	"bytes"
	"crypto/sha256"
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
	data, err := os.ReadFile("../../testdata/contracts/fixtures/helper/terminal-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Case      string `json:"case"`
			Valid     bool   `json:"valid"`
			Wire      string `json:"wire_base64"`
			Generated struct {
				PayloadByte        byte `json:"payload_byte"`
				UncompressedLength int  `json:"uncompressed_length"`
			} `json:"generated"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, vector := range fixture.Cases {
		if vector.Case == "output-raw-max" {
			data := bytes.Repeat([]byte{vector.Generated.PayloadByte}, vector.Generated.UncompressedLength)
			wire, err := EncodeTerminalOutput(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: data}, nil)
			if err != nil || len(wire) != MaxBinaryFrame {
				t.Fatalf("%s: wire=%d err=%v", vector.Case, len(wire), err)
			}
			frame, err := DecodeTerminalOutput(wire)
			if err != nil || len(frame.Data) != MaxTerminalOutputBytes {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
			continue
		}
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
		case "output-zstd-agent-log":
			frame, err := DecodeTerminalOutput(wire)
			if err != nil || frame.Encoding != TerminalOutputZstd || frame.Channel != TerminalStdout || frame.StreamID != 9 || frame.StartSequence != 13 || len(frame.Data) != 2000 || sha256.Sum256(frame.Data) != [32]byte{0xfb, 0x69, 0x7f, 0x2b, 0x20, 0x7f, 0xc7, 0x3b, 0xdc, 0x89, 0x18, 0x57, 0x23, 0x19, 0x0d, 0x2b, 0xbf, 0x5b, 0x1c, 0xd1, 0x42, 0x3a, 0xaf, 0xc4, 0xd6, 0x55, 0x4d, 0x90, 0xbe, 0x14, 0x30, 0x10} {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "output-unknown-encoding", "output-zstd-length-mismatch", "output-zstd-trailing-data", "output-zstd-declared-too-large", "output-sequence-overflow", "output-zero-declared-length":
			if _, err := DecodeTerminalOutput(wire); err == nil {
				t.Fatalf("%s accepted", vector.Case)
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
		`{"type":"heartbeat","type":"heartbeat","request_id":"req","version":"1.0"}`,
		`{"type":"hello","request_id":"req","version":"1.0","payload":{"min_version":"1.0","min_version":"1.0"}}`,
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
