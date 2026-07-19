package protocol

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
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
	data, err := os.ReadFile("../../testdata/contracts/fixtures/helper/transport.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Case           string   `json:"case"`
			WireChunks     []string `json:"wire_chunks_base64"`
			Count          int      `json:"expected_count"`
			DeclaredLength uint32   `json:"declared_length"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, vector := range fixture.Cases {
		wire := decodeWire(t, vector.WireChunks)
		switch vector.Case {
		case "structured-fragmented":
			frame, err := ReadFrame(&fragmented{data: wire, chunk: 2})
			if err != nil || frame.Type != "heartbeat" {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "structured-coalesced":
			reader := bytes.NewReader(wire)
			for index := 0; index < vector.Count; index++ {
				if _, err := ReadFrame(reader); err != nil {
					t.Fatalf("%s frame %d: %v", vector.Case, index, err)
				}
			}
		case "binary-fragmented":
			frame, err := ReadBinaryFrame(&fragmented{data: wire, chunk: 2})
			if err != nil || frame.Channel != Stdout || frame.StartSequence != 42 || string(frame.Data) != "abc" {
				t.Fatalf("%s: frame=%#v err=%v", vector.Case, frame, err)
			}
		case "malformed-short-binary-length":
			assertProtocolCode(t, readBinaryFrameError(wire), InvalidFrame)
		case "invalid-structured-utf8":
			_, err := ReadFrame(bytes.NewReader(wire))
			assertProtocolCode(t, err, Malformed)
		case "oversized-structured-frame":
			if len(wire) == 0 {
				wire = make([]byte, 4)
				binary.BigEndian.PutUint32(wire, vector.DeclaredLength)
			}
			_, err := ReadFrame(bytes.NewReader(wire))
			assertProtocolCode(t, err, Oversized)
		case "unknown-binary-channel":
			assertProtocolCode(t, readBinaryFrameError(wire), UnsupportedChannel)
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
