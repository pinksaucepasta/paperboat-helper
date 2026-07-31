package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func validFrame() Frame {
	return Frame{Type: "request", RequestID: "req_1", Version: "1.0", OperationID: "op_00000001", Capability: "terminal.v1", DeadlineMS: 1000, Payload: []byte(`{"action":"list"}`)}
}

func TestFrameRoundTripWithFragmentedReader(t *testing.T) {
	var wire bytes.Buffer
	if err := WriteFrame(&wire, validFrame()); err != nil {
		t.Fatal(err)
	}
	f, err := ReadFrame(&fragmented{data: wire.Bytes(), chunk: 2})
	if err != nil || f.OperationID != "op_00000001" {
		t.Fatalf("frame=%#v err=%v", f, err)
	}
}

func TestReadFrameAcceptsCoalescedFrames(t *testing.T) {
	var wire bytes.Buffer
	first := Frame{Type: "heartbeat", RequestID: "req_1", Version: "1.0"}
	second := Frame{Type: "heartbeat", RequestID: "req_2", Version: "1.0"}
	if err := WriteFrame(&wire, first); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&wire, second); err != nil {
		t.Fatal(err)
	}
	for _, requestID := range []string{"req_1", "req_2"} {
		got, err := ReadFrame(&wire)
		if err != nil || got.RequestID != requestID {
			t.Fatalf("frame=%#v err=%v", got, err)
		}
	}
}

func TestFrameRejectsOversizeBeforeAllocation(t *testing.T) {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], MaxStructuredFrame+1)
	_, err := ReadFrame(bytes.NewReader(prefix[:]))
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != Oversized {
		t.Fatalf("err=%v", err)
	}
}

func TestFrameRejectsUnknownFieldsAndInvalidUTF8(t *testing.T) {
	for _, payload := range [][]byte{[]byte(`{"type":"heartbeat","request_id":"r","version":"1.0","wat":1}`), {0xff}} {
		var wire bytes.Buffer
		var p [4]byte
		binary.BigEndian.PutUint32(p[:], uint32(len(payload)))
		wire.Write(p[:])
		wire.Write(payload)
		_, err := ReadFrame(&wire)
		var pe *Error
		if !errors.As(err, &pe) || pe.Code != Malformed {
			t.Fatalf("payload=%x err=%v", payload, err)
		}
	}
}

type fragmented struct {
	data       []byte
	off, chunk int
}

func (r *fragmented) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := r.chunk
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data)-r.off {
		n = len(r.data) - r.off
	}
	copy(p, r.data[r.off:r.off+n])
	r.off += n
	return n, nil
}
