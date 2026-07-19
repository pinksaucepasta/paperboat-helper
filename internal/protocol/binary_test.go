package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestBinaryFrameRoundTripFragmented(t *testing.T) {
	var wire bytes.Buffer
	want := BinaryFrame{Channel: Stdout, StartSequence: 42, Data: []byte("abc")}
	if err := WriteBinaryFrame(&wire, want); err != nil {
		t.Fatal(err)
	}
	got, err := ReadBinaryFrame(&fragmented{data: wire.Bytes(), chunk: 2})
	if err != nil {
		t.Fatal(err)
	}
	end, err := got.EndSequence()
	if err != nil || got.Channel != Stdout || got.StartSequence != 42 || end != 45 || string(got.Data) != "abc" {
		t.Fatalf("frame=%#v end=%d err=%v", got, end, err)
	}
}

func TestBinaryFrameRejectsHeaderChannelAndSequence(t *testing.T) {
	cases := []struct {
		name string
		wire []byte
		want Code
	}{
		{"short", binaryWire(8, make([]byte, 8)), InvalidFrame},
		{"channel", binaryWire(9, append([]byte{9}, make([]byte, 8)...)), UnsupportedChannel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ReadBinaryFrame(bytes.NewReader(tc.wire))
			var pe *Error
			if !errors.As(err, &pe) || pe.Code != tc.want {
				t.Fatalf("err=%v", err)
			}
		})
	}
	if err := WriteBinaryFrame(&bytes.Buffer{}, BinaryFrame{Channel: Stdout, StartSequence: math.MaxUint64, Data: []byte{1}}); err == nil {
		t.Fatal("expected sequence overflow")
	}
}

func TestBinaryFrameRejectsOversizeBeforeReadingBody(t *testing.T) {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], MaxBinaryFrame+1)
	_, err := ReadBinaryFrame(bytes.NewReader(prefix[:]))
	var pe *Error
	if !errors.As(err, &pe) || pe.Code != Oversized {
		t.Fatalf("err=%v", err)
	}
}

func binaryWire(n uint32, body []byte) []byte {
	wire := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(wire[:4], n)
	copy(wire[4:], body)
	return wire
}
