package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestTerminalV2FramesRoundTrip(t *testing.T) {
	inputWire, err := EncodeTerminalInput(TerminalInputFrame{StreamID: 7, Sequence: 9, Data: []byte{0, 1, 0xff}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	input, err := DecodeTerminalInput(inputWire)
	if err != nil || input.StreamID != 7 || input.Sequence != 9 || !bytes.Equal(input.Data, []byte{0, 1, 0xff}) {
		t.Fatalf("input = %#v, %v", input, err)
	}

	outputWire, err := EncodeTerminalOutput(TerminalOutputFrame{Channel: TerminalStderr, StreamID: 8, StartSequence: 10, Data: []byte("err")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	output, err := DecodeTerminalOutput(outputWire)
	if err != nil || output.Channel != TerminalStderr || output.StreamID != 8 || output.StartSequence != 10 || string(output.Data) != "err" {
		t.Fatalf("output = %#v, %v", output, err)
	}

	ackWire, err := EncodeTerminalACK(TerminalACKFrame{StreamID: 9, NextSequence: 123}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ack, err := DecodeTerminalACK(ackWire)
	if err != nil || ack.StreamID != 9 || ack.NextSequence != 123 {
		t.Fatalf("ack = %#v, %v", ack, err)
	}

	resizeWire, err := EncodeTerminalResize(TerminalResizeFrame{StreamID: 10, Columns: 120, Rows: 40, Sequence: 2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	resize, err := DecodeTerminalResize(resizeWire)
	if err != nil || resize.StreamID != 10 || resize.Columns != 120 || resize.Rows != 40 || resize.Sequence != 2 {
		t.Fatalf("resize = %#v, %v", resize, err)
	}
}

func TestTerminalV2FramesRejectMalformedAndOversized(t *testing.T) {
	if _, err := DecodeTerminalInput(make([]byte, terminalInputHeaderLen)); err == nil {
		t.Fatal("truncated input accepted")
	}
	if _, err := EncodeTerminalInput(TerminalInputFrame{StreamID: 1, Sequence: 1, Data: make([]byte, MaxBinaryFrame)}, nil); err == nil {
		t.Fatal("oversized input accepted")
	}
	if _, err := TerminalOpcode([]byte{99}); err == nil {
		t.Fatal("unknown opcode accepted")
	} else {
		var protocolErr *Error
		if !errors.As(err, &protocolErr) || protocolErr.Code != UnsupportedChannel {
			t.Fatalf("unknown opcode error = %v", err)
		}
	}
}

func BenchmarkEncodeTerminalInputOneByte(b *testing.B) {
	frame := TerminalInputFrame{StreamID: 1, Sequence: 1, Data: []byte{'x'}}
	dst := make([]byte, 0, terminalInputHeaderLen+1)
	b.ReportAllocs()
	for range b.N {
		if _, err := EncodeTerminalInput(frame, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeTerminalInput32Bytes(b *testing.B) {
	frame := TerminalInputFrame{StreamID: 1, Sequence: 1, Data: make([]byte, 32)}
	dst := make([]byte, 0, terminalInputHeaderLen+32)
	b.ReportAllocs()
	for range b.N {
		if _, err := EncodeTerminalInput(frame, dst); err != nil {
			b.Fatal(err)
		}
	}
}
