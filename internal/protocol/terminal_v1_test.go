package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestTerminalV1FramesRoundTrip(t *testing.T) {
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

func TestTerminalOutputAdaptiveCompression(t *testing.T) {
	data := bytes.Repeat([]byte("agent output with ansi \x1b[32mready\x1b[0m\r\n"), 200)
	wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 3, StartSequence: 41, Data: data}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := binary.BigEndian.Uint32(wire[15:19])
	if wire[2] != TerminalOutputZstd || got != uint32(len(data)) {
		t.Fatalf("encoding=%d declared=%d", wire[2], got)
	}
	frame, err := DecodeTerminalOutput(wire)
	if err != nil || frame.StartSequence != 41 || !bytes.Equal(frame.Data, data) {
		t.Fatalf("frame=%#v err=%v", frame, err)
	}
}

func TestTerminalOutputAdaptiveRawFallback(t *testing.T) {
	wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: []byte("prompt")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if wire[2] != TerminalOutputRaw || len(wire) != terminalOutputHeaderLen+6 {
		t.Fatalf("wire=%x", wire)
	}
}

func TestTerminalOutputRejectsDeclaredLengthAndTrailingData(t *testing.T) {
	wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: bytes.Repeat([]byte("x"), 4096)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	badLength := append([]byte(nil), wire...)
	binary.BigEndian.PutUint32(badLength[15:19], 4095)
	if _, err := DecodeTerminalOutput(badLength); err == nil {
		t.Fatal("declared-length mismatch accepted")
	}
	if _, err := DecodeTerminalOutput(append(wire, 0)); err == nil {
		t.Fatal("trailing data accepted")
	}
	emptyEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(false))
	if err != nil {
		t.Fatal(err)
	}
	emptyFrame := emptyEncoder.EncodeAll([]byte("x"), nil)
	emptyEncoder.Close()
	if _, err := DecodeTerminalOutput(append(wire, emptyFrame...)); err == nil {
		t.Fatal("concatenated empty frame accepted")
	}
}

func TestTerminalOutputChecksumFrameDecodesExactly(t *testing.T) {
	data := bytes.Repeat([]byte("checksum"), 300)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderCRC(true))
	if err != nil {
		t.Fatal(err)
	}
	payload := encoder.EncodeAll(data, nil)
	encoder.Close()
	wire, err := EncodeTerminalOutput(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Encoding: TerminalOutputZstd, UncompressedLength: uint32(len(data)), Data: payload}, nil)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := DecodeTerminalOutput(wire)
	if err != nil || !bytes.Equal(frame.Data, data) {
		t.Fatalf("frame=%#v err=%v", frame, err)
	}
}

func TestTerminalOutputRejectsDictionaryFrame(t *testing.T) {
	dictionary := []byte("paperboat terminal dictionary content that must never be negotiated")
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderDictRaw(7, dictionary))
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat(dictionary, 32)
	payload := encoder.EncodeAll(data, nil)
	encoder.Close()
	wire, err := EncodeTerminalOutput(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Encoding: TerminalOutputZstd, UncompressedLength: uint32(len(data)), Data: payload}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeTerminalOutput(wire); err == nil {
		t.Fatal("dictionary frame accepted")
	}
}

func TestTerminalOutputRejectsOversizedWindowBeforeDecode(t *testing.T) {
	data := bytes.Repeat([]byte("bounded window"), 200)
	encoder, err := zstd.NewWriter(nil, zstd.WithWindowSize(1<<20), zstd.WithSingleSegment(false), zstd.WithEncoderCRC(false))
	if err != nil {
		t.Fatal(err)
	}
	payload := encoder.EncodeAll(data, nil)
	encoder.Close()
	if len(payload) < 6 {
		t.Fatalf("payload too short: %d", len(payload))
	}
	payload[5] = 0x50 // Non-single-segment window descriptor for 1 MiB.
	wire, err := EncodeTerminalOutput(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Encoding: TerminalOutputZstd, UncompressedLength: uint32(len(data)), Data: payload}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := InspectTerminalOutput(wire); err == nil {
		t.Fatal("oversized zstd window accepted")
	}
}

func FuzzDecodeTerminalOutput(f *testing.F) {
	raw, _ := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: []byte("prompt")}, nil)
	compressed, _ := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStderr, StreamID: 2, StartSequence: 9, Data: bytes.Repeat([]byte("agent log\r\n"), 400)}, nil)
	for _, seed := range [][]byte{nil, {TerminalOutputOpcode}, raw, compressed, append(append([]byte(nil), compressed...), 0)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		frame, err := DecodeTerminalOutput(data)
		if err == nil {
			if len(frame.Data) == 0 || len(frame.Data) > MaxBinaryFrame-terminalOutputHeaderLen || frame.StreamID == 0 || uint64(len(frame.Data)) > math.MaxUint64-frame.StartSequence {
				t.Fatalf("invalid successful decode: %#v", frame)
			}
		}
	})
}

func TestTerminalOutputRoundTripsDeterministicSizes(t *testing.T) {
	random := rand.New(rand.NewSource(0x50425054))
	sequence := uint64(0)
	for _, size := range []int{1, 127, 128, 511, 1023, 1024, 4096, 32 << 10, MaxTerminalOutputBytes} {
		data := make([]byte, size)
		if _, err := random.Read(data); err != nil {
			t.Fatal(err)
		}
		channel := TerminalStdout
		if size%2 == 0 {
			channel = TerminalStderr
		}
		wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: channel, StreamID: 3, StartSequence: sequence, Data: data}, nil)
		if err != nil {
			t.Fatalf("size=%d encode: %v", size, err)
		}
		frame, err := DecodeTerminalOutput(wire)
		if err != nil || frame.Channel != channel || frame.StartSequence != sequence || !bytes.Equal(frame.Data, data) {
			t.Fatalf("size=%d frame=%#v err=%v", size, frame, err)
		}
		sequence += uint64(size)
	}
}

func TestTerminalCodecConcurrentUseAndRepeatedFailures(t *testing.T) {
	data := bytes.Repeat([]byte("concurrent agent output\r\n"), 1300)
	wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: data}, nil)
	if err != nil || wire[2] != TerminalOutputZstd {
		t.Fatalf("encoding=%d err=%v", wire[2], err)
	}
	var group sync.WaitGroup
	errorsSeen := make(chan error, 64)
	for worker := 0; worker < 64; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 50; iteration++ {
				frame, decodeErr := DecodeTerminalOutput(wire)
				if decodeErr != nil || !bytes.Equal(frame.Data, data) {
					errorsSeen <- decodeErr
					return
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() { group.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("bounded codec pool deadlocked")
	}
	close(errorsSeen)
	for decodeErr := range errorsSeen {
		t.Fatalf("concurrent decode: %v", decodeErr)
	}
	bad := append([]byte(nil), wire...)
	bad[len(bad)-1] ^= 0xff
	for iteration := 0; iteration < 1000; iteration++ {
		if _, err := DecodeTerminalOutput(bad); err == nil {
			t.Fatal("corrupt frame accepted")
		}
	}
	if len(terminalCodecSlots) != 0 {
		t.Fatalf("codec slots leaked: %d", len(terminalCodecSlots))
	}
}

func BenchmarkTerminalOutputAdaptive32KiB(b *testing.B) {
	frame := TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: bytes.Repeat([]byte("structured agent log line with ansi \x1b[0m\n"), 800)}
	b.ReportAllocs()
	for range b.N {
		if _, err := EncodeTerminalOutputAdaptive(frame, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func TestTerminalV1FramesRejectMalformedAndOversized(t *testing.T) {
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
