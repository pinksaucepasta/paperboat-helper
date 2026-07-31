package protocol

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

type terminalCompressionWorkload struct {
	name     string
	category string
	data     []byte
}

func BenchmarkTerminalOutputCodecSizes(b *testing.B) {
	for _, size := range []int{128, 512, 1024, 4 << 10, 32 << 10} {
		data := repeatToSize([]byte("agent output with ansi \x1b[32mready\x1b[0m\r\n"), size)
		b.Run(strconv.Itoa(size)+"/encode", func(b *testing.B) {
			frame := TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: data}
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for range b.N {
				if _, err := EncodeTerminalOutputAdaptive(frame, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
		wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: data}, nil)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(strconv.Itoa(size)+"/decode", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for range b.N {
				if _, err := DecodeTerminalOutput(wire); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTerminalOutputParallelAttachments(b *testing.B) {
	data := repeatToSize([]byte("parallel terminal output line\r\n"), 32<<10)
	for _, attachments := range []int{1, 4, 16} {
		b.Run(strconv.Itoa(attachments), func(b *testing.B) {
			b.SetBytes(int64(len(data) * attachments))
			b.ReportAllocs()
			for range b.N {
				var group sync.WaitGroup
				group.Add(attachments)
				for streamID := 1; streamID <= attachments; streamID++ {
					go func(streamID int) {
						defer group.Done()
						if _, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: uint32(streamID), Data: data}, nil); err != nil {
							b.Error(err)
						}
					}(streamID)
				}
				group.Wait()
			}
		})
	}
}

func terminalCompressionWorkloads() []terminalCompressionWorkload {
	random := make([]byte, 32<<10)
	_, _ = rand.New(rand.NewSource(0x50425054)).Read(random)
	return []terminalCompressionWorkload{
		{name: "agentic-4k", category: "compress", data: generatedAgenticOutput(4 << 10)},
		{name: "agentic-32k", category: "compress", data: generatedAgenticOutput(32 << 10)},
		{name: "source-log-32k", category: "compress", data: generatedSourceLog(32 << 10)},
		{name: "ansi-redraw-32k", category: "compress", data: generatedANSIRedraw(32 << 10)},
		{name: "short-response-128", category: "small", data: repeatToSize([]byte("ready\r\n"), 128)},
		{name: "incompressible-32k", category: "insufficient", data: random},
	}
}

func generatedAgenticOutput(size int) []byte {
	var output bytes.Buffer
	verbs := []string{"Inspecting", "Updating", "Verifying", "Replaying", "Comparing", "Formatting"}
	files := []string{"internal/session/manager.go", "internal/protocol/frame.go", "internal/tunnel/quic.go", "docs/operations.md"}
	for line := 0; output.Len() < size; line++ {
		fmt.Fprintf(&output, "%s %s before applying bounded change %04d.\r\n", verbs[line%len(verbs)], files[(line*7)%len(files)], line)
		fmt.Fprintf(&output, "%c func step%04d(ctx context.Context, sequence uint64) error { return resume(ctx, sequence+%d) }\r\n", []byte{' ', '+', '-'}[line%3], line, line%97)
	}
	return append([]byte(nil), output.Bytes()[:size]...)
}

func generatedSourceLog(size int) []byte {
	var output bytes.Buffer
	levels := []string{"DEBUG", "INFO", "WARN"}
	packages := []string{"terminal", "session", "history", "connector"}
	for line := 0; output.Len() < size; line++ {
		fmt.Fprintf(&output, "2026-07-31T12:%02d:%02d.%03dZ %s package=%s operation=step_%03d result=ok duration_ms=%d sequence=%d\n", line%60, (line*13)%60, (line*37)%1000, levels[line%len(levels)], packages[(line*5)%len(packages)], line%211, 3+(line*17)%89, line*4096)
	}
	return append([]byte(nil), output.Bytes()[:size]...)
}

func generatedANSIRedraw(size int) []byte {
	var output bytes.Buffer
	states := []string{"Analyzing", "Editing", "Testing", "Waiting"}
	for frame := 0; output.Len() < size; frame++ {
		progress := (frame * 7) % 101
		fmt.Fprintf(&output, "\x1b[2K\r\x1b[36m%s\x1b[0m [%03d%%] task=%04d\x1b[1A\x1b[2K\r\x1b[32mTests\x1b[0m %d passed %d pending\x1b[1B", states[frame%len(states)], progress, frame, frame*3, 200-frame%200)
	}
	return append([]byte(nil), output.Bytes()[:size]...)
}

func repeatToSize(pattern []byte, size int) []byte {
	result := make([]byte, size)
	for offset := 0; offset < len(result); offset += copy(result[offset:], pattern) {
	}
	return result
}

func TestTerminalCompressionWorkloadMatrix(t *testing.T) {
	for _, workload := range terminalCompressionWorkloads() {
		t.Run(workload.name, func(t *testing.T) {
			wire, err := EncodeTerminalOutputAdaptive(TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: workload.data}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := int(binary.BigEndian.Uint32(wire[15:19])); got != len(workload.data) {
				t.Fatalf("declared=%d want=%d", got, len(workload.data))
			}
			decoded, err := DecodeTerminalOutput(wire)
			if err != nil || !bytes.Equal(decoded.Data, workload.data) {
				t.Fatalf("round trip err=%v", err)
			}
			payloadBytes := len(wire) - terminalOutputHeaderLen
			t.Logf("category=%s raw_bytes=%d encoded_bytes=%d encoding=%d", workload.category, len(workload.data), payloadBytes, wire[2])
			switch workload.category {
			case "small", "insufficient":
				if wire[2] != TerminalOutputRaw || payloadBytes != len(workload.data) {
					t.Fatalf("encoding=%d payload=%d raw=%d", wire[2], payloadBytes, len(workload.data))
				}
			case "compress":
				if wire[2] != TerminalOutputZstd {
					t.Fatalf("encoding=%d", wire[2])
				}
				reduction := 100 * (len(workload.data) - payloadBytes) / len(workload.data)
				minimum := 50
				if len(workload.data) == 4<<10 {
					minimum = 35
				}
				if reduction < minimum {
					t.Fatalf("reduction=%d%% want>=%d%%", reduction, minimum)
				}
			default:
				t.Fatalf("unknown category %q", workload.category)
			}
		})
	}
}

func BenchmarkTerminalCompressionWorkloadMatrix(b *testing.B) {
	for _, workload := range terminalCompressionWorkloads() {
		b.Run(workload.name, func(b *testing.B) {
			frame := TerminalOutputFrame{Channel: TerminalStdout, StreamID: 1, Data: workload.data}
			b.SetBytes(int64(len(workload.data)))
			b.ReportAllocs()
			for range b.N {
				if _, err := EncodeTerminalOutputAdaptive(frame, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
