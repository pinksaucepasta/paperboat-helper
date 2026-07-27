package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	MaxBinaryFrame  = 256 << 10
	binaryHeaderLen = 9
	Stdout          = byte(1)
	Stderr          = byte(2)
	TerminalInput   = byte(3)
)

const UnsupportedChannel Code = "unsupported_channel"

type BinaryFrame struct {
	Channel       byte
	StartSequence uint64
	Data          []byte
	Release       func()
}

func (f BinaryFrame) EndSequence() (uint64, error) {
	if uint64(len(f.Data)) > math.MaxUint64-f.StartSequence {
		return 0, &Error{Code: InvalidFrame, Cause: errors.New("sequence overflow")}
	}
	return f.StartSequence + uint64(len(f.Data)), nil
}

func ReadBinaryFrame(r io.Reader) (BinaryFrame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return BinaryFrame{}, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n < binaryHeaderLen {
		return BinaryFrame{}, &Error{Code: InvalidFrame, Cause: errors.New("binary header is truncated")}
	}
	if n > MaxBinaryFrame {
		return BinaryFrame{}, &Error{Code: Oversized}
	}
	var header [binaryHeaderLen]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return BinaryFrame{}, &Error{Code: InvalidFrame, Cause: err}
	}
	if header[0] != Stdout && header[0] != Stderr && header[0] != TerminalInput {
		return BinaryFrame{}, &Error{Code: UnsupportedChannel}
	}
	f := BinaryFrame{Channel: header[0], StartSequence: binary.BigEndian.Uint64(header[1:])}
	f.Data = make([]byte, int(n)-binaryHeaderLen)
	if _, err := io.ReadFull(r, f.Data); err != nil {
		return BinaryFrame{}, &Error{Code: InvalidFrame, Cause: err}
	}
	if _, err := f.EndSequence(); err != nil {
		return BinaryFrame{}, err
	}
	return f, nil
}

func WriteBinaryFrame(w io.Writer, f BinaryFrame) error {
	if f.Channel != Stdout && f.Channel != Stderr && f.Channel != TerminalInput {
		return &Error{Code: UnsupportedChannel}
	}
	if len(f.Data) > MaxBinaryFrame-binaryHeaderLen {
		return &Error{Code: Oversized}
	}
	if _, err := f.EndSequence(); err != nil {
		return err
	}
	var wireHeader [4 + binaryHeaderLen]byte
	binary.BigEndian.PutUint32(wireHeader[:4], uint32(binaryHeaderLen+len(f.Data)))
	wireHeader[4] = f.Channel
	binary.BigEndian.PutUint64(wireHeader[5:], f.StartSequence)
	if err := writeAll(w, wireHeader[:]); err != nil {
		return fmt.Errorf("write binary header: %w", err)
	}
	if err := writeAll(w, f.Data); err != nil {
		return fmt.Errorf("write binary body: %w", err)
	}
	return nil
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
