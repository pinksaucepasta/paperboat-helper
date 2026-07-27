package protocol

import (
	"encoding/binary"
	"errors"
	"math"
)

// Terminal v2 uses one WebSocket binary message per frame. WebSocket message
// boundaries provide framing, so terminal frames do not carry another length.
const (
	TerminalInputOpcode  byte = 1
	TerminalOutputOpcode byte = 2
	TerminalACKOpcode    byte = 3
	TerminalResizeOpcode byte = 4

	TerminalStdout byte = 1
	TerminalStderr byte = 2

	terminalInputHeaderLen  = 13
	terminalOutputHeaderLen = 14
	terminalACKLen          = 13
	terminalResizeLen       = 17
)

type TerminalInputFrame struct {
	StreamID uint32
	Sequence uint64
	Data     []byte
}

type TerminalOutputFrame struct {
	Channel       byte
	StreamID      uint32
	StartSequence uint64
	Data          []byte
}

type TerminalACKFrame struct {
	StreamID     uint32
	NextSequence uint64
}

type TerminalResizeFrame struct {
	StreamID uint32
	Columns  uint16
	Rows     uint16
	Sequence uint64
}

func EncodeTerminalInput(frame TerminalInputFrame, dst []byte) ([]byte, error) {
	if frame.StreamID == 0 || frame.Sequence == 0 || len(frame.Data) == 0 || len(frame.Data) > MaxBinaryFrame-terminalInputHeaderLen {
		return nil, &Error{Code: InvalidFrame}
	}
	dst = growFrame(dst, terminalInputHeaderLen+len(frame.Data))
	dst[0] = TerminalInputOpcode
	binary.BigEndian.PutUint32(dst[1:5], frame.StreamID)
	binary.BigEndian.PutUint64(dst[5:13], frame.Sequence)
	copy(dst[13:], frame.Data)
	return dst, nil
}

func DecodeTerminalInput(message []byte) (TerminalInputFrame, error) {
	if len(message) <= terminalInputHeaderLen || len(message) > MaxBinaryFrame || message[0] != TerminalInputOpcode {
		return TerminalInputFrame{}, &Error{Code: InvalidFrame}
	}
	frame := TerminalInputFrame{StreamID: binary.BigEndian.Uint32(message[1:5]), Sequence: binary.BigEndian.Uint64(message[5:13]), Data: message[13:]}
	if frame.StreamID == 0 || frame.Sequence == 0 {
		return TerminalInputFrame{}, &Error{Code: InvalidFrame}
	}
	return frame, nil
}

func EncodeTerminalOutput(frame TerminalOutputFrame, dst []byte) ([]byte, error) {
	if (frame.Channel != TerminalStdout && frame.Channel != TerminalStderr) || frame.StreamID == 0 || len(frame.Data) == 0 || len(frame.Data) > MaxBinaryFrame-terminalOutputHeaderLen || uint64(len(frame.Data)) > math.MaxUint64-frame.StartSequence {
		return nil, &Error{Code: InvalidFrame}
	}
	dst = growFrame(dst, terminalOutputHeaderLen+len(frame.Data))
	dst[0], dst[1] = TerminalOutputOpcode, frame.Channel
	binary.BigEndian.PutUint32(dst[2:6], frame.StreamID)
	binary.BigEndian.PutUint64(dst[6:14], frame.StartSequence)
	copy(dst[14:], frame.Data)
	return dst, nil
}

func DecodeTerminalOutput(message []byte) (TerminalOutputFrame, error) {
	if len(message) <= terminalOutputHeaderLen || len(message) > MaxBinaryFrame || message[0] != TerminalOutputOpcode || (message[1] != TerminalStdout && message[1] != TerminalStderr) {
		return TerminalOutputFrame{}, &Error{Code: InvalidFrame}
	}
	frame := TerminalOutputFrame{Channel: message[1], StreamID: binary.BigEndian.Uint32(message[2:6]), StartSequence: binary.BigEndian.Uint64(message[6:14]), Data: message[14:]}
	if frame.StreamID == 0 || uint64(len(frame.Data)) > math.MaxUint64-frame.StartSequence {
		return TerminalOutputFrame{}, &Error{Code: InvalidFrame}
	}
	return frame, nil
}

func EncodeTerminalACK(frame TerminalACKFrame, dst []byte) ([]byte, error) {
	if frame.StreamID == 0 {
		return nil, &Error{Code: InvalidFrame}
	}
	dst = growFrame(dst, terminalACKLen)
	dst[0] = TerminalACKOpcode
	binary.BigEndian.PutUint32(dst[1:5], frame.StreamID)
	binary.BigEndian.PutUint64(dst[5:13], frame.NextSequence)
	return dst, nil
}

func DecodeTerminalACK(message []byte) (TerminalACKFrame, error) {
	if len(message) != terminalACKLen || message[0] != TerminalACKOpcode {
		return TerminalACKFrame{}, &Error{Code: InvalidFrame}
	}
	frame := TerminalACKFrame{StreamID: binary.BigEndian.Uint32(message[1:5]), NextSequence: binary.BigEndian.Uint64(message[5:13])}
	if frame.StreamID == 0 {
		return TerminalACKFrame{}, &Error{Code: InvalidFrame}
	}
	return frame, nil
}

func EncodeTerminalResize(frame TerminalResizeFrame, dst []byte) ([]byte, error) {
	if frame.StreamID == 0 || frame.Columns == 0 || frame.Rows == 0 || frame.Sequence == 0 {
		return nil, &Error{Code: InvalidFrame}
	}
	dst = growFrame(dst, terminalResizeLen)
	dst[0] = TerminalResizeOpcode
	binary.BigEndian.PutUint32(dst[1:5], frame.StreamID)
	binary.BigEndian.PutUint16(dst[5:7], frame.Columns)
	binary.BigEndian.PutUint16(dst[7:9], frame.Rows)
	binary.BigEndian.PutUint64(dst[9:17], frame.Sequence)
	return dst, nil
}

func DecodeTerminalResize(message []byte) (TerminalResizeFrame, error) {
	if len(message) != terminalResizeLen || message[0] != TerminalResizeOpcode {
		return TerminalResizeFrame{}, &Error{Code: InvalidFrame}
	}
	frame := TerminalResizeFrame{StreamID: binary.BigEndian.Uint32(message[1:5]), Columns: binary.BigEndian.Uint16(message[5:7]), Rows: binary.BigEndian.Uint16(message[7:9]), Sequence: binary.BigEndian.Uint64(message[9:17])}
	if frame.StreamID == 0 || frame.Columns == 0 || frame.Rows == 0 || frame.Sequence == 0 {
		return TerminalResizeFrame{}, &Error{Code: InvalidFrame}
	}
	return frame, nil
}

func TerminalOpcode(message []byte) (byte, error) {
	if len(message) == 0 {
		return 0, &Error{Code: InvalidFrame, Cause: errors.New("terminal frame is empty")}
	}
	if message[0] < TerminalInputOpcode || message[0] > TerminalResizeOpcode {
		return 0, &Error{Code: UnsupportedChannel}
	}
	return message[0], nil
}

func growFrame(dst []byte, size int) []byte {
	if cap(dst) < size {
		return make([]byte, size)
	}
	return dst[:size]
}
