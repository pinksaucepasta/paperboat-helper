package protocol

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

var terminalEncoderPool sync.Pool
var terminalDecoderPool sync.Pool
var terminalCodecSlots = make(chan struct{}, 16)

type compressionCounters struct {
	smallFrames, insufficientFrames, compressedFrames atomic.Uint64
	rawBytes, encodedBytes                            atomic.Uint64
	encodeFailures, decodeFailures                    atomic.Uint64
	encodeNanos, decodeNanos                          atomic.Uint64
}

type CompressionMetrics struct {
	SmallFrames        uint64
	InsufficientFrames uint64
	CompressedFrames   uint64
	RawBytes           uint64
	EncodedBytes       uint64
	EncodeFailures     uint64
	DecodeFailures     uint64
	EncodeNanos        uint64
	DecodeNanos        uint64
}

var terminalCompressionCounters compressionCounters

func TerminalCompressionMetrics() CompressionMetrics {
	return CompressionMetrics{
		SmallFrames:        terminalCompressionCounters.smallFrames.Load(),
		InsufficientFrames: terminalCompressionCounters.insufficientFrames.Load(),
		CompressedFrames:   terminalCompressionCounters.compressedFrames.Load(),
		RawBytes:           terminalCompressionCounters.rawBytes.Load(),
		EncodedBytes:       terminalCompressionCounters.encodedBytes.Load(),
		EncodeFailures:     terminalCompressionCounters.encodeFailures.Load(),
		DecodeFailures:     terminalCompressionCounters.decodeFailures.Load(),
		EncodeNanos:        terminalCompressionCounters.encodeNanos.Load(),
		DecodeNanos:        terminalCompressionCounters.decodeNanos.Load(),
	}
}

// Terminal v1 uses one WebSocket binary message per frame. WebSocket message
// boundaries provide framing, so terminal frames do not carry another length.
const (
	TerminalInputOpcode  byte = 1
	TerminalOutputOpcode byte = 2
	TerminalACKOpcode    byte = 3
	TerminalResizeOpcode byte = 4

	TerminalStdout byte = 1
	TerminalStderr byte = 2

	terminalInputHeaderLen               = 13
	terminalOutputHeaderLen              = 19
	TerminalOutputHeaderBytes            = terminalOutputHeaderLen
	TerminalOutputRaw               byte = 0
	TerminalOutputZstd              byte = 1
	TerminalOutputMinCompress            = 1024
	TerminalOutputMinSavingsBytes        = 32
	TerminalOutputMinSavingsPercent      = 5
	MaxTerminalOutputBytes               = MaxBinaryFrame - terminalOutputHeaderLen
	terminalACKLen                       = 13
	terminalResizeLen                    = 17
)

type TerminalInputFrame struct {
	StreamID uint32
	Sequence uint64
	Data     []byte
}

type TerminalOutputFrame struct {
	Channel            byte
	StreamID           uint32
	StartSequence      uint64
	Encoding           byte
	UncompressedLength uint32
	Data               []byte
}

type TerminalOutputInfo struct {
	Encoding           byte
	StreamID           uint32
	StartSequence      uint64
	UncompressedLength uint32
}

func InspectTerminalOutput(message []byte) (TerminalOutputInfo, error) {
	if len(message) <= terminalOutputHeaderLen || len(message) > MaxBinaryFrame || message[0] != TerminalOutputOpcode || (message[1] != TerminalStdout && message[1] != TerminalStderr) || (message[2] != TerminalOutputRaw && message[2] != TerminalOutputZstd) {
		return TerminalOutputInfo{}, &Error{Code: InvalidFrame}
	}
	info := TerminalOutputInfo{Encoding: message[2], StreamID: binary.BigEndian.Uint32(message[3:7]), StartSequence: binary.BigEndian.Uint64(message[7:15]), UncompressedLength: binary.BigEndian.Uint32(message[15:19])}
	if info.StreamID == 0 || info.UncompressedLength == 0 || info.UncompressedLength > MaxBinaryFrame-terminalOutputHeaderLen || uint64(info.UncompressedLength) > math.MaxUint64-info.StartSequence {
		return TerminalOutputInfo{}, &Error{Code: InvalidFrame}
	}
	payload := message[terminalOutputHeaderLen:]
	if info.Encoding == TerminalOutputRaw {
		if len(payload) != int(info.UncompressedLength) {
			return TerminalOutputInfo{}, &Error{Code: InvalidFrame}
		}
		return info, nil
	}
	var header zstd.Header
	if err := header.Decode(payload); err != nil || header.Skippable || header.DictionaryID != 0 || !header.HasFCS || header.FrameContentSize != uint64(info.UncompressedLength) || !header.SingleSegment && header.WindowSize > MaxTerminalOutputBytes {
		return TerminalOutputInfo{}, &Error{Code: InvalidFrame, Cause: errors.New("invalid zstd frame header")}
	}
	if size, err := zstdFrameSize(payload, header); err != nil || size != len(payload) {
		return TerminalOutputInfo{}, &Error{Code: InvalidFrame, Cause: errors.New("zstd payload must contain exactly one frame")}
	}
	return info, nil
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
	declared := frame.UncompressedLength
	if declared == 0 && frame.Encoding == TerminalOutputRaw {
		declared = uint32(len(frame.Data))
	}
	if (frame.Channel != TerminalStdout && frame.Channel != TerminalStderr) || frame.StreamID == 0 || len(frame.Data) == 0 || len(frame.Data) > MaxBinaryFrame-terminalOutputHeaderLen || declared == 0 || declared > MaxBinaryFrame-terminalOutputHeaderLen || uint64(declared) > math.MaxUint64-frame.StartSequence {
		return nil, &Error{Code: InvalidFrame}
	}
	if frame.Encoding != TerminalOutputRaw && frame.Encoding != TerminalOutputZstd {
		return nil, &Error{Code: InvalidFrame}
	}
	if frame.Encoding == TerminalOutputRaw && int(declared) != len(frame.Data) {
		return nil, &Error{Code: InvalidFrame}
	}
	dst = growFrame(dst, terminalOutputHeaderLen+len(frame.Data))
	dst[0], dst[1], dst[2] = TerminalOutputOpcode, frame.Channel, frame.Encoding
	binary.BigEndian.PutUint32(dst[3:7], frame.StreamID)
	binary.BigEndian.PutUint64(dst[7:15], frame.StartSequence)
	binary.BigEndian.PutUint32(dst[15:19], declared)
	copy(dst[19:], frame.Data)
	return dst, nil
}

func DecodeTerminalOutput(message []byte) (TerminalOutputFrame, error) {
	info, err := InspectTerminalOutput(message)
	if err != nil {
		terminalCompressionCounters.decodeFailures.Add(1)
		return TerminalOutputFrame{}, err
	}
	declared := int(info.UncompressedLength)
	var decoded []byte
	if info.Encoding == TerminalOutputRaw {
		decoded = message[19:]
	} else {
		started := time.Now()
		decoded, err = zstdDecode(message[19:], declared)
		terminalCompressionCounters.decodeNanos.Add(uint64(time.Since(started).Nanoseconds()))
		if err != nil {
			terminalCompressionCounters.decodeFailures.Add(1)
			return TerminalOutputFrame{}, &Error{Code: InvalidFrame, Cause: err}
		}
	}
	frame := TerminalOutputFrame{Channel: message[1], Encoding: info.Encoding, UncompressedLength: info.UncompressedLength, StreamID: info.StreamID, StartSequence: info.StartSequence, Data: decoded}
	if len(decoded) != declared {
		return TerminalOutputFrame{}, &Error{Code: InvalidFrame}
	}
	return frame, nil
}

func EncodeTerminalOutputAdaptive(frame TerminalOutputFrame, dst []byte) ([]byte, error) {
	started := time.Now()
	frame.Encoding = TerminalOutputRaw
	frame.UncompressedLength = uint32(len(frame.Data))
	decision := &terminalCompressionCounters.smallFrames
	if len(frame.Data) >= TerminalOutputMinCompress {
		decision = &terminalCompressionCounters.insufficientFrames
		terminalCodecSlots <- struct{}{}
		enc, err := takeTerminalEncoder()
		if err != nil {
			<-terminalCodecSlots
			terminalCompressionCounters.encodeFailures.Add(1)
			return nil, &Error{Code: InvalidFrame, Cause: err}
		}
		compressed := enc.EncodeAll(frame.Data, nil)
		terminalEncoderPool.Put(enc)
		<-terminalCodecSlots
		if len(compressed)+TerminalOutputMinSavingsBytes <= len(frame.Data) && len(compressed)*100 <= len(frame.Data)*(100-TerminalOutputMinSavingsPercent) {
			frame.Encoding, frame.Data = TerminalOutputZstd, compressed
			decision = &terminalCompressionCounters.compressedFrames
		}
	}
	wire, err := EncodeTerminalOutput(frame, dst)
	terminalCompressionCounters.encodeNanos.Add(uint64(time.Since(started).Nanoseconds()))
	if err != nil {
		terminalCompressionCounters.encodeFailures.Add(1)
		return nil, err
	}
	terminalCompressionCounters.encodedBytes.Add(uint64(len(wire) - terminalOutputHeaderLen))
	terminalCompressionCounters.rawBytes.Add(uint64(frame.UncompressedLength))
	decision.Add(1)
	return wire, nil
}

func zstdDecode(payload []byte, declared int) ([]byte, error) {
	var header zstd.Header
	if err := header.Decode(payload); err != nil || header.Skippable || header.DictionaryID != 0 || !header.HasFCS || header.FrameContentSize != uint64(declared) || !header.SingleSegment && header.WindowSize > MaxTerminalOutputBytes {
		return nil, errors.New("invalid zstd frame header")
	}
	if size, err := zstdFrameSize(payload, header); err != nil || size != len(payload) {
		return nil, errors.New("zstd payload must contain exactly one frame")
	}
	terminalCodecSlots <- struct{}{}
	defer func() { <-terminalCodecSlots }()
	dec, err := takeTerminalDecoder()
	if err != nil {
		return nil, err
	}
	defer terminalDecoderPool.Put(dec)
	out, err := dec.DecodeAll(payload, make([]byte, 0, declared))
	if err != nil || len(out) != declared {
		return nil, errors.New("invalid zstd output")
	}
	return out, nil
}

func takeTerminalEncoder() (*zstd.Encoder, error) {
	if pooled := terminalEncoderPool.Get(); pooled != nil {
		return pooled.(*zstd.Encoder), nil
	}
	return zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false), zstd.WithEncoderConcurrency(1))
}

func takeTerminalDecoder() (*zstd.Decoder, error) {
	if pooled := terminalDecoderPool.Get(); pooled != nil {
		return pooled.(*zstd.Decoder), nil
	}
	return zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(MaxBinaryFrame)*4), zstd.WithDecoderMaxWindow(uint64(MaxBinaryFrame)), zstd.WithDecoderConcurrency(1))
}

func zstdFrameSize(payload []byte, header zstd.Header) (int, error) {
	offset := header.HeaderSize
	for {
		if len(payload)-offset < 3 {
			return 0, errors.New("truncated zstd block header")
		}
		block := uint32(payload[offset]) | uint32(payload[offset+1])<<8 | uint32(payload[offset+2])<<16
		offset += 3
		last := block&1 != 0
		blockType := (block >> 1) & 3
		blockSize := int(block >> 3)
		if blockType == 3 {
			return 0, errors.New("reserved zstd block type")
		}
		payloadSize := blockSize
		if blockType == 1 {
			payloadSize = 1
		}
		if payloadSize < 0 || payloadSize > len(payload)-offset {
			return 0, errors.New("truncated zstd block")
		}
		offset += payloadSize
		if last {
			if header.HasCheckSum {
				if len(payload)-offset < 4 {
					return 0, errors.New("truncated zstd checksum")
				}
				offset += 4
			}
			return offset, nil
		}
	}
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
