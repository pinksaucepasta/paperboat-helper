package nativeproto

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	Version byte = 1

	RoleControl byte = 1
	RoleInput   byte = 2
	RoleOutput  byte = 3

	ConnectionIDSize = 16
	BindingSize      = 32
	MaxTokenSize     = 16 << 10
	MaxRecordSize    = 1 << 20

	RecordStructured byte = 1
	RecordBinary     byte = 2
)

var (
	Magic      = [4]byte{'P', 'B', 'T', '1'}
	ErrPreface = errors.New("invalid PBT1 preface")
	ErrRecord  = errors.New("invalid PBT1 record")
)

type Preface struct {
	Role         byte
	ConnectionID [ConnectionIDSize]byte
	Binding      []byte
	Token        string
}

func ReadPreface(r io.Reader) (Preface, error) {
	var header [4 + 1 + 1 + ConnectionIDSize + 2 + 2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Preface{}, err
	}
	if string(header[:4]) != string(Magic[:]) || header[4] != Version {
		return Preface{}, ErrPreface
	}
	p := Preface{Role: header[5]}
	copy(p.ConnectionID[:], header[6:6+ConnectionIDSize])
	bindingLen := int(binary.BigEndian.Uint16(header[22:24]))
	tokenLen := int(binary.BigEndian.Uint16(header[24:26]))
	if zeroID(p.ConnectionID) || tokenLen > MaxTokenSize || !validLengths(p.Role, bindingLen, tokenLen) {
		return Preface{}, ErrPreface
	}
	p.Binding = make([]byte, bindingLen)
	token := make([]byte, tokenLen)
	if _, err := io.ReadFull(r, p.Binding); err != nil {
		return Preface{}, err
	}
	if _, err := io.ReadFull(r, token); err != nil {
		return Preface{}, err
	}
	p.Token = string(token)
	return p, nil
}

func WritePreface(w io.Writer, p Preface) error {
	if zeroID(p.ConnectionID) || len(p.Token) > MaxTokenSize || !validLengths(p.Role, len(p.Binding), len(p.Token)) {
		return ErrPreface
	}
	buffer := make([]byte, 26+len(p.Binding)+len(p.Token))
	copy(buffer[:4], Magic[:])
	buffer[4], buffer[5] = Version, p.Role
	copy(buffer[6:22], p.ConnectionID[:])
	binary.BigEndian.PutUint16(buffer[22:24], uint16(len(p.Binding)))
	binary.BigEndian.PutUint16(buffer[24:26], uint16(len(p.Token)))
	copy(buffer[26:], p.Binding)
	copy(buffer[26+len(p.Binding):], p.Token)
	return writeFull(w, buffer)
}

func ReadRecord(r io.Reader, typed bool) (byte, []byte, error) {
	headerSize := 4
	if typed {
		headerSize = 5
	}
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	kind, offset := byte(RecordBinary), 0
	if typed {
		kind, offset = header[0], 1
		if kind != RecordStructured && kind != RecordBinary {
			return 0, nil, ErrRecord
		}
	}
	length := binary.BigEndian.Uint32(header[offset:])
	if length == 0 || length > MaxRecordSize {
		return 0, nil, ErrRecord
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return kind, payload, nil
}

func WriteRecord(w io.Writer, kind byte, payload []byte, typed bool) error {
	if len(payload) == 0 || len(payload) > MaxRecordSize || typed && kind != RecordStructured && kind != RecordBinary {
		return ErrRecord
	}
	headerSize := 4
	if typed {
		headerSize = 5
	}
	record := make([]byte, headerSize+len(payload))
	if typed {
		record[0] = kind
	}
	binary.BigEndian.PutUint32(record[headerSize-4:headerSize], uint32(len(payload)))
	copy(record[headerSize:], payload)
	return writeFull(w, record)
}

func validLengths(role byte, bindingLen, tokenLen int) bool {
	return role == RoleControl && bindingLen == 0 && tokenLen > 0 ||
		(role == RoleInput || role == RoleOutput) && bindingLen == BindingSize && tokenLen == 0
}

func zeroID(id [ConnectionIDSize]byte) bool {
	var zero [ConnectionIDSize]byte
	return id == zero
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) != 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}
