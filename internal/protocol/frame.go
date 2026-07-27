package protocol

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"unicode/utf8"
)

const MaxStructuredFrame = 64 << 10

type Code string

const (
	Malformed          Code = "malformed_frame"
	Oversized          Code = "frame_too_large"
	UnsupportedMessage Code = "unsupported_message_type"
	InvalidFrame       Code = "invalid_frame"
	CapabilityRequired Code = "capability_required"
	InvalidDeadline    Code = "invalid_deadline"
	CredentialExpired  Code = "credential_expired"
)

type Error struct {
	Code  Code
	Cause error
}

func (e *Error) Error() string {
	if e.Cause == nil {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Cause.Error()
}
func (e *Error) Unwrap() error { return e.Cause }

type Frame struct {
	Type        string          `json:"type"`
	RequestID   string          `json:"request_id"`
	Version     string          `json:"version"`
	OperationID string          `json:"operation_id,omitempty"`
	Capability  string          `json:"capability,omitempty"`
	DeadlineMS  uint32          `json:"deadline_ms,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

var allowedTypes = map[string]bool{"hello": true, "welcome": true, "request": true, "response": true, "error": true, "event": true, "cancel": true, "heartbeat": true, "detach": true}
var capabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{1,63}$`)

func (f Frame) Validate() error {
	if !allowedTypes[f.Type] {
		return &Error{Code: UnsupportedMessage}
	}
	if len(f.RequestID) < 1 || len(f.RequestID) > 128 || f.Version != ProtocolVersion {
		return &Error{Code: InvalidFrame}
	}
	if f.OperationID != "" && (len(f.OperationID) < 8 || len(f.OperationID) > 128) {
		return &Error{Code: InvalidFrame}
	}
	if f.Capability != "" && !capabilityPattern.MatchString(f.Capability) {
		return &Error{Code: CapabilityRequired}
	}
	if f.Payload != nil {
		if !json.Valid(f.Payload) {
			return &Error{Code: InvalidFrame, Cause: errors.New("invalid payload JSON")}
		}
		if err := rejectDuplicateKeys(f.Payload); err != nil {
			return &Error{Code: InvalidFrame, Cause: err}
		}
	}
	switch f.Type {
	case "hello":
		if f.Payload == nil {
			return &Error{Code: InvalidFrame}
		}
	case "request":
		if f.OperationID == "" || f.Payload == nil {
			return &Error{Code: InvalidFrame}
		}
		if f.Capability == "" {
			return &Error{Code: CapabilityRequired}
		}
		if f.DeadlineMS < 1 || f.DeadlineMS > 300000 {
			return &Error{Code: InvalidDeadline}
		}
	case "cancel":
		if f.OperationID == "" {
			return &Error{Code: InvalidFrame}
		}
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytesReader(data))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate or invalid object key")
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing payload JSON")
	}
	return nil
}

func ReadFrame(r io.Reader) (Frame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(r, prefix[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(prefix[:])
	if n == 0 {
		return Frame{}, &Error{Code: Malformed, Cause: errors.New("empty frame")}
	}
	if n > MaxStructuredFrame {
		return Frame{}, &Error{Code: Oversized}
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return Frame{}, &Error{Code: Malformed, Cause: err}
	}
	if !utf8.Valid(b) {
		return Frame{}, &Error{Code: Malformed, Cause: errors.New("invalid utf-8")}
	}
	if err := rejectDuplicateKeys(b); err != nil {
		return Frame{}, &Error{Code: Malformed, Cause: err}
	}
	var f Frame
	dec := json.NewDecoder(bytesReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return Frame{}, &Error{Code: Malformed, Cause: err}
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return Frame{}, &Error{Code: Malformed, Cause: errors.New("trailing JSON")}
	}
	if err := f.Validate(); err != nil {
		return Frame{}, err
	}
	return f, nil
}

func WriteFrame(w io.Writer, f Frame) error {
	if err := f.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(f)
	if err != nil {
		return &Error{Code: Malformed, Cause: err}
	}
	if len(b) > MaxStructuredFrame {
		return &Error{Code: Oversized}
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(b)))
	if err = writeAll(w, prefix[:]); err != nil {
		return fmt.Errorf("write frame prefix: %w", err)
	}
	if err = writeAll(w, b); err != nil {
		return fmt.Errorf("write frame: %w", err)
	}
	return nil
}

// bytesReader keeps the decoder input bounded and avoids exposing a mutable buffer.
type byteReader struct {
	b   []byte
	off int
}

func bytesReader(b []byte) io.Reader { return &byteReader{b: b} }
func (r *byteReader) Read(p []byte) (int, error) {
	if r.off == len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}
