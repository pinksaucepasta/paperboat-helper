package server

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

const (
	TerminalStreamContentType      = "application/vnd.paperboat.terminal.v2+records"
	recordKindStructured      byte = 1
	recordKindBinary          byte = 2
	maxBinaryRecord                = 256 << 10
)

type HTTPStreamHandlerConfig struct {
	Server     *Server
	Authorizer AuthorizerFactory
	Limiter    *ConnectionLimiter
}

type HTTPStreamHandler struct{ config HTTPStreamHandlerConfig }

func NewHTTPStreamHandler(config HTTPStreamHandlerConfig) (*HTTPStreamHandler, error) {
	if config.Server == nil || config.Authorizer == nil || config.Limiter == nil {
		return nil, ErrInvalidConfiguration
	}
	return &HTTPStreamHandler{config: config}, nil
}

func (h *HTTPStreamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Content-Type") != TerminalStreamContentType {
		http.Error(w, http.StatusText(http.StatusUnsupportedMediaType), http.StatusUnsupportedMediaType)
		return
	}
	token, ok := bearerToken(r.Header.Values("Authorization"))
	if !ok {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	authorizer, err := h.config.Authorizer(token)
	if err != nil || authorizer == nil {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	if !h.config.Limiter.acquire() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer h.config.Limiter.release()

	controller := http.NewResponseController(w)
	if err := controller.EnableFullDuplex(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", TerminalStreamContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return
	}

	connection := newHTTPStreamConnection(r.Context(), r.Body, w, controller)
	_ = h.config.Server.ServeAuthenticated(connection, authorizer)
}

type httpStreamConnection struct {
	ctx        context.Context
	cancel     context.CancelFunc
	body       io.ReadCloser
	writer     io.Writer
	controller *http.ResponseController
	readMu     sync.Mutex
	writeMu    sync.Mutex
	closeOnce  sync.Once
	revoked    atomic.Bool
}

func newHTTPStreamConnection(parent context.Context, body io.ReadCloser, writer io.Writer, controller *http.ResponseController) *httpStreamConnection {
	ctx, cancel := context.WithCancel(parent)
	connection := &httpStreamConnection{ctx: ctx, cancel: cancel, body: body, writer: writer, controller: controller}
	context.AfterFunc(ctx, func() { connection.revoked.Store(true) })
	return connection
}

func (c *httpStreamConnection) RevocationFlag() *atomic.Bool { return &c.revoked }
func (c *httpStreamConnection) Read([]byte) (int, error) {
	return 0, errors.New("stream reads are unavailable for terminal protocol 2.0")
}

func (c *httpStreamConnection) ReadApplication() (protocol.Frame, []byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	kind, payload, err := readRecord(c.body)
	if err != nil {
		return protocol.Frame{}, nil, err
	}
	if kind == recordKindBinary {
		return protocol.Frame{}, payload, nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	var frame protocol.Frame
	if err := decoder.Decode(&frame); err != nil || frame.Type == "" || frame.RequestID == "" || frame.Version != protocol.ProtocolVersion || decoder.Decode(&struct{}{}) != io.EOF {
		return protocol.Frame{}, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("invalid structured message")}
	}
	return frame, nil, nil
}

func readRecord(reader io.Reader) (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	limit := uint32(maxBinaryRecord)
	if header[0] == recordKindStructured {
		limit = protocol.MaxStructuredFrame
		if length == 0 {
			return 0, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("empty structured record")}
		}
	} else if header[0] != recordKindBinary {
		return 0, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("unknown record kind")}
	}
	if length == 0 || length > limit {
		return 0, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("record length exceeds limit")}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

func (c *httpStreamConnection) Write(data []byte) (int, error) {
	if err := c.writeRecord(recordKindStructured, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *httpStreamConnection) WriteStructured(frame protocol.Frame) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return c.writeRecord(recordKindStructured, encoded)
}

func (c *httpStreamConnection) WriteBinary(frame protocol.BinaryFrame) error {
	encoded, err := protocol.EncodeTerminalOutput(protocol.TerminalOutputFrame{Channel: frame.Channel, StreamID: 1, StartSequence: frame.StartSequence, Data: frame.Data}, nil)
	if err != nil {
		return err
	}
	return c.writeRecord(recordKindBinary, encoded)
}

func (c *httpStreamConnection) WriteTerminalOutput(streamID uint32, frame protocol.BinaryFrame) error {
	encoded, err := protocol.EncodeTerminalOutput(protocol.TerminalOutputFrame{Channel: frame.Channel, StreamID: streamID, StartSequence: frame.StartSequence, Data: frame.Data}, nil)
	if err != nil {
		return err
	}
	return c.writeRecord(recordKindBinary, encoded)
}

func (c *httpStreamConnection) writeRecord(kind byte, payload []byte) error {
	limit := maxBinaryRecord
	if kind == recordKindStructured {
		limit = protocol.MaxStructuredFrame
	}
	if len(payload) == 0 || len(payload) > limit {
		return &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("record length exceeds limit")}
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	var header [5]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(payload)))
	if _, err := c.writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := c.writer.Write(payload); err != nil {
		return err
	}
	return c.controller.Flush()
}

func (c *httpStreamConnection) Close() error { return c.CloseProtocol(protocol.CloseNormal, "closed") }
func (c *httpStreamConnection) CloseProtocol(int, string) error {
	var err error
	c.closeOnce.Do(func() { c.cancel(); err = c.body.Close() })
	return err
}
