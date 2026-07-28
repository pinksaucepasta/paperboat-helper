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

	"github.com/coder/websocket"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

const (
	DefaultWebSocketSubprotocol = "paperboat.terminal.v2"
	DefaultMaxWebSocketMessage  = 1 << 20
)

type AuthorizerFactory func(string) (Authorizer, error)

type WebSocketHandlerConfig struct {
	Server          *Server
	Authorizer      AuthorizerFactory
	OriginPatterns  []string
	Subprotocol     string
	MaxConnections  int
	MaxMessageBytes int64
	Limiter         *ConnectionLimiter
}

type WebSocketHandler struct {
	config  WebSocketHandlerConfig
	limiter *ConnectionLimiter
}

type ConnectionLimiter struct{ slots chan struct{} }

func NewConnectionLimiter(max int) (*ConnectionLimiter, error) {
	if max < 1 {
		return nil, ErrInvalidConfiguration
	}
	return &ConnectionLimiter{slots: make(chan struct{}, max)}, nil
}

func (l *ConnectionLimiter) acquire() bool {
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *ConnectionLimiter) release() { <-l.slots }

func NewWebSocketHandler(config WebSocketHandlerConfig) (*WebSocketHandler, error) {
	if config.Subprotocol == "" {
		config.Subprotocol = DefaultWebSocketSubprotocol
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 128
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = DefaultMaxWebSocketMessage
	}
	if config.Limiter == nil {
		config.Limiter, _ = NewConnectionLimiter(config.MaxConnections)
	}
	if config.Server == nil || config.Authorizer == nil || config.MaxConnections < 1 || config.MaxMessageBytes < protocol.MaxStructuredFrame+4 || config.MaxMessageBytes > 4<<20 || len(config.Subprotocol) > 128 {
		return nil, ErrInvalidConfiguration
	}
	return &WebSocketHandler{config: config, limiter: config.Limiter}, nil
}

func (h *WebSocketHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	token, ok := bearerToken(request.Header.Values("Authorization"))
	if !ok {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	authorizer, err := h.config.Authorizer(token)
	if err != nil || authorizer == nil {
		http.Error(writer, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	if !h.limiter.acquire() {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	defer h.limiter.release()
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{h.config.Subprotocol}, OriginPatterns: append([]string(nil), h.config.OriginPatterns...), CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	if connection.Subprotocol() != h.config.Subprotocol {
		_ = connection.Close(websocket.StatusPolicyViolation, "subprotocol_required")
		return
	}
	connection.SetReadLimit(h.config.MaxMessageBytes)
	wrapped := newWebSocketConnection(request.Context(), connection)
	_ = h.config.Server.ServeAuthenticated(wrapped, authorizer)
}

func bearerToken(values []string) (string, bool) {
	if len(values) != 1 || len(values[0]) > (16<<10)+16 {
		return "", false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) == 0 || len(parts[1]) > 16<<10 {
		return "", false
	}
	return parts[1], true
}

type webSocketConnection struct {
	connection *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	readMu     sync.Mutex
	closeOnce  sync.Once
	revoked    atomic.Bool
}

func newWebSocketConnection(parent context.Context, connection *websocket.Conn) *webSocketConnection {
	ctx, cancel := context.WithCancel(parent)
	wrapped := &webSocketConnection{connection: connection, ctx: ctx, cancel: cancel}
	context.AfterFunc(ctx, func() { wrapped.revoked.Store(true) })
	return wrapped
}

func (c *webSocketConnection) RevocationFlag() *atomic.Bool { return &c.revoked }

func (c *webSocketConnection) Read(buffer []byte) (int, error) {
	return 0, errors.New("stream reads are unavailable for terminal protocol 2.0")
}

func (c *webSocketConnection) ReadApplication() (protocol.Frame, []byte, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	messageType, data, err := c.connection.Read(c.ctx)
	if err != nil {
		return protocol.Frame{}, nil, classifyWebSocketRead(err)
	}
	if messageType == websocket.MessageBinary {
		return protocol.Frame{}, data, nil
	}
	if messageType != websocket.MessageText || len(data) == 0 || len(data) > protocol.MaxStructuredFrame {
		return protocol.Frame{}, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("invalid structured message")}
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	var frame protocol.Frame
	if err := decoder.Decode(&frame); err != nil || frame.Type == "" || frame.RequestID == "" || frame.Version != protocol.ProtocolVersion {
		return protocol.Frame{}, nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("invalid structured message")}
	}
	return frame, nil, nil
}

func (c *webSocketConnection) Write(data []byte) (int, error) {
	if err := c.connection.Write(c.ctx, websocket.MessageText, append([]byte(nil), data...)); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *webSocketConnection) WriteStructured(frame protocol.Frame) error {
	encoded, err := json.Marshal(frame)
	if err != nil || len(encoded) > protocol.MaxStructuredFrame {
		return err
	}
	return c.connection.Write(c.ctx, websocket.MessageText, encoded)
}

func (c *webSocketConnection) WriteBinary(frame protocol.BinaryFrame) error {
	encoded, err := protocol.EncodeTerminalOutput(protocol.TerminalOutputFrame{Channel: frame.Channel, StreamID: 1, StartSequence: frame.StartSequence, Data: frame.Data}, nil)
	if err != nil {
		return err
	}
	return c.connection.Write(c.ctx, websocket.MessageBinary, encoded)
}

func (c *webSocketConnection) WriteTerminalOutput(streamID uint32, frame protocol.BinaryFrame) error {
	if streamID == 0 || len(frame.Data) == 0 || (frame.Channel != protocol.TerminalStdout && frame.Channel != protocol.TerminalStderr) {
		return &protocol.Error{Code: protocol.InvalidFrame}
	}
	writer, err := c.connection.Writer(c.ctx, websocket.MessageBinary)
	if err != nil {
		return err
	}
	var header [14]byte
	header[0], header[1] = protocol.TerminalOutputOpcode, frame.Channel
	binary.BigEndian.PutUint32(header[2:6], streamID)
	binary.BigEndian.PutUint64(header[6:14], frame.StartSequence)
	if _, err = writer.Write(header[:]); err == nil {
		_, err = writer.Write(frame.Data)
	}
	return errors.Join(err, writer.Close())
}

func (c *webSocketConnection) Close() error { return c.CloseProtocol(protocol.CloseNormal, "closed") }

func (c *webSocketConnection) CloseProtocol(code int, reason string) error {
	var result error
	c.closeOnce.Do(func() {
		c.cancel()
		result = c.connection.Close(websocket.StatusCode(code), reason)
	})
	return result
}

func classifyWebSocketRead(err error) error {
	status := websocket.CloseStatus(err)
	if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
		return io.EOF
	}
	return err
}
