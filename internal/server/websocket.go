package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

const (
	DefaultWebSocketSubprotocol = "paperboat.helper.v1"
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
}

type WebSocketHandler struct {
	config WebSocketHandlerConfig
	slots  chan struct{}
}

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
	if config.Server == nil || config.Authorizer == nil || config.MaxConnections < 1 || config.MaxMessageBytes < protocol.MaxStructuredFrame+4 || config.MaxMessageBytes > 4<<20 || len(config.Subprotocol) > 128 {
		return nil, ErrInvalidConfiguration
	}
	return &WebSocketHandler{config: config, slots: make(chan struct{}, config.MaxConnections)}, nil
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
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{Subprotocols: []string{h.config.Subprotocol}, OriginPatterns: append([]string(nil), h.config.OriginPatterns...), CompressionMode: websocket.CompressionDisabled})
	if err != nil {
		return
	}
	if connection.Subprotocol() != h.config.Subprotocol {
		_ = connection.Close(websocket.StatusPolicyViolation, "subprotocol_required")
		return
	}
	connection.SetReadLimit(h.config.MaxMessageBytes)
	wrapped := newWebSocketConnection(connection)
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
	reader     io.Reader
	closeOnce  sync.Once
}

func newWebSocketConnection(connection *websocket.Conn) *webSocketConnection {
	ctx, cancel := context.WithCancel(context.Background())
	return &webSocketConnection{connection: connection, ctx: ctx, cancel: cancel}
}

func (c *webSocketConnection) Read(buffer []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	for {
		if c.reader != nil {
			n, err := c.reader.Read(buffer)
			if err == io.EOF {
				c.reader = nil
			}
			if n > 0 {
				return n, nil
			}
			if err != nil && err != io.EOF {
				return 0, err
			}
		}
		messageType, reader, err := c.connection.Reader(c.ctx)
		if err != nil {
			return 0, classifyWebSocketRead(err)
		}
		if messageType == websocket.MessageBinary {
			binaryFrame, binaryErr := protocol.ReadBinaryFrame(reader)
			if binaryErr != nil || binaryFrame.Channel != protocol.TerminalInput {
				return 0, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("invalid terminal input frame")}
			}
			payload, payloadErr := decodeTerminalInput(binaryFrame)
			if payloadErr != nil {
				return 0, payloadErr
			}
			var encoded bytes.Buffer
			frame := protocol.Frame{Type: "input", RequestID: "input_" + strconv.FormatUint(binaryFrame.StartSequence, 10), Version: protocol.ProtocolVersion, Capability: "terminal.v1", Payload: payload}
			if writeErr := protocol.WriteFrame(&encoded, frame); writeErr != nil {
				return 0, writeErr
			}
			c.reader = bytes.NewReader(encoded.Bytes())
			continue
		}
		if messageType != websocket.MessageText {
			return 0, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("unsupported client message type")}
		}
		c.reader = reader
	}
}

func decodeTerminalInput(frame protocol.BinaryFrame) (json.RawMessage, error) {
	if len(frame.Data) < 12 {
		return nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("terminal input binding is truncated")}
	}
	sessionLength := int(binary.BigEndian.Uint16(frame.Data[:2]))
	attachmentLength := int(binary.BigEndian.Uint16(frame.Data[2:4]))
	generation := binary.BigEndian.Uint64(frame.Data[4:12])
	headerEnd := 12 + sessionLength + attachmentLength
	if sessionLength < 1 || sessionLength > 128 || attachmentLength < 1 || attachmentLength > 128 || generation == 0 || headerEnd >= len(frame.Data) {
		return nil, &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("terminal input binding is invalid")}
	}
	payload, err := json.Marshal(terminalStreamInput{
		Sequence: frame.StartSequence, SessionID: string(frame.Data[12 : 12+sessionLength]),
		AttachmentID: string(frame.Data[12+sessionLength : headerEnd]), Generation: generation,
		BytesBase64: base64.StdEncoding.EncodeToString(frame.Data[headerEnd:]),
	})
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *webSocketConnection) Write(data []byte) (int, error) {
	if err := c.connection.Write(c.ctx, websocket.MessageText, append([]byte(nil), data...)); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (c *webSocketConnection) WriteStructured(frame protocol.Frame) error {
	var encoded bytes.Buffer
	if err := protocol.WriteFrame(&encoded, frame); err != nil {
		return err
	}
	return c.connection.Write(c.ctx, websocket.MessageText, encoded.Bytes())
}

func (c *webSocketConnection) WriteBinary(frame protocol.BinaryFrame) error {
	var encoded bytes.Buffer
	if err := protocol.WriteBinaryFrame(&encoded, frame); err != nil {
		return err
	}
	return c.connection.Write(c.ctx, websocket.MessageBinary, encoded.Bytes())
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
