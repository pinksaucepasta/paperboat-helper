package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

type singleListener struct {
	mu     sync.Mutex
	conn   net.Conn
	closed chan struct{}
}

func (l *singleListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.conn != nil {
		connection := l.conn
		l.conn = nil
		l.mu.Unlock()
		return connection, nil
	}
	l.mu.Unlock()
	<-l.closed
	return nil, net.ErrClosed
}
func (l *singleListener) Close() error {
	select {
	case <-l.closed:
	default:
		close(l.closed)
	}
	return nil
}
func (l *singleListener) Addr() net.Addr { return pipeAddr("helper") }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

type websocketDomainHandler struct{}

func (websocketDomainHandler) Handle(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
	return operation.Outcome{Result: json.RawMessage(`{"ok":true}`)}
}
func (websocketDomainHandler) OpenStream(_ context.Context, _ Authorization, _ string, payload json.RawMessage, _ operation.Outcome, _ bool) (OutputStream, bool, error) {
	if !bytes.Contains(payload, []byte(`"stream":true`)) {
		return nil, false, nil
	}
	return &oneFrameStream{ready: true}, true, nil
}

type oneFrameStream struct {
	mu    sync.Mutex
	ready bool
}

func (s *oneFrameStream) Next(ctx context.Context) (protocol.BinaryFrame, error) {
	s.mu.Lock()
	if s.ready {
		s.ready = false
		s.mu.Unlock()
		return protocol.BinaryFrame{Channel: protocol.Stdout, StartSequence: 7, Data: []byte("output")}, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return protocol.BinaryFrame{}, ctx.Err()
}
func (*oneFrameStream) Close() error { return nil }

func websocketTestHandler(t *testing.T, tokenSeen *string) *WebSocketHandler {
	t.Helper()
	journal, _ := operation.NewJournal(16)
	server, err := New(Config{
		Negotiator: protocol.Negotiator{Profile: config.BYOD, Available: map[string]bool{"terminal.v1": true, "health.v1": true}},
		Journal:    journal, Handler: websocketDomainHandler{}, MaxConcurrent: 4,
		HeartbeatInterval: time.Hour, PeerTimeout: 2 * time.Hour, MutationDeadline: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	handler, err := NewWebSocketHandler(WebSocketHandlerConfig{Server: server, MaxConnections: 2, Authorizer: func(token string) (Authorizer, error) {
		*tokenSeen = token
		return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
			return Authorization{JournalBinding: "principal", ClientID: "cli"}, nil
		}), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func dialPipe(t *testing.T, handler http.Handler, headers http.Header, subprotocols []string) (*websocket.Conn, func()) {
	t.Helper()
	serverSide, clientSide := net.Pipe()
	listener := &singleListener{conn: serverSide, closed: make(chan struct{})}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: time.Second}
	done := make(chan struct{})
	go func() { _ = httpServer.Serve(listener); close(done) }()
	used := false
	transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
		if used {
			return nil, net.ErrClosed
		}
		used = true
		return clientSide, nil
	}}
	connection, response, err := websocket.Dial(context.Background(), "ws://helper.test/runtime", &websocket.DialOptions{HTTPClient: &http.Client{Transport: transport}, HTTPHeader: headers, Subprotocols: subprotocols})
	if err != nil {
		listener.Close()
		transport.CloseIdleConnections()
		<-done
		if response != nil {
			response.Body.Close()
		}
		t.Fatalf("dial: %v", err)
	}
	cleanup := func() {
		_ = connection.Close(websocket.StatusNormalClosure, "done")
		_ = listener.Close()
		transport.CloseIdleConnections()
		<-done
	}
	return connection, cleanup
}

func TestWebSocketHandlerRejectsMissingOrAmbiguousCredentialBeforeUpgrade(t *testing.T) {
	seen := ""
	handler := websocketTestHandler(t, &seen)
	for _, values := range [][]string{nil, {"Bearer one", "Bearer two"}, {"Basic token"}, {"Bearer"}} {
		request := httptest.NewRequest(http.MethodGet, "http://helper.test/runtime", nil)
		request.Header["Authorization"] = values
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("values=%v status=%d", values, response.Code)
		}
	}
	if seen != "" {
		t.Fatalf("credential reached factory: %q", seen)
	}
}

func TestDecodeTerminalInputPreservesBindingAndBytes(t *testing.T) {
	sessionID, attachmentID := "ses_12345678", "att_12345678"
	raw := []byte("\x1b[<64;20;10M")
	data := make([]byte, 12+len(sessionID)+len(attachmentID)+len(raw))
	binary.BigEndian.PutUint16(data[:2], uint16(len(sessionID)))
	binary.BigEndian.PutUint16(data[2:4], uint16(len(attachmentID)))
	binary.BigEndian.PutUint64(data[4:12], 7)
	copy(data[12:], sessionID)
	copy(data[12+len(sessionID):], attachmentID)
	copy(data[12+len(sessionID)+len(attachmentID):], raw)
	payload, err := decodeTerminalInput(protocol.BinaryFrame{Channel: protocol.TerminalInput, StartSequence: 9, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	var input terminalStreamInput
	if json.Unmarshal(payload, &input) != nil || input.Sequence != 9 || input.SessionID != sessionID || input.AttachmentID != attachmentID || input.Generation != 7 {
		t.Fatalf("input=%#v", input)
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(input.BytesBase64)
	if err != nil || !bytes.Equal(decoded, raw) {
		t.Fatalf("bytes=%q err=%v", decoded, err)
	}
}

func TestWebSocketHandlerRejectsCrossOriginBeforeUpgrade(t *testing.T) {
	seen := ""
	handler := websocketTestHandler(t, &seen)
	request := httptest.NewRequest(http.MethodGet, "http://helper.test/runtime", nil)
	request.Host = "helper.test"
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "13")
	request.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	request.Header.Set("Origin", "https://evil.test")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestWebSocketRequiresNegotiatedSubprotocol(t *testing.T) {
	seen := ""
	handler := websocketTestHandler(t, &seen)
	connection, cleanup := dialPipe(t, handler, http.Header{"Authorization": []string{"Bearer token"}}, nil)
	defer cleanup()
	_, _, err := connection.Read(context.Background())
	if status := websocket.CloseStatus(err); status != websocket.StatusPolicyViolation {
		t.Fatalf("status=%d err=%v", status, err)
	}
}

func TestWebSocketTransportFragmentsStructuredAndSeparatesBinary(t *testing.T) {
	seen := ""
	handler := websocketTestHandler(t, &seen)
	headers := http.Header{"Authorization": []string{"Bearer signed-credential"}}
	connection, cleanup := dialPipe(t, handler, headers, []string{DefaultWebSocketSubprotocol})
	defer cleanup()
	var hello bytes.Buffer
	_ = protocol.WriteFrame(&hello, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)})
	wire := hello.Bytes()
	if err := connection.Write(context.Background(), websocket.MessageText, wire[:3]); err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(context.Background(), websocket.MessageText, wire[3:]); err != nil {
		t.Fatal(err)
	}
	messageType, encoded, err := connection.Read(context.Background())
	if err != nil || messageType != websocket.MessageText {
		t.Fatalf("welcome type=%v err=%v", messageType, err)
	}
	welcome, err := protocol.ReadFrame(bytes.NewReader(encoded))
	if err != nil || welcome.Type != "welcome" {
		t.Fatalf("welcome=%#v err=%v", welcome, err)
	}
	var request bytes.Buffer
	_ = protocol.WriteFrame(&request, protocol.Frame{Type: "request", RequestID: "req_stream", Version: "1.0", OperationID: "op_stream_0001", Capability: "terminal.v1", DeadlineMS: 1000, Payload: json.RawMessage(`{"stream":true}`)})
	if err := connection.Write(context.Background(), websocket.MessageText, request.Bytes()); err != nil {
		t.Fatal(err)
	}
	messageType, encoded, err = connection.Read(context.Background())
	if err != nil || messageType != websocket.MessageText {
		t.Fatalf("response type=%v err=%v", messageType, err)
	}
	response, err := protocol.ReadFrame(bytes.NewReader(encoded))
	if err != nil || response.Type != "response" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	messageType, encoded, err = connection.Read(context.Background())
	if err != nil || messageType != websocket.MessageBinary {
		t.Fatalf("binary type=%v err=%v", messageType, err)
	}
	binary, err := protocol.ReadBinaryFrame(bytes.NewReader(encoded))
	if err != nil || binary.StartSequence != 7 || string(binary.Data) != "output" {
		t.Fatalf("binary=%#v err=%v", binary, err)
	}
	if seen != "signed-credential" {
		t.Fatalf("token=%q", seen)
	}
}

func TestWebSocketRejectsBinaryStructuredInputWithMalformedClose(t *testing.T) {
	seen := ""
	handler := websocketTestHandler(t, &seen)
	connection, cleanup := dialPipe(t, handler, http.Header{"Authorization": []string{"Bearer token"}}, []string{DefaultWebSocketSubprotocol})
	defer cleanup()
	if err := connection.Write(context.Background(), websocket.MessageBinary, []byte("not structured")); err != nil {
		t.Fatal(err)
	}
	_, _, err := connection.Read(context.Background())
	if status := websocket.CloseStatus(err); status != websocket.StatusCode(protocol.CloseMalformed) {
		t.Fatalf("status=%d err=%v", status, err)
	}
}
