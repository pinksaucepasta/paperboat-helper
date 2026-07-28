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
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

func TestHTTPRecordCarriesCanonicalTerminalV2Payload(t *testing.T) {
	data, err := os.ReadFile("../../testdata/contracts/fixtures/helper/terminal-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Case  string `json:"case"`
			Valid bool   `json:"valid"`
			Wire  string `json:"wire_base64"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, test := range fixture.Cases {
		if !test.Valid {
			continue
		}
		t.Run(test.Case, func(t *testing.T) {
			payload, err := base64.StdEncoding.DecodeString(test.Wire)
			if err != nil {
				t.Fatal(err)
			}
			kind, got, err := readRecord(bytes.NewReader(encodeTestRecord(recordKindBinary, payload)))
			if err != nil || kind != recordKindBinary || !bytes.Equal(got, payload) {
				t.Fatalf("kind=%d payload=%x err=%v", kind, got, err)
			}
		})
	}
}

func encodeTestRecord(kind byte, payload []byte) []byte {
	result := make([]byte, 5+len(payload))
	result[0] = kind
	binary.BigEndian.PutUint32(result[1:5], uint32(len(payload)))
	copy(result[5:], payload)
	return result
}

func TestRecordCodecHandlesFragmentationCoalescingAndBounds(t *testing.T) {
	structured := []byte(`{"type":"hello"}`)
	binaryFrame := []byte{1, 2, 3}
	wire := append(encodeTestRecord(recordKindStructured, structured), encodeTestRecord(recordKindBinary, binaryFrame)...)
	reader := &oneByteReader{reader: bytes.NewReader(wire)}
	kind, payload, err := readRecord(reader)
	if err != nil || kind != recordKindStructured || !bytes.Equal(payload, structured) {
		t.Fatalf("structured kind=%d payload=%q err=%v", kind, payload, err)
	}
	kind, payload, err = readRecord(reader)
	if err != nil || kind != recordKindBinary || !bytes.Equal(payload, binaryFrame) {
		t.Fatalf("binary kind=%d payload=%v err=%v", kind, payload, err)
	}
	for name, test := range map[string]struct {
		kind byte
		size int
	}{
		"maximum-structured": {kind: recordKindStructured, size: protocol.MaxStructuredFrame},
		"maximum-binary":     {kind: recordKindBinary, size: maxBinaryRecord},
	} {
		t.Run(name, func(t *testing.T) {
			want := bytes.Repeat([]byte{'x'}, test.size)
			kind, got, err := readRecord(bytes.NewReader(encodeTestRecord(test.kind, want)))
			if err != nil || kind != test.kind || !bytes.Equal(got, want) {
				t.Fatalf("kind=%d bytes=%d err=%v", kind, len(got), err)
			}
		})
	}

	for name, wire := range map[string][]byte{
		"unknown-kind":         encodeTestRecord(9, []byte("x")),
		"empty-structured":     encodeTestRecord(recordKindStructured, nil),
		"oversized-structured": headerOnly(recordKindStructured, protocol.MaxStructuredFrame+1),
		"oversized-binary":     headerOnly(recordKindBinary, maxBinaryRecord+1),
		"truncated-header":     {recordKindStructured, 0},
		"truncated-payload":    append(headerOnly(recordKindBinary, 2), 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := readRecord(bytes.NewReader(wire))
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

type oneByteReader struct{ reader io.Reader }

func (r *oneByteReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 1 {
		buffer = buffer[:1]
	}
	return r.reader.Read(buffer)
}

func headerOnly(kind byte, length uint32) []byte {
	result := make([]byte, 5)
	result[0] = kind
	binary.BigEndian.PutUint32(result[1:], length)
	return result
}

func TestHTTPStreamConnectionRejectsMalformedStructuredJSON(t *testing.T) {
	connection := newHTTPStreamConnection(context.Background(), io.NopCloser(bytes.NewReader(encodeTestRecord(recordKindStructured, []byte(`{"type":`)))), io.Discard, http.NewResponseController(httptest.NewRecorder()))
	_, _, err := connection.ReadApplication()
	var protocolErr *protocol.Error
	if !errors.As(err, &protocolErr) || protocolErr.Code != protocol.InvalidFrame {
		t.Fatalf("err=%v", err)
	}
}

func TestHTTPStreamHandlerFlushesBeforeOpenRequestCompletes(t *testing.T) {
	journal, err := operation.NewJournal(16)
	if err != nil {
		t.Fatal(err)
	}
	protocolServer, err := New(Config{
		Negotiator: protocol.Negotiator{Profile: config.BYOD, Available: map[string]bool{"terminal.v2": true, "health.v1": true}},
		Journal:    journal, Handler: websocketDomainHandler{}, MaxConcurrent: 4,
		HeartbeatInterval: time.Hour, PeerTimeout: 2 * time.Hour, MutationDeadline: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = protocolServer.Shutdown(ctx)
	})
	limiter, _ := NewConnectionLimiter(1)
	seen := ""
	handler, err := NewHTTPStreamHandler(HTTPStreamHandlerConfig{Server: protocolServer, Limiter: limiter, Authorizer: func(token string) (Authorizer, error) {
		seen = token
		return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
			return Authorization{JournalBinding: "principal", ClientID: "cli", SessionID: "ses_test"}, nil
		}), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestReader, requestWriter := io.Pipe()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, requestReader)
	request.Header.Set("Authorization", "Bearer signed-credential")
	request.Header.Set("Content-Type", TerminalStreamContentType)
	responseResult := make(chan struct {
		response *http.Response
		err      error
	}, 1)
	go func() {
		response, requestErr := server.Client().Do(request)
		responseResult <- struct {
			response *http.Response
			err      error
		}{response, requestErr}
	}()

	var response *http.Response
	select {
	case result := <-responseResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		response = result.response
	case <-time.After(time.Second):
		t.Fatal("response headers waited for the open request body")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != TerminalStreamContentType {
		t.Fatalf("status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}

	hello, _ := json.Marshal(protocol.Frame{Type: "hello", RequestID: "req_hello", Version: protocol.ProtocolVersion, Payload: json.RawMessage(`{"min_version":"2.0","max_version":"2.0","capabilities":["terminal.v2","health.v1"]}`)})
	if _, err := requestWriter.Write(encodeTestRecord(recordKindStructured, hello)); err != nil {
		t.Fatal(err)
	}
	kind, payload, err := readRecord(response.Body)
	if err != nil || kind != recordKindStructured {
		t.Fatalf("kind=%d payload=%q err=%v", kind, payload, err)
	}
	var welcome protocol.Frame
	if err := json.Unmarshal(payload, &welcome); err != nil || welcome.Type != "welcome" {
		t.Fatalf("welcome=%#v err=%v", welcome, err)
	}
	if seen != "signed-credential" {
		t.Fatalf("token=%q", seen)
	}
	attach, _ := json.Marshal(protocol.Frame{Type: "request", RequestID: "req_stream", Version: protocol.ProtocolVersion, OperationID: "op_stream_0001", Capability: "terminal.v2", DeadlineMS: 1000, Payload: json.RawMessage(`{"action":"attach","session_id":"ses_test"}`)})
	if _, err := requestWriter.Write(encodeTestRecord(recordKindStructured, attach)); err != nil {
		t.Fatal(err)
	}
	kind, payload, err = readRecord(response.Body)
	if err != nil || kind != recordKindStructured {
		t.Fatalf("response kind=%d payload=%q err=%v", kind, payload, err)
	}
	var attached protocol.Frame
	if err := json.Unmarshal(payload, &attached); err != nil || attached.Type != "response" || !bytes.Contains(attached.Payload, []byte(`"stream_id":1`)) {
		t.Fatalf("attached=%#v err=%v", attached, err)
	}
	kind, payload, err = readRecord(response.Body)
	if err != nil || kind != recordKindBinary {
		t.Fatalf("binary kind=%d payload=%v err=%v", kind, payload, err)
	}
	output, err := protocol.DecodeTerminalOutput(payload)
	if err != nil || output.StreamID != 1 || output.StartSequence != 7 || string(output.Data) != "output" {
		t.Fatalf("output=%#v err=%v", output, err)
	}
	cancel()
	_ = requestWriter.Close()
}

func TestHTTPStreamHandlerRejectsInvalidRequestsBeforeServing(t *testing.T) {
	limiter, _ := NewConnectionLimiter(1)
	handler := &HTTPStreamHandler{config: HTTPStreamHandlerConfig{Limiter: limiter, Authorizer: func(string) (Authorizer, error) { return nil, errors.New("denied") }}}
	tests := []struct {
		method, contentType, authorization string
		want                               int
	}{
		{http.MethodGet, TerminalStreamContentType, "Bearer token", http.StatusMethodNotAllowed},
		{http.MethodPost, "application/json", "Bearer token", http.StatusUnsupportedMediaType},
		{http.MethodPost, TerminalStreamContentType, "", http.StatusUnauthorized},
		{http.MethodPost, TerminalStreamContentType, "Bearer token", http.StatusUnauthorized},
	}
	for _, test := range tests {
		request := httptest.NewRequest(test.method, "http://helper.test/v1/runtime-stream", nil)
		request.Header.Set("Content-Type", test.contentType)
		request.Header.Set("Authorization", test.authorization)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.want {
			t.Fatalf("method=%s status=%d want=%d", test.method, response.Code, test.want)
		}
	}
}

func TestHTTPStreamHandlerUsesSharedConnectionLimiter(t *testing.T) {
	limiter, _ := NewConnectionLimiter(1)
	if !limiter.acquire() {
		t.Fatal("failed to reserve shared slot")
	}
	defer limiter.release()
	handler := &HTTPStreamHandler{config: HTTPStreamHandlerConfig{
		Limiter: limiter,
		Authorizer: func(string) (Authorizer, error) {
			return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) { return Authorization{}, nil }), nil
		},
	}}
	request := httptest.NewRequest(http.MethodPost, "http://helper.test/v1/runtime-stream", nil)
	request.Header.Set("Content-Type", TerminalStreamContentType)
	request.Header.Set("Authorization", "Bearer token")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q", response.Code, response.Header().Get("Retry-After"))
	}
}

type countedReadCloser struct{ closes atomic.Int32 }

func (*countedReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (r *countedReadCloser) Close() error {
	r.closes.Add(1)
	return nil
}

func TestHTTPStreamConnectionConcurrentCloseCancelsAndClosesBodyOnce(t *testing.T) {
	body := &countedReadCloser{}
	connection := newHTTPStreamConnection(context.Background(), body, io.Discard, http.NewResponseController(httptest.NewRecorder()))
	var group sync.WaitGroup
	for range 16 {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = connection.Close()
		}()
	}
	group.Wait()
	if body.closes.Load() != 1 {
		t.Fatalf("body closes=%d", body.closes.Load())
	}
	select {
	case <-connection.ctx.Done():
	default:
		t.Fatal("connection context was not canceled")
	}
}
