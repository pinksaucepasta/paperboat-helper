package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/history"
	"github.com/pinksaucepasta/paperboat-helper/internal/observability"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type authorizerFunc func(context.Context, protocol.Frame) (Authorization, error)

func (f authorizerFunc) Authorize(ctx context.Context, frame protocol.Frame) (Authorization, error) {
	return f(ctx, frame)
}

type handlerFunc func(context.Context, Authorization, string, json.RawMessage) operation.Outcome

func (f handlerFunc) Handle(ctx context.Context, authorization Authorization, capability string, payload json.RawMessage) operation.Outcome {
	return f(ctx, authorization, capability, payload)
}

func testServer(t *testing.T, authorize authorizerFunc, handle handlerFunc, maxConcurrent int) *Server {
	t.Helper()
	journal, err := operation.NewJournal(32)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Config{
		Negotiator: protocol.Negotiator{Profile: config.BYOD, Available: map[string]bool{"terminal.v1": true, "health.v1": true}},
		Journal:    journal, Authorizer: authorize, Handler: handle, MaxConcurrent: maxConcurrent,
		HeartbeatInterval: time.Hour, PeerTimeout: 2 * time.Hour, MutationDeadline: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return server
}

func hello(t *testing.T, conn net.Conn) protocol.Frame {
	t.Helper()
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","health.v1"]}`)
	if err := protocol.WriteFrame(conn, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	frame, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != "welcome" {
		t.Fatalf("welcome=%#v", frame)
	}
	return frame
}

func request(id, operationID string, payload json.RawMessage) protocol.Frame {
	return protocol.Frame{Type: "request", RequestID: id, Version: "1.0", OperationID: operationID, Capability: "terminal.v1", DeadlineMS: 30_000, Payload: payload}
}

func TestServeRequiresHelloBeforeMutation(t *testing.T) {
	var authorized, handled atomic.Int32
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		authorized.Add(1)
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		handled.Add(1)
		return operation.Outcome{}
	}, 1)
	client, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.Serve(peer) }()
	if err := protocol.WriteFrame(client, request("req_1", "op_00000001", json.RawMessage(`{"action":"list"}`))); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.ReadFrame(client)
	if err != nil || response.Type != "error" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	_ = client.Close()
	if err := <-done; !errors.Is(err, ErrHandshakeRequired) {
		t.Fatalf("serve err=%v", err)
	}
	if authorized.Load() != 0 || handled.Load() != 0 {
		t.Fatalf("authorized=%d handled=%d", authorized.Load(), handled.Load())
	}
}

func TestErrorEnvelopeUsesContractFieldNames(t *testing.T) {
	frame := errorFrame("req_1", "invalid_request", "invalid request", false)
	var payload map[string]any
	if err := json.Unmarshal(frame.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"code", "message", "requestId", "retryable"} {
		if _, ok := payload[field]; !ok {
			t.Fatalf("missing %s in %s", field, frame.Payload)
		}
	}
	if _, leaked := payload["RequestID"]; leaked {
		t.Fatalf("Go field name leaked in %s", frame.Payload)
	}
}

func TestReplayGapErrorCarriesOnlyBoundedSequenceDetails(t *testing.T) {
	outcome := domainResult(nil, &history.GapError{RequestedSequence: 100, EarliestSequence: 1024, LatestSequence: 2048})
	frame := errorFrameWithDetails("req_1", outcome.ErrorCode, "operation failed", false, outcome.Result)
	var envelope struct {
		Code    string            `json:"code"`
		Details map[string]uint64 `json:"details"`
	}
	if err := json.Unmarshal(frame.Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Code != "replay_gap" || envelope.Details["requested_sequence"] != 100 || envelope.Details["earliest_sequence"] != 1024 || envelope.Details["latest_sequence"] != 2048 || len(envelope.Details) != 3 {
		t.Fatalf("envelope=%s", frame.Payload)
	}
}

func TestStaleGenerationErrorCarriesCurrentGeneration(t *testing.T) {
	outcome := domainResult(nil, &session.StaleGenerationError{CurrentGeneration: 7})
	frame := errorFrameWithDetails("req_1", outcome.ErrorCode, "operation failed", false, outcome.Result)
	var envelope struct {
		Details map[string]uint64 `json:"details"`
	}
	if err := json.Unmarshal(frame.Payload, &envelope); err != nil || envelope.Details["current_generation"] != 7 || len(envelope.Details) != 1 {
		t.Fatalf("payload=%s err=%v", frame.Payload, err)
	}
}

type streamTestConnection struct {
	mu         sync.Mutex
	structured []protocol.Frame
	closeCode  int
}

func (*streamTestConnection) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *streamTestConnection) Write(data []byte) (int, error) { return len(data), nil }
func (*streamTestConnection) Close() error                     { return nil }
func (c *streamTestConnection) WriteStructured(frame protocol.Frame) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.structured = append(c.structured, frame)
	return nil
}
func (*streamTestConnection) WriteBinary(protocol.BinaryFrame) error { return nil }
func (c *streamTestConnection) CloseProtocol(code int, _ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closeCode = code
	return nil
}

type errorOutputStream struct{ err error }

func (s errorOutputStream) Next(context.Context) (protocol.BinaryFrame, error) {
	return protocol.BinaryFrame{}, s.err
}
func (errorOutputStream) Close() error { return nil }

func TestSlowConsumerStreamSendsBoundedErrorBefore4408Close(t *testing.T) {
	details, _ := json.Marshal(map[string]any{"queued_bytes": float64(1 << 20), "secret": "discard"})
	connection := &streamTestConnection{}
	server := &Server{}
	server.wg.Add(1)
	server.stream(context.Background(), &lockedWriter{writer: connection, connection: connection}, connection, errorOutputStream{&StreamError{Code: "slow_consumer", Details: details, CloseCode: protocol.CloseSlowConsumer}})
	if connection.closeCode != protocol.CloseSlowConsumer || len(connection.structured) != 1 {
		t.Fatalf("code=%d frames=%#v", connection.closeCode, connection.structured)
	}
	var envelope struct {
		Code    string         `json:"code"`
		Details map[string]any `json:"details"`
	}
	if err := json.Unmarshal(connection.structured[0].Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Code != "slow_consumer" || envelope.Details["queued_bytes"] != float64(1<<20) || len(envelope.Details) != 1 {
		t.Fatalf("payload=%s", connection.structured[0].Payload)
	}
}

func TestHelloRejectsDuplicateCapabilities(t *testing.T) {
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		return operation.Outcome{}
	}, 1)
	client, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- server.Serve(peer) }()
	payload := json.RawMessage(`{"min_version":"1.0","max_version":"1.0","capabilities":["terminal.v1","terminal.v1","health.v1"]}`)
	if err := protocol.WriteFrame(client, protocol.Frame{Type: "hello", RequestID: "req_hello", Version: "1.0", Payload: payload}); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.ReadFrame(client)
	if err != nil || response.Type != "error" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	_ = client.Close()
	if err := <-done; !errors.Is(err, ErrHandshakeRequired) {
		t.Fatalf("serve err=%v", err)
	}
}

func TestServeAuthorizesBeforeHandlerAndReplaysResult(t *testing.T) {
	var mu sync.Mutex
	var order []string
	var calls atomic.Int32
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		mu.Lock()
		order = append(order, "authorize")
		mu.Unlock()
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		mu.Lock()
		order = append(order, "handle")
		mu.Unlock()
		calls.Add(1)
		return operation.Outcome{Result: json.RawMessage(`{"sessions":[]}`)}
	}, 2)
	client, peer := net.Pipe()
	go server.Serve(peer)
	hello(t, client)
	for _, requestID := range []string{"req_1", "req_2"} {
		if err := protocol.WriteFrame(client, request(requestID, "op_00000001", json.RawMessage(`{"action":"list"}`))); err != nil {
			t.Fatal(err)
		}
		response, err := protocol.ReadFrame(client)
		if err != nil || response.Type != "response" {
			t.Fatalf("response=%#v err=%v", response, err)
		}
		var payload struct {
			Replay bool `json:"replay"`
		}
		if err := json.Unmarshal(response.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Replay != (requestID == "req_2") {
			t.Fatalf("request=%s replay=%v", requestID, payload.Replay)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler calls=%d", calls.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 || order[0] != "authorize" || order[1] != "handle" || order[2] != "authorize" {
		t.Fatalf("order=%v", order)
	}
}

func TestServerMetricsUseOnlyBoundedOperationLabels(t *testing.T) {
	metrics, err := observability.NewRegistry(observability.DefaultDescriptors())
	if err != nil {
		t.Fatal(err)
	}
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "principal"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		return operation.Outcome{Result: json.RawMessage(`{}`)}
	}, 2)
	server.config.Metrics = metrics
	client, peer := net.Pipe()
	go server.Serve(peer)
	hello(t, client)
	for _, requestID := range []string{"req_1", "req_2"} {
		if response := sendRequest(t, client, request(requestID, "op_00000001", json.RawMessage(`{"action":"list"}`))); response.Type != "response" {
			t.Fatalf("response=%#v", response)
		}
	}
	series := metrics.Snapshot()
	if len(series) != 2 {
		t.Fatalf("series=%#v", series)
	}
	for _, value := range series {
		if value.Labels["component"] != "session" || value.Labels["result"] != "ok" && value.Labels["result"] != "replayed" || value.Value != 1 {
			t.Fatalf("series=%#v", series)
		}
	}
}

func TestExplicitCancellationStopsOperation(t *testing.T) {
	started := make(chan struct{})
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(ctx context.Context, _ Authorization, _ string, _ json.RawMessage) operation.Outcome {
		close(started)
		<-ctx.Done()
		return operation.Outcome{ErrorCode: "operation_canceled"}
	}, 1)
	client, peer := net.Pipe()
	go server.Serve(peer)
	hello(t, client)
	if err := protocol.WriteFrame(client, request("req_1", "op_00000001", json.RawMessage(`{"action":"close"}`))); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := protocol.WriteFrame(client, protocol.Frame{Type: "cancel", RequestID: "req_cancel", Version: "1.0", OperationID: "op_00000001"}); err != nil {
		t.Fatal(err)
	}
	seenCancel, seenResult := false, false
	for !seenCancel || !seenResult {
		frame, err := protocol.ReadFrame(client)
		if err != nil {
			t.Fatal(err)
		}
		seenCancel = seenCancel || frame.RequestID == "req_cancel" && frame.Type == "response"
		seenResult = seenResult || frame.RequestID == "req_1" && frame.Type == "error"
	}
}

func TestCancellationCannotCrossAuthorizationBinding(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "principal-a"}, nil
	}, func(ctx context.Context, _ Authorization, _ string, _ json.RawMessage) operation.Outcome {
		close(started)
		<-ctx.Done()
		close(canceled)
		return operation.Outcome{ErrorCode: "operation_canceled"}
	}, 2)
	clientA, peerA := net.Pipe()
	go server.ServeAuthenticated(peerA, authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "principal-a"}, nil
	}))
	hello(t, clientA)
	if err := protocol.WriteFrame(clientA, request("req_a", "op_00000001", json.RawMessage(`{"action":"close"}`))); err != nil {
		t.Fatal(err)
	}
	<-started

	clientB, peerB := net.Pipe()
	go server.ServeAuthenticated(peerB, authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "principal-b"}, nil
	}))
	hello(t, clientB)
	if response := sendRequest(t, clientB, protocol.Frame{Type: "cancel", RequestID: "req_cancel_b", Version: "1.0", OperationID: "op_00000001"}); response.Type != "response" {
		t.Fatalf("cancel b=%#v", response)
	}
	select {
	case <-canceled:
		t.Fatal("different principal canceled operation")
	case <-time.After(20 * time.Millisecond):
	}
	if err := protocol.WriteFrame(clientA, protocol.Frame{Type: "cancel", RequestID: "req_cancel_a", Version: "1.0", OperationID: "op_00000001"}); err != nil {
		t.Fatal(err)
	}
	for {
		response, err := protocol.ReadFrame(clientA)
		if err != nil {
			t.Fatal(err)
		}
		if response.RequestID == "req_cancel_a" {
			if response.Type != "response" {
				t.Fatalf("cancel a=%#v", response)
			}
			break
		}
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("own principal could not cancel operation")
	}
}

func TestDisconnectDoesNotCancelDurableOperation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	completed := make(chan struct{})
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(ctx context.Context, _ Authorization, _ string, _ json.RawMessage) operation.Outcome {
		close(started)
		select {
		case <-ctx.Done():
			t.Errorf("operation canceled on disconnect: %v", ctx.Err())
		case <-release:
		}
		close(completed)
		return operation.Outcome{Result: json.RawMessage(`{"closed":true}`)}
	}, 1)
	client, peer := net.Pipe()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(peer) }()
	hello(t, client)
	if err := protocol.WriteFrame(client, request("req_1", "op_00000001", json.RawMessage(`{"action":"close"}`))); err != nil {
		t.Fatal(err)
	}
	<-started
	_ = client.Close()
	<-serveDone
	close(release)
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("operation did not complete after disconnect")
	}
}

func TestConcurrencyLimitRejectsBeforeSecondHandler(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	server := testServer(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{JournalBinding: "user:1"}, nil
	}, func(context.Context, Authorization, string, json.RawMessage) operation.Outcome {
		calls.Add(1)
		close(started)
		<-release
		return operation.Outcome{}
	}, 1)
	client, peer := net.Pipe()
	go server.Serve(peer)
	hello(t, client)
	if err := protocol.WriteFrame(client, request("req_1", "op_00000001", json.RawMessage(`{"action":"one"}`))); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := protocol.WriteFrame(client, request("req_2", "op_00000002", json.RawMessage(`{"action":"two"}`))); err != nil {
		t.Fatal(err)
	}
	response, err := protocol.ReadFrame(client)
	if err != nil || response.RequestID != "req_2" || response.Type != "error" {
		t.Fatalf("response=%#v err=%v", response, err)
	}
	close(release)
	if calls.Load() != 1 {
		t.Fatalf("handler calls=%d", calls.Load())
	}
}
