package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	pberrors "github.com/pinksaucepasta/paperboat-helper/internal/errors"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
)

var (
	ErrInvalidConfiguration  = errors.New("invalid server configuration")
	ErrHandshakeRequired     = errors.New("hello required before requests")
	ErrCapabilityUnavailable = errors.New("capability unavailable")
	ErrServerStopped         = errors.New("server stopped")
	ErrStreamClosed          = errors.New("output stream closed")
)

type Connection interface {
	io.Reader
	io.Writer
	io.Closer
}

type FramedConnection interface {
	Connection
	WriteStructured(protocol.Frame) error
	WriteBinary(protocol.BinaryFrame) error
}

type ProtocolCloser interface {
	CloseProtocol(int, string) error
}

// Authorizer must validate the operation credential and all applicable bindings.
// It runs before Handler, including for requests that name nonexistent resources.
type Authorizer interface {
	Authorize(context.Context, protocol.Frame) (Authorization, error)
}

type Authorization struct {
	// JournalBinding is a stable, non-secret identity and resource binding. It is
	// included in idempotency hashing so operation IDs cannot cross principals.
	JournalBinding string
	EnvironmentID  string
	UserID         string
	ClientID       string
	SessionID      string
	ResourceID     string
	ExpiresAt      time.Time
	Value          any
}

type Handler interface {
	Handle(context.Context, Authorization, string, json.RawMessage) operation.Outcome
}

type OutputStream interface {
	Next(context.Context) (protocol.BinaryFrame, error)
	Close() error
}

type StreamError struct {
	Code      string
	Details   json.RawMessage
	CloseCode int
}

func (e *StreamError) Error() string { return e.Code }

type StreamEnd struct {
	Payload json.RawMessage
}

func (e *StreamEnd) Error() string { return "output stream ended" }

type StreamHandler interface {
	OpenStream(context.Context, Authorization, string, json.RawMessage, operation.Outcome, bool) (OutputStream, bool, error)
}

type ControlHandler interface {
	HandleControl(context.Context, Authorization, protocol.Frame) operation.Outcome
}

type InputHandler interface {
	HandleInput(context.Context, Authorization, json.RawMessage) error
}

type Config struct {
	Negotiator        protocol.Negotiator
	Journal           *operation.Journal
	Authorizer        Authorizer
	Handler           Handler
	MaxConcurrent     int
	HeartbeatInterval time.Duration
	PeerTimeout       time.Duration
	MutationDeadline  time.Duration
	Metrics           MetricRecorder
}

type MetricRecorder interface {
	Record(string, float64, map[string]string) error
}

type Server struct {
	config Config
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}

	mu      sync.Mutex
	stopped bool
	running map[string]context.CancelFunc
	conns   map[Connection]struct{}
	wg      sync.WaitGroup
}

func New(config Config) (*Server, error) {
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = helperconfig.DefaultResources.MaxConcurrentOps
	}
	if config.Journal == nil || config.Handler == nil || config.MaxConcurrent < 1 ||
		config.HeartbeatInterval <= 0 || config.PeerTimeout < config.HeartbeatInterval ||
		config.MutationDeadline <= 0 || config.MutationDeadline > 5*time.Minute {
		return nil, ErrInvalidConfiguration
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{config: config, ctx: ctx, cancel: cancel, sem: make(chan struct{}, config.MaxConcurrent), running: make(map[string]context.CancelFunc), conns: make(map[Connection]struct{})}, nil
}

func (s *Server) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return ErrServerStopped
	}
	return nil
}

func (s *Server) Serve(conn Connection) error {
	return s.ServeAuthenticated(conn, s.config.Authorizer)
}

// ServeAuthenticated binds one already-established transport credential to one
// connection. HTTP/WSS adapters should use this entry point rather than sharing
// credential state between connections.
func (s *Server) ServeAuthenticated(conn Connection, authorizer Authorizer) (serveErr error) {
	if conn == nil {
		return ErrInvalidConfiguration
	}
	if authorizer == nil {
		_ = conn.Close()
		return ErrInvalidConfiguration
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		_ = conn.Close()
		return ErrServerStopped
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		if closer, ok := conn.(ProtocolCloser); ok {
			_ = closer.CloseProtocol(connectionCloseCode(serveErr), closeReason(serveErr))
		} else {
			_ = conn.Close()
		}
	}()

	writer := &lockedWriter{writer: conn, connection: conn}
	connectionCtx, cancelConnection := context.WithCancel(s.ctx)
	defer cancelConnection()
	first, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	if first.Type != "hello" {
		_ = writer.write(errorFrame(first.RequestID, "protocol_incompatible", "hello required", false))
		return ErrHandshakeRequired
	}
	var hello helloPayload
	if err := decodePayload(first.Payload, &hello); err != nil {
		_ = writer.write(errorFrame(first.RequestID, "invalid_request", "invalid hello", false))
		return err
	}
	if err := hello.validate(); err != nil {
		_ = writer.write(errorFrame(first.RequestID, "invalid_request", "invalid hello", false))
		return err
	}
	welcome, err := s.config.Negotiator.Negotiate(hello.MinVersion, hello.MaxVersion, hello.Capabilities)
	if err != nil {
		_ = writer.write(errorFrame(first.RequestID, protocolCode(err), "protocol negotiation failed", false))
		return err
	}
	payload, _ := json.Marshal(welcomePayload{Version: welcome.Version, Capabilities: welcome.Capabilities})
	if err := writer.write(protocol.Frame{Type: "welcome", RequestID: first.RequestID, Version: protocol.ProtocolVersion, Payload: payload}); err != nil {
		return err
	}
	selected := make(map[string]bool, len(welcome.Capabilities))
	for _, capability := range welcome.Capabilities {
		selected[capability] = true
	}
	var lastInputSequence uint64

	frames := make(chan readResult, 1)
	readerDone := make(chan struct{})
	defer close(readerDone)
	go readFrames(conn, frames, readerDone)
	heartbeat := time.NewTicker(s.config.HeartbeatInterval)
	defer heartbeat.Stop()
	timeout := time.NewTimer(s.config.PeerTimeout)
	defer timeout.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return ErrServerStopped
		case <-timeout.C:
			return context.DeadlineExceeded
		case <-heartbeat.C:
			requestID := "heartbeat"
			if err := writer.write(protocol.Frame{Type: "heartbeat", RequestID: requestID, Version: protocol.ProtocolVersion}); err != nil {
				return err
			}
		case result := <-frames:
			if result.err != nil {
				return result.err
			}
			resetTimer(timeout, s.config.PeerTimeout)
			switch result.frame.Type {
			case "heartbeat":
				continue
			case "ack", "detach":
				s.handleControl(result.frame, writer, authorizer)
			case "input":
				if !selected["terminal.v1"] {
					return ErrCapabilityUnavailable
				}
				var input terminalStreamInput
				if decodePayload(result.frame.Payload, &input) != nil || input.Sequence != lastInputSequence+1 {
					return &protocol.Error{Code: protocol.InvalidFrame, Cause: errors.New("terminal input sequence is invalid")}
				}
				if err := s.handleInput(result.frame, authorizer); err != nil {
					return err
				}
				lastInputSequence = input.Sequence
			case "cancel":
				cancelCtx, cancel := context.WithTimeout(s.ctx, min(s.config.MutationDeadline, 5*time.Second))
				authorization, authErr := authorizer.Authorize(cancelCtx, result.frame)
				cancel()
				if authErr != nil || authorization.JournalBinding == "" {
					_ = writer.write(errorFrame(result.frame.RequestID, "not_found_or_forbidden", "operation was not found or is not available", false))
					continue
				}
				s.cancelOperation(operationKey(authorization.JournalBinding, result.frame.OperationID))
				_ = writer.write(protocol.Frame{Type: "response", RequestID: result.frame.RequestID, Version: protocol.ProtocolVersion, OperationID: result.frame.OperationID, Payload: json.RawMessage(`{"canceled":true}`)})
			case "request":
				if !selected[result.frame.Capability] {
					_ = writer.write(errorFrame(result.frame.RequestID, "capability_required", "capability was not negotiated", false))
					continue
				}
				s.startRequest(result.frame, writer, authorizer, connectionCtx, conn)
			default:
				_ = writer.write(errorFrame(result.frame.RequestID, "invalid_request", "unexpected frame", false))
			}
		}
	}
}

func (s *Server) handleInput(frame protocol.Frame, authorizer Authorizer) error {
	handler, ok := s.config.Handler.(InputHandler)
	if !ok {
		return ErrCapabilityUnavailable
	}
	ctx, cancel := context.WithTimeout(s.ctx, min(s.config.MutationDeadline, 5*time.Second))
	defer cancel()
	authorization, err := authorizer.Authorize(ctx, frame)
	if err != nil || authorization.ClientID == "" {
		return ErrCapabilityUnavailable
	}
	return handler.HandleInput(ctx, authorization, frame.Payload)
}

func (s *Server) handleControl(frame protocol.Frame, writer *lockedWriter, authorizer Authorizer) {
	handler, ok := s.config.Handler.(ControlHandler)
	if !ok {
		_ = writer.write(errorFrame(frame.RequestID, "invalid_request", "unsupported control frame", false))
		return
	}
	ctx, cancel := context.WithTimeout(s.ctx, min(s.config.MutationDeadline, 5*time.Second))
	defer cancel()
	authorization, err := authorizer.Authorize(ctx, frame)
	if err != nil || authorization.JournalBinding == "" {
		_ = writer.write(errorFrame(frame.RequestID, "not_found_or_forbidden", "resource was not found or is not available", false))
		return
	}
	outcome := handler.HandleControl(ctx, authorization, frame)
	if outcome.ErrorCode != "" {
		_ = writer.write(errorFrame(frame.RequestID, outcome.ErrorCode, "attachment control failed", false))
		return
	}
	_ = writer.write(protocol.Frame{Type: "response", RequestID: frame.RequestID, Version: protocol.ProtocolVersion, Payload: outcome.Result})
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.cancel()
		for conn := range s.conns {
			_ = conn.Close()
		}
		for _, cancel := range s.running {
			cancel()
		}
	}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) startRequest(frame protocol.Frame, writer *lockedWriter, authorizer Authorizer, connectionCtx context.Context, conn Connection) {
	select {
	case s.sem <- struct{}{}:
	default:
		s.recordOperation("protocol", "rejected")
		_ = writer.write(errorFrame(frame.RequestID, "resource_limit", "operation concurrency limit reached", true))
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-s.sem }()
		deadline := time.Duration(frame.DeadlineMS) * time.Millisecond
		if deadline > s.config.MutationDeadline {
			deadline = s.config.MutationDeadline
		}
		opCtx, cancel := context.WithTimeout(s.ctx, deadline)
		authorization, err := authorizer.Authorize(opCtx, frame)
		if err != nil || authorization.JournalBinding == "" {
			cancel()
			s.recordOperation("auth", "rejected")
			_ = writer.write(errorFrame(frame.RequestID, "not_found_or_forbidden", "resource was not found or is not available", false))
			return
		}
		activeKey := operationKey(authorization.JournalBinding, frame.OperationID)
		owner, ok := s.registerOperation(activeKey, cancel)
		if !ok {
			cancel()
			_ = writer.write(errorFrame(frame.RequestID, "unavailable", "server is stopping", true))
			return
		}
		defer func() {
			if owner {
				s.unregisterOperation(activeKey)
			}
			cancel()
		}()
		journalRequest, err := json.Marshal(struct {
			Binding    string          `json:"binding"`
			Capability string          `json:"capability"`
			Payload    json.RawMessage `json:"payload"`
		}{Binding: authorization.JournalBinding, Capability: frame.Capability, Payload: frame.Payload})
		if err != nil {
			_ = writer.write(errorFrame(frame.RequestID, "invalid_request", "invalid operation", false))
			return
		}
		outcome, replay, err := s.config.Journal.Execute(opCtx, frame.OperationID, journalRequest, func(ctx context.Context) operation.Outcome {
			return s.config.Handler.Handle(ctx, authorization, frame.Capability, frame.Payload)
		})
		if err != nil {
			code := operationErrorCode(err)
			s.recordOperation(componentForCapability(frame.Capability), metricResult(code, false))
			_ = writer.write(errorFrame(frame.RequestID, code, operationErrorMessage(code), errors.Is(err, context.DeadlineExceeded)))
			return
		}
		if outcome.ErrorCode != "" {
			s.recordOperation(componentForCapability(frame.Capability), metricResult(outcome.ErrorCode, false))
			_ = writer.write(errorFrameWithDetails(frame.RequestID, outcome.ErrorCode, "operation failed", false, outcome.Result))
			return
		}
		responsePayload, _ := json.Marshal(struct {
			Result json.RawMessage `json:"result"`
			Replay bool            `json:"replay"`
		}{Result: outcome.Result, Replay: replay})
		stream, hasStream, streamErr := s.openStream(connectionCtx, authorization, frame, outcome, replay)
		if streamErr != nil {
			_ = writer.write(errorFrame(frame.RequestID, "unavailable", "output stream could not be opened", true))
			return
		}
		s.recordOperation(componentForCapability(frame.Capability), metricResult("", replay))
		if err := writer.write(protocol.Frame{Type: "response", RequestID: frame.RequestID, Version: protocol.ProtocolVersion, OperationID: frame.OperationID, Payload: responsePayload}); err != nil {
			if hasStream {
				_ = stream.Close()
			}
			return
		}
		if hasStream {
			s.wg.Add(1)
			go s.stream(connectionCtx, writer, conn, stream)
		}
	}()
}

func (s *Server) openStream(ctx context.Context, authorization Authorization, frame protocol.Frame, outcome operation.Outcome, replay bool) (OutputStream, bool, error) {
	handler, ok := s.config.Handler.(StreamHandler)
	if !ok {
		return nil, false, nil
	}
	return handler.OpenStream(ctx, authorization, frame.Capability, frame.Payload, outcome, replay)
}

func (s *Server) stream(ctx context.Context, writer *lockedWriter, conn Connection, stream OutputStream) {
	defer s.wg.Done()
	defer stream.Close()
	for {
		frame, err := stream.Next(ctx)
		if err != nil {
			var streamEnd *StreamEnd
			if errors.As(err, &streamEnd) {
				_ = writer.write(protocol.Frame{Type: "event", RequestID: "stream", Version: protocol.ProtocolVersion, Capability: "terminal.v1", Payload: streamEnd.Payload})
				return
			}
			var streamError *StreamError
			if errors.As(err, &streamError) {
				_ = writer.write(errorFrameWithDetails("stream", streamError.Code, "output stream closed", false, streamError.Details))
				if closer, ok := conn.(ProtocolCloser); ok {
					_ = closer.CloseProtocol(streamError.CloseCode, streamError.Code)
				} else {
					_ = conn.Close()
				}
				return
			}
			if ctx.Err() == nil && !errors.Is(err, ErrStreamClosed) {
				_ = conn.Close()
			}
			return
		}
		if err := writer.writeBinary(frame); err != nil {
			_ = conn.Close()
			return
		}
	}
}

func (s *Server) registerOperation(id string, cancel context.CancelFunc) (owner, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return false, false
	}
	if s.running[id] != nil {
		return false, true
	}
	s.running[id] = cancel
	return true, true
}

func (s *Server) unregisterOperation(id string) {
	s.mu.Lock()
	delete(s.running, id)
	s.mu.Unlock()
}

func (s *Server) cancelOperation(id string) {
	s.mu.Lock()
	cancel := s.running[id]
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func operationKey(binding, operationID string) string { return binding + "\x00" + operationID }

type helloPayload struct {
	MinVersion   string   `json:"min_version"`
	MaxVersion   string   `json:"max_version"`
	Capabilities []string `json:"capabilities"`
}

func (p helloPayload) validate() error {
	if p.MinVersion == "" || p.MaxVersion == "" || len(p.Capabilities) > 64 {
		return ErrHandshakeRequired
	}
	seen := make(map[string]bool, len(p.Capabilities))
	for _, capability := range p.Capabilities {
		if seen[capability] {
			return ErrHandshakeRequired
		}
		seen[capability] = true
	}
	return nil
}

type welcomePayload struct {
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities"`
}

type readResult struct {
	frame protocol.Frame
	err   error
}

func readFrames(reader io.Reader, results chan<- readResult, done <-chan struct{}) {
	for {
		frame, err := protocol.ReadFrame(reader)
		select {
		case results <- readResult{frame: frame, err: err}:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}

type lockedWriter struct {
	mu         sync.Mutex
	writer     io.Writer
	connection Connection
}

func (w *lockedWriter) write(frame protocol.Frame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if connection, ok := w.connection.(FramedConnection); ok {
		return connection.WriteStructured(frame)
	}
	return protocol.WriteFrame(w.writer, frame)
}

func (w *lockedWriter) writeBinary(frame protocol.BinaryFrame) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if connection, ok := w.connection.(FramedConnection); ok {
		return connection.WriteBinary(frame)
	}
	return protocol.WriteBinaryFrame(w.writer, frame)
}

func closeReason(err error) string {
	if err == nil || errors.Is(err, io.EOF) {
		return "closed"
	}
	var protocolError *protocol.Error
	if errors.As(err, &protocolError) {
		return string(protocolError.Code)
	}
	return "unavailable"
}

func connectionCloseCode(err error) int {
	if err == nil || errors.Is(err, io.EOF) {
		return protocol.CloseNormal
	}
	return protocol.CloseCode(err)
}

func decodePayload(payload []byte, target any) error {
	decoder := json.NewDecoder(newBytesReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

type byteReader struct{ data []byte }

func newBytesReader(data []byte) *byteReader { return &byteReader{data: data} }
func (r *byteReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func resetTimer(timer *time.Timer, duration time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(duration)
}

func errorFrame(requestID, code, message string, retryable bool) protocol.Frame {
	return errorFrameWithDetails(requestID, code, message, retryable, nil)
}

func errorFrameWithDetails(requestID, code, message string, retryable bool, details json.RawMessage) protocol.Frame {
	values := safeErrorDetails(code, details)
	payload, _ := json.Marshal(pberrors.Error{Code: pberrors.Code(code), Message: message, RequestID: requestID, Retryable: retryable, Details: values})
	return protocol.Frame{Type: "error", RequestID: requestID, Version: protocol.ProtocolVersion, Payload: payload}
}

func safeErrorDetails(code string, details json.RawMessage) map[string]any {
	if len(details) == 0 {
		return nil
	}
	if code == "slow_consumer" {
		var value struct {
			QueuedBytes uint64 `json:"queued_bytes"`
		}
		if json.Unmarshal(details, &value) != nil {
			return nil
		}
		return map[string]any{"queued_bytes": value.QueuedBytes}
	}
	if code == "stale_generation" {
		var value struct {
			CurrentGeneration uint64 `json:"current_generation"`
		}
		if json.Unmarshal(details, &value) != nil {
			return nil
		}
		return map[string]any{"current_generation": value.CurrentGeneration}
	}
	if code != "replay_gap" {
		return nil
	}
	var gap struct {
		RequestedSequence uint64 `json:"requested_sequence"`
		EarliestSequence  uint64 `json:"earliest_sequence"`
		LatestSequence    uint64 `json:"latest_sequence"`
	}
	if json.Unmarshal(details, &gap) != nil {
		return nil
	}
	return map[string]any{"requested_sequence": gap.RequestedSequence, "earliest_sequence": gap.EarliestSequence, "latest_sequence": gap.LatestSequence}
}

func protocolCode(err error) string {
	var protocolError *protocol.Error
	if errors.As(err, &protocolError) {
		return string(protocolError.Code)
	}
	return "unavailable"
}

func operationErrorCode(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "operation_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, operation.ErrOperationConflict):
		return "operation_id_conflict"
	case errors.Is(err, operation.ErrOperationUncertain):
		return "operation_uncertain"
	case errors.Is(err, operation.ErrJournalFull):
		return "resource_limit"
	default:
		return "unavailable"
	}
}

func operationErrorMessage(code string) string {
	if code == "operation_canceled" {
		return "operation canceled"
	}
	if code == "deadline_exceeded" {
		return "operation deadline exceeded"
	}
	return "operation could not be completed"
}

func (s *Server) recordOperation(component, result string) {
	if s.config.Metrics != nil {
		_ = s.config.Metrics.Record("paperboat_helper_operations_total", 1, map[string]string{"component": component, "result": result})
	}
}

func componentForCapability(capability string) string {
	switch capability {
	case "terminal.v1":
		return "session"
	case "upload.v1":
		return "upload"
	case "activity.v1":
		return "activity"
	case "preview.public.v1":
		return "preview"
	case "update.signed.v1":
		return "update"
	default:
		return "protocol"
	}
}

func metricResult(code string, replay bool) string {
	if replay {
		return "replayed"
	}
	switch code {
	case "":
		return "ok"
	case "operation_id_conflict", "input_id_conflict":
		return "conflict"
	case "operation_canceled":
		return "canceled"
	case "deadline_exceeded":
		return "deadline"
	case "unavailable", "operation_uncertain", "storage_unavailable", "resource_limit":
		return "unavailable"
	default:
		return "rejected"
	}
}
