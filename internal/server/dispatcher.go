package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/history"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/process"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
)

type HealthSource interface{ Snapshot() health.Snapshot }
type SessionLauncher interface {
	Launch(context.Context, process.LaunchRequest) (session.Snapshot, error)
}

type DispatcherConfig struct {
	Sessions       *session.Manager
	Previews       *preview.Registry
	PreviewControl preview.PreviewControl
	ConfigApply    configapply.Handler
	Updates        interface {
		Activate(context.Context, bootstrap.ArtifactManifest, bootstrap.ArtifactManifest) (string, error)
	}
	Health          HealthSource
	SessionLauncher SessionLauncher
	WorkspaceRoot   string
	Random          io.Reader
	Now             func() time.Time
	Writers         *filetransfer.WriterRegistry
}

type Dispatcher struct{ config DispatcherConfig }

type terminalOutputStream struct {
	manager      *session.Manager
	sessionID    string
	attachmentID string
	writers      *filetransfer.WriterRegistry
}

const terminalExitObservationInterval = 100 * time.Millisecond

func NewDispatcher(config DispatcherConfig) (*Dispatcher, error) {
	if config.Sessions == nil || config.Health == nil || config.SessionLauncher == nil || !filepath.IsAbs(config.WorkspaceRoot) || config.Random == nil {
		return nil, ErrInvalidConfiguration
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	return &Dispatcher{config: config}, nil
}

func (d *Dispatcher) Capabilities() []string {
	capabilities := []string{"terminal.v2", "health.v1"}
	if d.config.Previews != nil {
		capabilities = append(capabilities, "preview.public.v1")
	}
	if d.config.ConfigApply != nil {
		capabilities = append(capabilities, "config.apply.v1")
	}
	if d.config.Updates != nil {
		capabilities = append(capabilities, "update.signed.v1")
	}
	return capabilities
}

func (d *Dispatcher) Handle(ctx context.Context, authorization Authorization, capability string, payload json.RawMessage) operation.Outcome {
	switch capability {
	case "terminal.v2":
		return d.terminal(ctx, authorization, payload)
	case "preview.public.v1":
		return d.preview(ctx, authorization, payload)
	case "health.v1":
		return result(d.config.Health.Snapshot())
	case "config.apply.v1":
		return d.configApply(ctx, authorization, payload)
	case "update.signed.v1":
		return d.update(ctx, payload)
	default:
		return failure("capability_required")
	}
}

type signedUpdateRequest struct {
	WorkerArtifact      bootstrap.ArtifactManifest `json:"worker_artifact"`
	HostServiceArtifact bootstrap.ArtifactManifest `json:"host_service_artifact"`
}

func (d *Dispatcher) update(ctx context.Context, payload json.RawMessage) operation.Outcome {
	if d.config.Updates == nil {
		return failure("capability_required")
	}
	var request signedUpdateRequest
	if decodeStrict(payload, &request) != nil {
		return failure("invalid_request")
	}
	version, err := d.config.Updates.Activate(ctx, request.WorkerArtifact, request.HostServiceArtifact)
	if err != nil {
		return failure("update_activation_failed")
	}
	return result(struct {
		Version string `json:"version"`
	}{version})
}

type configApplyRequest struct {
	Action           string `json:"action"`
	AssignmentID     string `json:"assignment_id"`
	ExpectedRevision string `json:"expected_revision"`
	ObservedRevision string `json:"observed_revision"`
}

func (d *Dispatcher) configApply(ctx context.Context, authorization Authorization, payload json.RawMessage) operation.Outcome {
	if d.config.ConfigApply == nil {
		return failure("capability_required")
	}
	var request configApplyRequest
	if decodeStrict(payload, &request) != nil || authorization.ResourceID == "" || request.AssignmentID != authorization.ResourceID {
		return failure("not_found_or_forbidden")
	}
	value, err := d.config.ConfigApply.Handle(ctx, configapply.Request{
		Action: request.Action, AssignmentID: request.AssignmentID,
		ExpectedRevision: request.ExpectedRevision, ObservedRevision: request.ObservedRevision,
	})
	return domainResult(value, err)
}

type attachmentControl struct {
	SessionID    string `json:"session_id"`
	AttachmentID string `json:"attachment_id"`
	NextSequence uint64 `json:"next_sequence,omitempty"`
}

func (d *Dispatcher) HandleTerminalInput(_ context.Context, authorization Authorization, sessionID, attachmentID string, generation uint64, data []byte) error {
	if authorization.ClientID == "" || sessionID == "" || attachmentID == "" || generation == 0 || (authorization.SessionID != "" && authorization.SessionID != sessionID) {
		return session.ErrInvalidInput
	}
	err := d.config.Sessions.WriteStream(sessionID, attachmentID, generation, data)
	if err == nil && d.config.Writers != nil {
		d.config.Writers.Input(sessionID, attachmentID, authorization.ClientID, d.config.Now())
	}
	return err
}

func (d *Dispatcher) HandleTerminalACK(_ context.Context, authorization Authorization, sessionID, attachmentID string, nextSequence uint64) error {
	if authorization.ClientID == "" || sessionID == "" || attachmentID == "" || (authorization.SessionID != "" && authorization.SessionID != sessionID) {
		return session.ErrInvalidInput
	}
	return d.config.Sessions.Acknowledge(sessionID, attachmentID, nextSequence)
}

func (d *Dispatcher) HandleTerminalResize(_ context.Context, authorization Authorization, sessionID, attachmentID string, columns, rows uint16) error {
	if authorization.ClientID == "" || sessionID == "" || attachmentID == "" || columns == 0 || rows == 0 || (authorization.SessionID != "" && authorization.SessionID != sessionID) {
		return session.ErrInvalidInput
	}
	return d.config.Sessions.Resize(sessionID, attachmentID, pty.Dimensions{Columns: columns, Rows: rows}, d.config.Now())
}

func (d *Dispatcher) HandleControl(_ context.Context, authorization Authorization, frame protocol.Frame) operation.Outcome {
	var control attachmentControl
	if decodeStrict(frame.Payload, &control) != nil || control.SessionID == "" || control.AttachmentID == "" || authorization.ClientID == "" {
		return failure("invalid_request")
	}
	if authorization.SessionID != "" && authorization.SessionID != control.SessionID {
		return failure("not_found_or_forbidden")
	}
	var err error
	switch frame.Type {
	case "ack":
		err = d.config.Sessions.Acknowledge(control.SessionID, control.AttachmentID, control.NextSequence)
	case "detach":
		err = d.config.Sessions.Detach(control.SessionID, control.AttachmentID)
		if err == nil && d.config.Writers != nil {
			d.config.Writers.Detach(control.SessionID, control.AttachmentID)
		}
	default:
		return failure("invalid_request")
	}
	return domainResult(struct{}{}, err)
}

func (d *Dispatcher) OpenStream(_ context.Context, authorization Authorization, capability string, payload json.RawMessage, outcome operation.Outcome, replay bool) (OutputStream, bool, error) {
	if capability != "terminal.v2" || outcome.ErrorCode != "" {
		return nil, false, nil
	}
	var request terminalRequest
	if decodeStrict(payload, &request) != nil || request.Action != "attach" {
		return nil, false, nil
	}
	var response struct {
		AttachmentID string `json:"attachment_id"`
	}
	if json.Unmarshal(outcome.Result, &response) != nil || response.AttachmentID == "" {
		return nil, false, ErrInvalidConfiguration
	}
	if replay {
		var err error
		if request.AtLiveBoundary {
			_, err = d.config.Sessions.AttachLive(request.SessionID, response.AttachmentID)
		} else {
			_, err = d.config.Sessions.Attach(request.SessionID, response.AttachmentID, request.FromSequence)
		}
		if err != nil {
			return nil, false, err
		}
	}
	if d.config.Writers != nil {
		d.config.Writers.Attach(request.SessionID, response.AttachmentID, authorization.ClientID)
	}
	return &terminalOutputStream{manager: d.config.Sessions, sessionID: request.SessionID, attachmentID: response.AttachmentID, writers: d.config.Writers}, true, nil
}

func (s *terminalOutputStream) Next(ctx context.Context) (protocol.BinaryFrame, error) {
	for {
		waitCtx, cancel := context.WithTimeout(ctx, terminalExitObservationInterval)
		event, err := s.manager.WaitNext(waitCtx, s.sessionID, s.attachmentID)
		cancel()
		if err == nil {
			return protocol.BinaryFrame{Channel: event.Channel, StartSequence: event.StartSequence, Data: event.Data, Release: event.Release}, nil
		}
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			snapshot, snapshotErr := s.manager.Snapshot(s.sessionID)
			if snapshotErr != nil {
				return protocol.BinaryFrame{}, snapshotErr
			}
			if snapshot.State != session.Exited && snapshot.State != session.Closed {
				continue
			}
			payload, marshalErr := json.Marshal(struct {
				Event         string          `json:"event"`
				SessionID     string          `json:"session_id"`
				State         session.State   `json:"state"`
				FinalSequence uint64          `json:"final_sequence"`
				Exit          *pty.ExitResult `json:"exit,omitempty"`
			}{"terminal_stream_end", snapshot.ID, snapshot.State, snapshot.LatestSequence, snapshot.Exit})
			if marshalErr != nil {
				return protocol.BinaryFrame{}, marshalErr
			}
			return protocol.BinaryFrame{}, &StreamEnd{Payload: payload}
		}
		if errors.Is(err, session.ErrAttachmentEvicted) {
			_, queued, _ := s.manager.AttachmentStatus(s.sessionID, s.attachmentID)
			details, _ := json.Marshal(struct {
				QueuedBytes uint64 `json:"queued_bytes"`
			}{queued})
			return protocol.BinaryFrame{}, &StreamError{Code: "slow_consumer", Details: details, CloseCode: protocol.CloseSlowConsumer}
		}
		if errors.Is(err, session.ErrInvalidTransition) || errors.Is(err, session.ErrAttachmentUnknown) {
			return protocol.BinaryFrame{}, ErrStreamClosed
		}
		return protocol.BinaryFrame{}, err
	}
}

func (s *terminalOutputStream) Close() error {
	err := s.manager.Detach(s.sessionID, s.attachmentID)
	if s.writers != nil {
		s.writers.Detach(s.sessionID, s.attachmentID)
	}
	return err
}

type terminalRequest struct {
	Action         string            `json:"action"`
	OperationID    string            `json:"operation_id,omitempty"`
	Name           string            `json:"name,omitempty"`
	CWD            string            `json:"cwd,omitempty"`
	SessionID      string            `json:"session_id,omitempty"`
	AttachmentID   string            `json:"attachment_id,omitempty"`
	FromSequence   uint64            `json:"from_sequence,omitempty"`
	AtLiveBoundary bool              `json:"at_live_boundary,omitempty"`
	Generation     uint64            `json:"generation,omitempty"`
	Columns        uint16            `json:"columns,omitempty"`
	Rows           uint16            `json:"rows,omitempty"`
	Environment    map[string]string `json:"environment,omitempty"`
	Signal         string            `json:"signal,omitempty"`
}

type terminalAttachResponse struct {
	StreamID     uint32 `json:"stream_id,omitempty"`
	AttachmentID string `json:"attachment_id"`
	Session      struct {
		Snapshot session.Snapshot `json:"snapshot"`
		Replay   struct {
			FromSequence     uint64 `json:"from_sequence"`
			ToSequence       uint64 `json:"to_sequence"`
			EarliestSequence uint64 `json:"earliest_sequence"`
			LatestSequence   uint64 `json:"latest_sequence"`
		} `json:"replay"`
	} `json:"session"`
}

func newTerminalAttachResponse(attachmentID string, attached session.AttachResult) terminalAttachResponse {
	response := terminalAttachResponse{AttachmentID: attachmentID}
	response.Session.Snapshot = attached.Snapshot
	response.Session.Replay.FromSequence = attached.Replay.FromSequence
	response.Session.Replay.ToSequence = attached.Replay.ToSequence
	response.Session.Replay.EarliestSequence = attached.Replay.EarliestSequence
	response.Session.Replay.LatestSequence = attached.Replay.LatestSequence
	return response
}

func (d *Dispatcher) terminal(ctx context.Context, authorization Authorization, payload json.RawMessage) operation.Outcome {
	var request terminalRequest
	if decodeStrict(payload, &request) != nil || authorization.ClientID == "" {
		return failure("invalid_request")
	}
	if authorization.SessionID != "" && request.SessionID != "" && authorization.SessionID != request.SessionID {
		return failure("not_found_or_forbidden")
	}
	switch request.Action {
	case "list":
		if authorization.SessionID != "" {
			value, err := d.config.Sessions.Snapshot(authorization.SessionID)
			if err != nil {
				return domainResult(nil, err)
			}
			return result(struct {
				Sessions []session.Snapshot `json:"sessions"`
			}{[]session.Snapshot{value}})
		}
		return result(struct {
			Sessions []session.Snapshot `json:"sessions"`
		}{d.config.Sessions.List()})
	case "get", "snapshot":
		value, err := d.config.Sessions.Snapshot(request.SessionID)
		return domainResult(value, err)
	case "create":
		cwd, ok := d.cwd(request.CWD)
		if request.SessionID == "" {
			request.SessionID = authorization.SessionID
		}
		if !ok || request.Columns == 0 || request.Rows == 0 {
			return failure("invalid_request")
		}
		value, err := d.config.SessionLauncher.Launch(ctx, process.LaunchRequest{ID: request.SessionID, Name: request.Name, CWD: cwd, Dimensions: pty.Dimensions{Columns: request.Columns, Rows: request.Rows}, Environment: request.Environment})
		return domainResult(value, err)
	case "attach", "replay":
		attachmentID := request.AttachmentID
		if attachmentID == "" {
			attachmentID = d.randomID("att_")
		}
		if attachmentID == "" {
			return failure("unavailable")
		}
		var value session.AttachResult
		var err error
		if request.Action == "attach" && request.AtLiveBoundary {
			value, err = d.config.Sessions.AttachLive(request.SessionID, attachmentID)
		} else {
			value, err = d.config.Sessions.Attach(request.SessionID, attachmentID, request.FromSequence)
		}
		if err != nil {
			return domainResult(nil, err)
		}
		// Replay bytes travel as binary frames after this control response. Keeping
		// them out of JSON prevents a terminal-sized replay from overflowing the
		// structured-frame limit and avoids delivering the same output twice.
		return result(newTerminalAttachResponse(attachmentID, value))
	case "detach":
		err := d.config.Sessions.Detach(request.SessionID, request.AttachmentID)
		if err == nil && d.config.Writers != nil {
			d.config.Writers.Detach(request.SessionID, request.AttachmentID)
		}
		return domainResult(struct{}{}, err)
	case "resize":
		err := d.config.Sessions.Resize(request.SessionID, request.AttachmentID, pty.Dimensions{Columns: request.Columns, Rows: request.Rows}, d.config.Now())
		return domainResult(struct{}{}, err)
	case "signal":
		err := d.config.Sessions.Signal(request.SessionID, request.Generation, pty.Signal(request.Signal))
		return domainResult(struct{}{}, err)
	case "clear":
		sequence, err := d.config.Sessions.Clear(request.SessionID)
		return domainResult(struct {
			Sequence uint64 `json:"sequence"`
		}{sequence}, err)
	case "restart":
		value, err := d.config.Sessions.Restart(request.SessionID)
		return domainResult(value, err)
	case "close":
		value, err := d.config.Sessions.Close(ctx, request.SessionID)
		return domainResult(value, err)
	case "delete":
		return domainResult(struct{}{}, d.config.Sessions.Delete(request.SessionID))
	default:
		return failure("invalid_request")
	}
}

type previewRequest struct {
	Action                string `json:"action"`
	LogicalName           string `json:"logical_name,omitempty"`
	TargetHost            string `json:"target_host,omitempty"`
	TargetPort            uint16 `json:"target_port,omitempty"`
	PublicAcknowledgement bool   `json:"public_acknowledgement,omitempty"`
	Access                string `json:"access,omitempty"`
	Identity              string `json:"identity,omitempty"`
}

func (d *Dispatcher) preview(ctx context.Context, authorization Authorization, payload json.RawMessage) operation.Outcome {
	if d.config.Previews == nil || authorization.EnvironmentID == "" {
		return failure("capability_required")
	}
	var request previewRequest
	if decodeStrict(payload, &request) != nil || request.Access != "" {
		return failure("unsupported_preview_policy")
	}
	identity := authorization.ResourceID
	if request.Identity != "" && (d.config.PreviewControl != nil || request.Identity != identity) {
		return failure("not_found_or_forbidden")
	}
	switch request.Action {
	case "list":
		if d.config.PreviewControl != nil {
			return domainResult(d.config.PreviewControl.List(ctx))
		}
		return domainResult(d.config.Previews.ListEnvironment(authorization.EnvironmentID), nil)
	case "register":
		target := preview.Target{Host: request.TargetHost, Port: request.TargetPort}
		if d.config.PreviewControl != nil {
			remote, err := d.config.PreviewControl.Register(ctx, request.LogicalName, target, request.PublicAcknowledgement, 0, false)
			if err != nil {
				return failure("preview_control_unavailable")
			}
			value, err := d.config.Previews.RegisterCanonical(remote.PreviewKey, remote.URL, authorization.EnvironmentID, remote.LogicalName, target)
			return domainResult(value, err)
		}
		value, err := d.config.Previews.Register(identity, authorization.EnvironmentID, request.LogicalName, target, request.PublicAcknowledgement)
		return domainResult(value, err)
	case "get":
		if identity == "" {
			return failure("not_found_or_forbidden")
		}
		value, err := d.config.Previews.Get(identity)
		return domainResult(value, err)
	case "probe":
		if identity == "" {
			return failure("not_found_or_forbidden")
		}
		value, err := d.config.Previews.Probe(ctx, identity)
		return domainResult(value, err)
	case "remove":
		if d.config.PreviewControl != nil {
			remote, err := d.config.PreviewControl.Remove(ctx, request.LogicalName)
			if err != nil {
				return failure("preview_control_unavailable")
			}
			return domainResult(d.config.Previews.Remove(remote.PreviewKey))
		}
		if identity == "" {
			return failure("not_found_or_forbidden")
		}
		value, err := d.config.Previews.Remove(identity)
		return domainResult(value, err)
	default:
		return failure("invalid_request")
	}
}

func (d *Dispatcher) cwd(value string) (string, bool) {
	if value == "" {
		value = d.config.WorkspaceRoot
	}
	if !filepath.IsAbs(value) {
		value = filepath.Join(d.config.WorkspaceRoot, value)
	}
	clean := filepath.Clean(value)
	relative, err := filepath.Rel(d.config.WorkspaceRoot, clean)
	return clean, err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func (d *Dispatcher) randomID(prefix string) string {
	var data [16]byte
	if _, err := io.ReadFull(d.config.Random, data[:]); err != nil {
		return ""
	}
	return prefix + hex.EncodeToString(data[:])
}

func decodeStrict(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
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

func result(value any) operation.Outcome {
	encoded, err := json.Marshal(value)
	if err != nil {
		return failure("unavailable")
	}
	return operation.Outcome{Result: encoded}
}

func failure(code string) operation.Outcome { return operation.Outcome{ErrorCode: code} }

func failureDetails(code string, details any) operation.Outcome {
	encoded, err := json.Marshal(details)
	if err != nil {
		return failure("unavailable")
	}
	return operation.Outcome{ErrorCode: code, Result: encoded}
}

func domainResult(value any, err error) operation.Outcome {
	if err == nil {
		return result(value)
	}
	var gap *history.GapError
	if errors.As(err, &gap) {
		return failureDetails("replay_gap", struct {
			RequestedSequence uint64 `json:"requested_sequence"`
			EarliestSequence  uint64 `json:"earliest_sequence"`
			LatestSequence    uint64 `json:"latest_sequence"`
		}{gap.RequestedSequence, gap.EarliestSequence, gap.LatestSequence})
	}
	var stale *session.StaleGenerationError
	if errors.As(err, &stale) {
		return failureDetails("stale_generation", struct {
			CurrentGeneration uint64 `json:"current_generation"`
		}{stale.CurrentGeneration})
	}
	switch {
	case errors.Is(err, context.Canceled):
		return failure("operation_canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return failure("deadline_exceeded")
	case errors.Is(err, session.ErrSessionUnknown), errors.Is(err, preview.ErrNotFound):
		return failure("not_found_or_forbidden")
	case errors.Is(err, session.ErrSessionRunning):
		return failure("session_running")
	case errors.Is(err, session.ErrStaleGeneration):
		return failure("stale_generation")
	case errors.Is(err, session.ErrInputConflict):
		return failure("input_id_conflict")
	case errors.Is(err, session.ErrInputUnknown):
		return failure("input_unknown")
	case errors.Is(err, preview.ErrResourceLimit):
		return failure("resource_limit")
	case errors.Is(err, session.ErrResourceLimit), errors.Is(err, session.ErrInputJournalFull):
		return failure("resource_limit")
	case errors.Is(err, preview.ErrInvalidTarget):
		return failure("invalid_preview_target")
	case errors.Is(err, configapply.ErrRevisionConflict):
		return failure("config_revision_conflict")
	case errors.Is(err, configapply.ErrInvalidRequest):
		return failure("invalid_request")
	default:
		return failure("invalid_request")
	}
}
