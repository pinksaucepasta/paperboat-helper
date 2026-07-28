package preview

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const maxAgentRequestBytes = 8 << 10

type AgentHandlerConfig struct {
	Token         string
	EnvironmentID string
	Registry      *Registry
	Control       PreviewControl
	RoutesChanged func()
}

type AgentHandler struct{ config AgentHandlerConfig }

type agentRequest struct {
	Action                string `json:"action"`
	LogicalName           string `json:"logical_name,omitempty"`
	TargetPort            uint16 `json:"target_port,omitempty"`
	PublicAcknowledgement bool   `json:"public_acknowledgement,omitempty"`
	DurationSeconds       int64  `json:"duration_seconds,omitempty"`
	Indefinite            bool   `json:"indefinite,omitempty"`
}

func NewAgentHandler(config AgentHandlerConfig) (*AgentHandler, error) {
	if len(config.Token) < 32 || config.EnvironmentID == "" || config.Registry == nil || config.Control == nil {
		return nil, ErrControlClientInvalid
	}
	return &AgentHandler{config: config}, nil
}

func (h *AgentHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("Content-Type", "application/json")
	if !h.authorized(request) {
		writeAgentError(writer, http.StatusNotFound, "not_found")
		return
	}
	if request.Method != http.MethodPost || request.URL.RawQuery != "" {
		writeAgentError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxAgentRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var operation agentRequest
	if decoder.Decode(&operation) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		writeAgentError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	var value any
	var err error
	switch operation.Action {
	case "list":
		if operation.LogicalName != "" || operation.TargetPort != 0 || operation.PublicAcknowledgement {
			writeAgentError(writer, http.StatusBadRequest, "invalid_request")
			return
		}
		value, err = h.list(request)
	case "create":
		if operation.TargetPort == 0 || !operation.PublicAcknowledgement || operation.DurationSeconds < 0 || operation.Indefinite && operation.DurationSeconds != 0 {
			writeAgentError(writer, http.StatusBadRequest, "public_acknowledgement_required")
			return
		}
		value, err = h.create(request, operation)
	case "remove":
		if operation.LogicalName == "" || operation.TargetPort != 0 || operation.PublicAcknowledgement {
			writeAgentError(writer, http.StatusBadRequest, "invalid_request")
			return
		}
		value, err = h.remove(request, operation.LogicalName)
	default:
		writeAgentError(writer, http.StatusBadRequest, "invalid_request")
		return
	}
	if err != nil {
		writeAgentError(writer, http.StatusBadGateway, "preview_control_unavailable")
		return
	}
	_ = json.NewEncoder(writer).Encode(map[string]any{"data": value})
}

func (h *AgentHandler) authorized(request *http.Request) bool {
	values := request.Header.Values("Authorization")
	if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer ") {
		return false
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	return len(token) == len(h.config.Token) && subtle.ConstantTimeCompare([]byte(token), []byte(h.config.Token)) == 1
}

func (h *AgentHandler) list(request *http.Request) ([]ControlRecord, error) {
	remote, err := h.config.Control.List(request.Context())
	if err != nil {
		return nil, err
	}
	remoteKeys := make(map[string]bool, len(remote))
	for _, record := range remote {
		if record.EnvironmentID != h.config.EnvironmentID || record.TargetPort < 1 || record.TargetPort > 65535 {
			return nil, ErrControlClientInvalid
		}
		remoteKeys[record.PreviewKey] = true
	}
	changed := false
	for _, local := range h.config.Registry.ListEnvironment(h.config.EnvironmentID) {
		if !remoteKeys[local.Identity] {
			if _, removeErr := h.config.Registry.Remove(local.Identity); removeErr != nil && !errors.Is(removeErr, ErrNotFound) {
				return nil, removeErr
			}
			changed = true
		}
	}
	for _, record := range remote {
		_, err := h.config.Registry.RegisterCanonical(record.PreviewKey, record.URL, h.config.EnvironmentID, record.LogicalName, Target{Host: "127.0.0.1", Port: uint16(record.TargetPort)})
		if err != nil && !errors.Is(err, ErrIdentityConflict) {
			return nil, err
		}
	}
	if changed && h.config.RoutesChanged != nil {
		h.config.RoutesChanged()
	}
	return remote, nil
}

func (h *AgentHandler) create(request *http.Request, operation agentRequest) (ControlRecord, error) {
	target := Target{Host: "127.0.0.1", Port: operation.TargetPort}
	remote, err := h.config.Control.Register(request.Context(), operation.LogicalName, target, true, time.Duration(operation.DurationSeconds)*time.Second, operation.Indefinite)
	if err != nil || remote.EnvironmentID != h.config.EnvironmentID {
		return ControlRecord{}, errors.Join(err, ErrControlClientInvalid)
	}
	reconciled, err := h.list(request)
	if err != nil {
		return ControlRecord{}, err
	}
	for _, record := range reconciled {
		if record.PreviewKey == remote.PreviewKey {
			remote = record
			break
		}
	}
	if h.config.RoutesChanged != nil {
		h.config.RoutesChanged()
	}
	return remote, err
}

func (h *AgentHandler) remove(request *http.Request, logicalName string) (ControlRecord, error) {
	remote, err := h.config.Control.Remove(request.Context(), logicalName)
	if err != nil || remote.EnvironmentID != h.config.EnvironmentID {
		return ControlRecord{}, errors.Join(err, ErrControlClientInvalid)
	}
	if _, removeErr := h.config.Registry.Remove(remote.PreviewKey); removeErr != nil && !errors.Is(removeErr, ErrNotFound) {
		return ControlRecord{}, removeErr
	}
	if h.config.RoutesChanged != nil {
		h.config.RoutesChanged()
	}
	return remote, nil
}

func writeAgentError(writer http.ResponseWriter, status int, code string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"error": map[string]string{"code": code, "message": http.StatusText(status), "status": strconv.Itoa(status)}})
}
