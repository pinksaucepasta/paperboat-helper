package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	pberrors "github.com/pinksaucepasta/paperboat-helper/internal/errors"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/upload"
)

const (
	HeaderRequestID   = "X-Paperboat-Request-ID"
	HeaderOperationID = "X-Paperboat-Operation-ID"
	HeaderDeadlineMS  = "X-Paperboat-Deadline-Ms"
	HeaderFileName    = "X-Paperboat-File-Name"
	HeaderFileMIME    = "X-Paperboat-File-Mime"
	HeaderFileSize    = "X-Paperboat-File-Size"
	HeaderFileSHA256  = "X-Paperboat-File-Sha256"
	HeaderReplayed    = "X-Paperboat-Replayed"
)

type UploadHandlerConfig struct {
	Stager           *upload.Stager
	Journal          *operation.Journal
	Authorizer       AuthorizerFactory
	MaxConcurrent    int
	MaxBodyBytes     int64
	MutationDeadline time.Duration
	Metrics          MetricRecorder
}

type UploadHandler struct {
	config UploadHandlerConfig
	slots  chan struct{}
}

func (h *UploadHandler) Capabilities() []string { return []string{"upload.v1"} }

type uploadMetadata struct {
	Name   string `json:"name"`
	MIME   string `json:"mime"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type uploadRequestError struct {
	stage string
	cause error
}

func (e *uploadRequestError) Error() string { return e.cause.Error() }
func (e *uploadRequestError) Unwrap() error { return e.cause }

func NewUploadHandler(config UploadHandlerConfig) (*UploadHandler, error) {
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = helperconfig.DefaultResources.MaxConcurrentUploads
	}
	if config.MaxBodyBytes == 0 {
		config.MaxBodyBytes = upload.DefaultMaxBytes + 64<<10
	}
	if config.MutationDeadline == 0 {
		config.MutationDeadline = 5 * time.Minute
	}
	if config.Stager == nil || config.Journal == nil || config.Authorizer == nil || config.MaxConcurrent < 1 || config.MaxBodyBytes < upload.DefaultMaxBytes || config.MaxBodyBytes > upload.DefaultMaxBytes+1<<20 || config.MutationDeadline <= 0 || config.MutationDeadline > 5*time.Minute {
		return nil, ErrInvalidConfiguration
	}
	return &UploadHandler{config: config, slots: make(chan struct{}, config.MaxConcurrent)}, nil
}

func (h *UploadHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	requestID := request.Header.Get(HeaderRequestID)
	operationID := request.Header.Get(HeaderOperationID)
	metadata, deadline, err := parseUploadMetadata(request)
	if err != nil {
		writeHTTPErrorDetails(writer, requestID, "invalid_request", http.StatusBadRequest, false, map[string]any{"stage": "metadata"})
		return
	}
	if len(requestID) < 1 || len(requestID) > 128 || len(operationID) < 8 || len(operationID) > 128 {
		writeHTTPErrorDetails(writer, requestID, "invalid_request", http.StatusBadRequest, false, map[string]any{"stage": "request_identity"})
		return
	}
	token, ok := bearerToken(request.Header.Values("Authorization"))
	if !ok {
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return
	}
	authorizer, err := h.config.Authorizer(token)
	if err != nil || authorizer == nil {
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return
	}
	select {
	case h.slots <- struct{}{}:
		defer func() { <-h.slots }()
	default:
		writer.Header().Set("Retry-After", "1")
		writeHTTPError(writer, requestID, "resource_limit", http.StatusTooManyRequests, true)
		return
	}
	if deadline > h.config.MutationDeadline {
		deadline = h.config.MutationDeadline
	}
	ctx, cancel := context.WithTimeout(request.Context(), deadline)
	defer cancel()
	metadataJSON, _ := json.Marshal(metadata)
	frame := protocol.Frame{Type: "request", RequestID: requestID, Version: protocol.ProtocolVersion, OperationID: operationID, Capability: "upload.v1", DeadlineMS: uint32(deadline / time.Millisecond), Payload: metadataJSON}
	authorization, err := authorizer.Authorize(ctx, frame)
	if err != nil {
		h.record("rejected")
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return
	}
	if authorization.JournalBinding == "" || authorization.EnvironmentID == "" {
		h.record("rejected")
		writeHTTPError(writer, requestID, "not_found_or_forbidden", http.StatusNotFound, false)
		return
	}
	canonical, _ := json.Marshal(struct {
		Binding string         `json:"binding"`
		Upload  uploadMetadata `json:"upload"`
	}{authorization.JournalBinding, metadata})
	outcome, replay, executeErr := h.config.Journal.Execute(ctx, operationID, canonical, func(runCtx context.Context) operation.Outcome {
		result, stageErr := h.stageMultipart(runCtx, writer, request, authorization, metadata)
		if stageErr != nil {
			failure := operation.Outcome{ErrorCode: uploadErrorCode(stageErr)}
			var requestErr *uploadRequestError
			if errors.As(stageErr, &requestErr) {
				failure.Result, _ = json.Marshal(map[string]string{"stage": requestErr.stage})
			}
			return failure
		}
		encoded, marshalErr := json.Marshal(result)
		if marshalErr != nil {
			return operation.Outcome{ErrorCode: "unavailable"}
		}
		return operation.Outcome{Result: encoded}
	})
	if executeErr != nil {
		code := operationErrorCode(executeErr)
		h.record(metricResult(code, false))
		writeHTTPError(writer, requestID, code, operationHTTPStatus(code), errors.Is(executeErr, context.DeadlineExceeded))
		return
	}
	if outcome.ErrorCode != "" {
		h.record(metricResult(outcome.ErrorCode, false))
		var details map[string]any
		if len(outcome.Result) != 0 {
			_ = json.Unmarshal(outcome.Result, &details)
		}
		writeHTTPErrorDetails(writer, requestID, outcome.ErrorCode, uploadHTTPStatus(outcome.ErrorCode), outcome.ErrorCode == "resource_limit" || outcome.ErrorCode == "storage_unavailable", details)
		return
	}
	if replay {
		writer.Header().Set(HeaderReplayed, "true")
		writer.Header().Set("Connection", "close")
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusCreated)
	_, _ = writer.Write(outcome.Result)
	h.record(metricResult("", replay))
}

func (h *UploadHandler) record(result string) {
	if h.config.Metrics != nil {
		_ = h.config.Metrics.Record("paperboat_helper_operations_total", 1, map[string]string{"component": "upload", "result": result})
	}
}

func parseUploadMetadata(request *http.Request) (uploadMetadata, time.Duration, error) {
	size, err := strconv.ParseInt(request.Header.Get(HeaderFileSize), 10, 64)
	if err != nil || size < 1 || size > upload.DefaultMaxBytes {
		return uploadMetadata{}, 0, errors.New("invalid size")
	}
	deadlineMS, err := strconv.ParseUint(request.Header.Get(HeaderDeadlineMS), 10, 32)
	if err != nil || deadlineMS < 1 || deadlineMS > 300000 {
		return uploadMetadata{}, 0, errors.New("invalid deadline")
	}
	metadata := uploadMetadata{Name: request.Header.Get(HeaderFileName), MIME: request.Header.Get(HeaderFileMIME), Size: size, SHA256: request.Header.Get(HeaderFileSHA256)}
	if metadata.Name == "" || metadata.MIME == "" || len(metadata.SHA256) != 64 || strings.ToLower(metadata.SHA256) != metadata.SHA256 {
		return uploadMetadata{}, 0, errors.New("invalid metadata")
	}
	decoded, err := hex.DecodeString(metadata.SHA256)
	if err != nil || len(decoded) != 32 {
		return uploadMetadata{}, 0, errors.New("invalid digest")
	}
	return metadata, time.Duration(deadlineMS) * time.Millisecond, nil
}

func (h *UploadHandler) stageMultipart(ctx context.Context, writer http.ResponseWriter, request *http.Request, authorization Authorization, metadata uploadMetadata) (upload.Result, error) {
	request.Body = http.MaxBytesReader(writer, request.Body, h.config.MaxBodyBytes)
	reader, err := request.MultipartReader()
	if err != nil {
		return upload.Result{}, &uploadRequestError{stage: "multipart_header", cause: err}
	}
	part, err := reader.NextPart()
	if err != nil {
		return upload.Result{}, &uploadRequestError{stage: "multipart_part", cause: err}
	}
	if part.FormName() != "file" || part.FileName() != metadata.Name || part.Header.Get("Content-Type") != metadata.MIME {
		part.Close()
		return upload.Result{}, &uploadRequestError{stage: "multipart_metadata", cause: errors.New("multipart metadata mismatch")}
	}
	result, err := h.config.Stager.Stage(ctx, upload.Request{EnvironmentID: authorization.EnvironmentID, DisplayName: metadata.Name, DeclaredMIME: metadata.MIME, DeclaredSize: metadata.Size, CredentialExpiry: authorization.ExpiresAt, ExpectedSHA256: metadata.SHA256, Body: part})
	part.Close()
	if err != nil {
		return upload.Result{}, err
	}
	next, nextErr := reader.NextPart()
	if next != nil {
		next.Close()
	}
	if nextErr != io.EOF {
		removeErr := h.config.Stager.Remove(result.Path)
		return upload.Result{}, &uploadRequestError{stage: "multipart_extra", cause: errors.Join(errors.New("multipart must contain exactly one file"), nextErr, removeErr)}
	}
	relativePath := result.Path
	absolutePath, pathErr := h.config.Stager.AbsolutePath(relativePath)
	if pathErr != nil {
		return upload.Result{}, errors.Join(pathErr, h.config.Stager.Remove(relativePath))
	}
	result.Path = absolutePath
	return result, nil
}

func uploadErrorCode(err error) string {
	var uploadErr *upload.Error
	if errors.As(err, &uploadErr) {
		return string(uploadErr.Code)
	}
	if errors.Is(err, context.Canceled) {
		return "operation_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "invalid_request"
}

func uploadHTTPStatus(code string) int {
	switch code {
	case "resource_limit":
		return http.StatusTooManyRequests
	case "storage_unavailable", "unavailable":
		return http.StatusServiceUnavailable
	case "operation_canceled", "deadline_exceeded":
		return http.StatusRequestTimeout
	default:
		return http.StatusBadRequest
	}
}

func operationHTTPStatus(code string) int {
	switch code {
	case "operation_id_conflict":
		return http.StatusConflict
	case "resource_limit":
		return http.StatusTooManyRequests
	case "deadline_exceeded", "operation_canceled":
		return http.StatusRequestTimeout
	default:
		return http.StatusServiceUnavailable
	}
}

func writeHTTPError(writer http.ResponseWriter, requestID, code string, status int, retryable bool) {
	writeHTTPErrorDetails(writer, requestID, code, status, retryable, nil)
}

func writeHTTPErrorDetails(writer http.ResponseWriter, requestID, code string, status int, retryable bool, details map[string]any) {
	if requestID == "" {
		requestID = "unknown"
	}
	payload, _ := json.Marshal(pberrors.Error{Code: pberrors.Code(code), Message: http.StatusText(status), RequestID: requestID, Retryable: retryable, Details: details})
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = fmt.Fprintln(writer, string(payload))
}
