package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

const (
	HeaderUploadOffset = "Upload-Offset"
	HeaderUploadLength = "Upload-Length"
	HeaderUploadDigest = "Upload-Digest"
)

type FileTransferHandlerConfig struct {
	Service        *filetransfer.Service
	Journal        *operation.Journal
	Authorizer     AuthorizerFactory
	AllowDirection func(Authorization, string) bool
	ResolveClient  func(Authorization, string, string) (string, error)
	Owns           func(Authorization, store.FileTransfer) bool
	MaxCreateBytes int64
}

type FileTransferHandler struct{ config FileTransferHandlerConfig }

type createFileTransferRequest struct {
	BatchID   string              `json:"batch_id"`
	Direction string              `json:"direction"`
	SessionID string              `json:"session_id"`
	Files     []filetransfer.File `json:"files"`
}

type createFileTransferResponse struct {
	BatchID   string               `json:"batch_id"`
	Transfers []store.FileTransfer `json:"transfers"`
}

type completeFileTransferResponse struct {
	Transfer store.FileTransfer `json:"transfer"`
	Result   struct {
		Code string `json:"code"`
		Path string `json:"path,omitempty"`
	} `json:"result"`
}

func NewFileTransferHandler(config FileTransferHandlerConfig) (*FileTransferHandler, error) {
	if config.MaxCreateBytes == 0 {
		config.MaxCreateBytes = 32 << 10
	}
	if config.Service == nil || config.Journal == nil || config.Authorizer == nil || config.AllowDirection == nil || config.MaxCreateBytes < 1024 || config.MaxCreateBytes > 1<<20 {
		return nil, ErrInvalidConfiguration
	}
	return &FileTransferHandler{config: config}, nil
}

func (h *FileTransferHandler) Capabilities() []string { return []string{"file-transfer.v1"} }

func (h *FileTransferHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	requestID := request.Header.Get(HeaderRequestID)
	authorization, release, ok := h.authorize(writer, request, requestID)
	if !ok {
		return
	}
	defer release()
	if authorization.Revoked != nil && authorization.Revoked.Load() {
		writeHTTPError(writer, requestID, "canceled", http.StatusUnauthorized, false)
		return
	}
	if authorization.RevokedSignal != nil {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		go func() {
			select {
			case <-authorization.RevokedSignal:
				cancel()
				_ = request.Body.Close()
			case <-ctx.Done():
			}
		}()
		request = request.WithContext(ctx)
	}
	relative := strings.TrimPrefix(path.Clean(request.URL.Path), "/v1/file-transfers")
	if relative == "." || relative == "" || relative == "/" {
		h.serveCollection(writer, request, requestID, authorization)
		return
	}
	if relative == "/pending" {
		h.servePending(writer, request, requestID, authorization)
		return
	}
	parts := strings.Split(strings.Trim(relative, "/"), "/")
	if len(parts) < 1 || len(parts) > 2 || parts[0] == "" {
		writeHTTPError(writer, requestID, "not_found", http.StatusNotFound, false)
		return
	}
	id := parts[0]
	if len(parts) == 1 {
		h.serveManifest(writer, request, requestID, authorization, id)
		return
	}
	switch parts[1] {
	case "content":
		h.serveContent(writer, request, requestID, authorization, id)
	case "complete":
		h.serveComplete(writer, request, requestID, authorization, id)
	case "receipt":
		h.serveReceipt(writer, request, requestID, authorization, id)
	default:
		writeHTTPError(writer, requestID, "not_found", http.StatusNotFound, false)
	}
}

func (h *FileTransferHandler) authorize(writer http.ResponseWriter, request *http.Request, requestID string) (Authorization, func(), bool) {
	release := func() {}
	token, ok := bearerToken(request.Header.Values("Authorization"))
	if !ok {
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return Authorization{}, release, false
	}
	authorizer, err := h.config.Authorizer(token)
	if err != nil || authorizer == nil {
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return Authorization{}, release, false
	}
	if closer, ok := authorizer.(AuthorizationCloser); ok {
		release = closer.CloseAuthorization
	}
	frame := protocol.Frame{Type: "request", Version: protocol.ProtocolVersion, RequestID: requestID, OperationID: request.Header.Get(HeaderOperationID), Capability: "file-transfer.v1"}
	authz, err := authorizer.Authorize(request.Context(), frame)
	if err != nil || authz.JournalBinding == "" || authz.ClientID == "" && h.config.ResolveClient == nil {
		release()
		writeHTTPError(writer, requestID, "unauthorized", http.StatusUnauthorized, false)
		return Authorization{}, func() {}, false
	}
	return authz, release, true
}

func (h *FileTransferHandler) serveCollection(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	operationID := request.Header.Get(HeaderOperationID)
	if len(operationID) < 8 || len(operationID) > 128 {
		writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, h.config.MaxCreateBytes))
	if err != nil {
		writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
		return
	}
	var input createFileTransferRequest
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil || input.BatchID == "" || input.SessionID == "" || authorization.SessionID != "" && input.SessionID != authorization.SessionID || !h.config.AllowDirection(authorization, input.Direction) {
		writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
		return
	}
	if transfers, exists, matches := h.recoverBatch(request.Context(), authorization, input); exists {
		if !matches {
			writeHTTPError(writer, requestID, "operation_conflict", http.StatusConflict, false)
			return
		}
		writer.Header().Set(HeaderReplayed, "true")
		writeJSON(writer, http.StatusCreated, createFileTransferResponse{BatchID: input.BatchID, Transfers: transfers})
		return
	}
	canonical, _ := json.Marshal(struct {
		Binding string                    `json:"binding"`
		Request createFileTransferRequest `json:"request"`
	}{authorization.JournalBinding, input})
	clientID := authorization.ClientID
	var resolveErr error
	if h.config.ResolveClient != nil {
		clientID, resolveErr = h.config.ResolveClient(authorization, input.Direction, input.SessionID)
	}
	if resolveErr != nil || clientID == "" {
		writeHTTPError(writer, requestID, "no_active_writer", http.StatusConflict, false)
		return
	}
	outcome, replay, err := h.config.Journal.Execute(request.Context(), operationID, canonical, func(ctx context.Context) operation.Outcome {
		transfers, createErr := h.config.Service.Create(ctx, filetransfer.CreateRequest{BatchID: input.BatchID, Direction: input.Direction, SessionID: input.SessionID, ClientID: clientID, Files: input.Files})
		if createErr != nil {
			return operation.Outcome{ErrorCode: fileTransferErrorCode(createErr)}
		}
		encoded, marshalErr := json.Marshal(createFileTransferResponse{BatchID: input.BatchID, Transfers: transfers})
		if marshalErr != nil {
			return operation.Outcome{ErrorCode: "storage_unavailable"}
		}
		return operation.Outcome{Result: encoded}
	})
	if err != nil {
		writeHTTPError(writer, requestID, operationErrorCode(err), operationHTTPStatus(operationErrorCode(err)), false)
		return
	}
	if outcome.ErrorCode != "" {
		writeHTTPError(writer, requestID, outcome.ErrorCode, fileTransferHTTPStatus(outcome.ErrorCode), false)
		return
	}
	if replay {
		writer.Header().Set(HeaderReplayed, "true")
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusCreated)
	_, _ = writer.Write(outcome.Result)
}

func (h *FileTransferHandler) recoverBatch(ctx context.Context, authorization Authorization, input createFileTransferRequest) ([]store.FileTransfer, bool, bool) {
	existing, err := h.config.Service.Batch(ctx, input.BatchID)
	if err != nil {
		return nil, false, false
	}
	if len(existing) != len(input.Files) {
		return nil, true, false
	}
	ordered := make([]store.FileTransfer, len(input.Files))
	used := make([]bool, len(existing))
	for inputIndex, file := range input.Files {
		matched := -1
		for existingIndex, transfer := range existing {
			if used[existingIndex] || transfer.Direction != input.Direction || transfer.SessionID != input.SessionID || h.config.ResolveClient == nil && authorization.ClientID != "" && transfer.ClientID != authorization.ClientID {
				continue
			}
			if transfer.Basename == file.Basename && transfer.Size == file.Size && transfer.SHA256 == file.SHA256 {
				matched = existingIndex
				break
			}
		}
		if matched < 0 {
			return nil, true, false
		}
		used[matched] = true
		ordered[inputIndex] = existing[matched]
	}
	return ordered, true, true
}

func (h *FileTransferHandler) serveManifest(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization, id string) {
	transfer, ok := h.owned(writer, request, requestID, authorization, id)
	if !ok {
		return
	}
	switch request.Method {
	case http.MethodGet:
		writeJSON(writer, http.StatusOK, transfer)
	case http.MethodDelete:
		if err := h.config.Service.Cancel(request.Context(), id); err != nil {
			writeFileTransferError(writer, requestID, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, http.MethodGet, http.MethodDelete)
	}
}

func (h *FileTransferHandler) serveContent(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization, id string) {
	transfer, ok := h.owned(writer, request, requestID, authorization, id)
	if !ok {
		return
	}
	etag := `"sha256:` + transfer.SHA256 + `"`
	switch request.Method {
	case http.MethodHead:
		writer.Header().Set(HeaderUploadOffset, strconv.FormatInt(transfer.CommittedOffset, 10))
		writer.Header().Set(HeaderUploadLength, strconv.FormatInt(transfer.Size, 10))
		writer.Header().Set(HeaderUploadDigest, "sha256="+transfer.SHA256)
		writer.Header().Set("ETag", etag)
		writer.WriteHeader(http.StatusNoContent)
	case http.MethodPatch:
		if request.Header.Get("Content-Type") != "application/offset+octet-stream" {
			writeHTTPError(writer, requestID, "invalid_request", http.StatusUnsupportedMediaType, false)
			return
		}
		offset, err := strconv.ParseInt(request.Header.Get(HeaderUploadOffset), 10, 64)
		if err != nil {
			writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
			return
		}
		stopClose := make(chan struct{})
		go func() {
			select {
			case <-h.config.Service.CancellationSignal(id):
				_ = request.Body.Close()
			case <-request.Context().Done():
			case <-stopClose:
			}
		}()
		updated, err := h.config.Service.Append(request.Context(), id, offset, request.Body)
		close(stopClose)
		if err != nil {
			writeFileTransferError(writer, requestID, err)
			return
		}
		writer.Header().Set(HeaderUploadOffset, strconv.FormatInt(updated.CommittedOffset, 10))
		writer.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if match := request.Header.Get("If-Match"); match != "" && match != etag {
			writeHTTPError(writer, requestID, "precondition_failed", http.StatusPreconditionFailed, false)
			return
		}
		file, current, err := h.config.Service.OpenContent(request.Context(), id)
		if err != nil {
			writeFileTransferError(writer, requestID, err)
			return
		}
		defer file.Close()
		writer.Header().Set("ETag", etag)
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("Accept-Ranges", "bytes")
		http.ServeContent(writer, request, current.Basename, current.CreatedAt, contextReadSeeker{ctx: request.Context(), reader: file})
	default:
		methodNotAllowed(writer, http.MethodHead, http.MethodPatch, http.MethodGet)
	}
}

type contextReadSeeker struct {
	ctx    context.Context
	reader io.ReadSeeker
}

func (r contextReadSeeker) Read(buffer []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(buffer)
	}
}

func (r contextReadSeeker) Seek(offset int64, whence int) (int64, error) {
	return r.reader.Seek(offset, whence)
}

func (h *FileTransferHandler) serveComplete(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization, id string) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	if _, ok := h.owned(writer, request, requestID, authorization, id); !ok {
		return
	}
	transfer, err := h.config.Service.Complete(request.Context(), id)
	if err != nil {
		writeFileTransferError(writer, requestID, err)
		return
	}
	result := completeFileTransferResponse{Transfer: transfer}
	result.Result.Code = "published"
	if transfer.State == "pending" {
		result.Result.Code = "pending"
	}
	if transfer.Direction == "pb_to_pbh" {
		published, err := h.config.Service.PublishedPath(request.Context(), id)
		if err != nil {
			writeFileTransferError(writer, requestID, err)
			return
		}
		result.Result.Path = published
	}
	writeJSON(writer, http.StatusOK, result)
}

func (h *FileTransferHandler) servePending(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	sessionID := request.URL.Query().Get("session_id")
	if sessionID == "" || authorization.SessionID != "" && sessionID != authorization.SessionID {
		writeHTTPError(writer, requestID, "not_found_or_forbidden", http.StatusNotFound, false)
		return
	}
	waitSeconds, err := strconv.Atoi(request.URL.Query().Get("wait_seconds"))
	if err != nil || waitSeconds < 0 || waitSeconds > 30 {
		writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
		return
	}
	deadline := time.NewTimer(time.Duration(waitSeconds) * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		transfers, err := h.config.Service.Pending(request.Context(), authorization.ClientID, sessionID, 10)
		if err != nil {
			writeFileTransferError(writer, requestID, err)
			return
		}
		if len(transfers) > 0 {
			writeJSON(writer, http.StatusOK, map[string]any{"transfers": transfers})
			return
		}
		if waitSeconds == 0 {
			writeJSON(writer, http.StatusOK, map[string]any{"transfers": []store.FileTransfer{}})
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-deadline.C:
			writeJSON(writer, http.StatusOK, map[string]any{"transfers": []store.FileTransfer{}})
			return
		case <-ticker.C:
		}
	}
}

func (h *FileTransferHandler) serveReceipt(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization, id string) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	transfer, ok := h.owned(writer, request, requestID, authorization, id)
	if !ok {
		return
	}
	var input struct {
		ResultCode string `json:"result_code"`
		Path       string `json:"path"`
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || !validReceipt(input.ResultCode, input.Path) {
		writeHTTPError(writer, requestID, "invalid_request", http.StatusBadRequest, false)
		return
	}
	if err := h.config.Service.Receipt(request.Context(), id, transfer.ClientID, input.ResultCode, input.Path); err != nil {
		writeFileTransferError(writer, requestID, err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func validReceipt(code, path string) bool {
	if code == "stored" {
		return path != "" && !strings.HasPrefix(path, "/") && strings.HasPrefix(path, "Paperboat Inbox/")
	}
	if path != "" {
		return false
	}
	switch code {
	case "invalid_path", "invalid_size", "digest_mismatch", "offset_conflict", "recipient_unavailable", "storage_unavailable", "resource_limit", "canceled", "delivery_timeout":
		return true
	default:
		return false
	}
}

func (h *FileTransferHandler) owned(writer http.ResponseWriter, request *http.Request, requestID string, authorization Authorization, id string) (store.FileTransfer, bool) {
	transfer, err := h.config.Service.Get(request.Context(), id)
	owned := transfer.ClientID == authorization.ClientID && (!(authorization.SessionID != "") || transfer.SessionID == authorization.SessionID)
	if h.config.Owns != nil {
		owned = h.config.Owns(authorization, transfer)
	}
	if err != nil || !owned {
		writeHTTPError(writer, requestID, "not_found_or_forbidden", http.StatusNotFound, false)
		return store.FileTransfer{}, false
	}
	return transfer, true
}
func methodNotAllowed(writer http.ResponseWriter, methods ...string) {
	writer.Header().Set("Allow", strings.Join(methods, ", "))
	http.Error(writer, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
}
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
func writeFileTransferError(writer http.ResponseWriter, requestID string, err error) {
	code := fileTransferErrorCode(err)
	writeHTTPError(writer, requestID, code, fileTransferHTTPStatus(code), code == "storage_unavailable" || code == "resource_limit")
}
func fileTransferErrorCode(err error) string {
	var transferErr *filetransfer.Error
	if errors.As(err, &transferErr) {
		return string(transferErr.Code)
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "delivery_timeout"
	}
	return "storage_unavailable"
}
func fileTransferHTTPStatus(code string) int {
	switch code {
	case "invalid_path":
		return http.StatusNotFound
	case "invalid_size", "batch_limit":
		return http.StatusBadRequest
	case "offset_conflict":
		return http.StatusConflict
	case "no_active_writer", "recipient_unavailable":
		return http.StatusConflict
	case "digest_mismatch":
		return http.StatusUnprocessableEntity
	case "canceled":
		return 499
	case "resource_limit":
		return http.StatusTooManyRequests
	default:
		return http.StatusServiceUnavailable
	}
}
