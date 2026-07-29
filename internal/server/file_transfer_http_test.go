package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

type readProbe struct{ reads int }

func (r *readProbe) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }
func (*readProbe) Close() error               { return nil }

type revocableBody struct {
	started chan struct{}
	closed  chan struct{}
	once    sync.Once
}

type closingFileAuthorizer struct {
	authorization Authorization
	closed        *atomic.Int32
}

func (a *closingFileAuthorizer) Authorize(context.Context, protocol.Frame) (Authorization, error) {
	return a.authorization, nil
}
func (a *closingFileAuthorizer) CloseAuthorization() { a.closed.Add(1) }

func (b *revocableBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.closed
	return 0, net.ErrClosed
}
func (b *revocableBody) Close() error {
	select {
	case <-b.closed:
	default:
		close(b.closed)
	}
	return nil
}

func fileTransferTestHandler(t *testing.T) (*FileTransferHandler, *store.Store) {
	t.Helper()
	root := t.TempDir()
	durable, err := store.Open(context.Background(), store.Config{Root: filepath.Join(root, "state")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	service, err := filetransfer.New(filetransfer.Config{Root: filepath.Join(root, "files"), Store: durable})
	if err != nil {
		t.Fatal(err)
	}
	journal, _ := operation.NewJournal(32)
	handler, err := NewFileTransferHandler(FileTransferHandlerConfig{Service: service, Journal: journal, Authorizer: func(token string) (Authorizer, error) {
		if token != "token" {
			return nil, errors.New("invalid")
		}
		return authorizerFunc(func(context.Context, protocol.Frame) (Authorization, error) {
			return Authorization{JournalBinding: "env:1:cli:1", EnvironmentID: "env_1", ClientID: "cli_1", SessionID: "ses_1"}, nil
		}), nil
	}, AllowDirection: func(_ Authorization, direction string) bool { return direction == "pb_to_pbh" }})
	if err != nil {
		t.Fatal(err)
	}
	return handler, durable
}
func transferRequest(method, url string, body []byte) *http.Request {
	request := httptest.NewRequest(method, url, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set(HeaderRequestID, "req_ft")
	request.Header.Set(HeaderOperationID, "operation_ft_1")
	return request
}
func transferDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestFileTransferHTTPResumesCompletesAndRangesOpaqueContent(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	data := []byte("abcdefgh")
	input, _ := json.Marshal(createFileTransferRequest{BatchID: "fb_1", Direction: "pb_to_pbh", SessionID: "ses_1", Files: []filetransfer.File{{Basename: "archive.dat", Size: int64(len(data)), SHA256: transferDigest(data)}}})
	request := transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", input)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create=%d %s", response.Code, response.Body.String())
	}
	var created createFileTransferResponse
	if err := json.Unmarshal(response.Body.Bytes(), &created); err != nil || len(created.Transfers) != 1 {
		t.Fatalf("created=%#v err=%v", created, err)
	}
	id := created.Transfers[0].ID
	for _, part := range []struct {
		offset int64
		chunk  []byte
	}{{0, data[:3]}, {3, data[3:]}} {
		request = transferRequest(http.MethodPatch, "http://helper.test/v1/file-transfers/"+id+"/content", part.chunk)
		request.Header.Set("Content-Type", "application/offset+octet-stream")
		request.Header.Set(HeaderUploadOffset, strconv.FormatInt(part.offset, 10))
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("patch=%d %s", response.Code, response.Body.String())
		}
	}
	request = transferRequest(http.MethodHead, "http://helper.test/v1/file-transfers/"+id+"/content", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || response.Header().Get(HeaderUploadOffset) != "8" || response.Header().Get("ETag") == "" {
		t.Fatalf("head=%d headers=%v", response.Code, response.Header())
	}
	for attempt := 0; attempt < 2; attempt++ {
		request = transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers/"+id+"/complete", nil)
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("complete attempt=%d status=%d %s", attempt, response.Code, response.Body.String())
		}
	}
	etag := `"sha256:` + transferDigest(data) + `"`
	request = transferRequest(http.MethodGet, "http://helper.test/v1/file-transfers/"+id+"/content", nil)
	request.Header.Set("If-Match", etag)
	request.Header.Set("Range", "bytes=2-5")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusPartialContent || response.Body.String() != "cdef" || response.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("download=%d body=%q headers=%v", response.Code, response.Body.String(), response.Header())
	}
}

func TestFileTransferHTTPCreateIsIdempotentAndOffsetConflictReportsCommittedOffset(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	data := []byte("x")
	input, _ := json.Marshal(createFileTransferRequest{BatchID: "fb_1", Direction: "pb_to_pbh", SessionID: "ses_1", Files: []filetransfer.File{{Basename: "x", Size: 1, SHA256: transferDigest(data)}}})
	var first createFileTransferResponse
	for attempt := 0; attempt < 2; attempt++ {
		request := transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", input)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusCreated {
			t.Fatalf("attempt=%d status=%d", attempt, response.Code)
		}
		var got createFileTransferResponse
		_ = json.Unmarshal(response.Body.Bytes(), &got)
		if attempt == 0 {
			first = got
		} else if got.Transfers[0].ID != first.Transfers[0].ID || response.Header().Get(HeaderReplayed) != "true" {
			t.Fatalf("replay=%#v headers=%v", got, response.Header())
		}
	}
	id := first.Transfers[0].ID
	request := transferRequest(http.MethodPatch, "http://helper.test/v1/file-transfers/"+id+"/content", data)
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	request.Header.Set(HeaderUploadOffset, "1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("conflict=%d %s", response.Code, response.Body.String())
	}
}

func TestFileTransferHTTPAuthenticatesBeforeReadingCreateBody(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	probe := &readProbe{}
	request := transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", nil)
	request.Header.Set("Authorization", "Bearer wrong")
	request.Body = probe
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || probe.reads != 0 {
		t.Fatalf("status=%d reads=%d", response.Code, probe.reads)
	}
}

func TestFileTransferHTTPRejectsSessionOutsideCredentialBinding(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	input, _ := json.Marshal(createFileTransferRequest{BatchID: "fb_1", Direction: "pb_to_pbh", SessionID: "ses_other", Files: []filetransfer.File{{Basename: "x", Size: 0, SHA256: transferDigest(nil)}}})
	request := transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", input)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestFileTransferHTTPPendingIsRecipientPinnedAndReceiptIsRelative(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	data := []byte("delivery")
	created, err := handler.config.Service.Create(context.Background(), filetransfer.CreateRequest{BatchID: "fb_send", Direction: "pbh_to_pb", SessionID: "ses_1", ClientID: "cli_1", Files: []filetransfer.File{{Basename: "delivery.bin", Size: int64(len(data)), SHA256: transferDigest(data)}}})
	if err != nil {
		t.Fatal(err)
	}
	id := created[0].ID
	if _, err := handler.config.Service.Append(context.Background(), id, 0, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if _, err := handler.config.Service.Complete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	request := transferRequest(http.MethodGet, "http://helper.test/v1/file-transfers/pending?session_id=ses_1&wait_seconds=0", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !bytes.Contains(response.Body.Bytes(), []byte(id)) {
		t.Fatalf("pending=%d %s", response.Code, response.Body.String())
	}
	receipt, _ := json.Marshal(map[string]any{"result_code": "stored", "path": "Paperboat Inbox/delivery.bin"})
	request = transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers/"+id+"/receipt", receipt)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("receipt=%d %s", response.Code, response.Body.String())
	}
	manifest, err := handler.config.Service.Get(context.Background(), id)
	if err != nil || manifest.State != "delivered" || manifest.ReceiptPath != "Paperboat Inbox/delivery.bin" {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
	request = transferRequest(http.MethodGet, "http://helper.test/v1/file-transfers/pending?session_id=ses_1&wait_seconds=0", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || bytes.Contains(response.Body.Bytes(), []byte(id)) {
		t.Fatalf("redelivery=%d %s", response.Code, response.Body.String())
	}
}

func TestFileTransferHTTPRejectsUnknownOrPathBearingFailureReceipt(t *testing.T) {
	for _, input := range []struct{ code, path string }{
		{"made_up", ""},
		{"storage_unavailable", "Paperboat Inbox/x"},
		{"stored", "/absolute/x"},
	} {
		if validReceipt(input.code, input.path) {
			t.Fatalf("accepted receipt %#v", input)
		}
	}
}

func TestFileTransferHTTPAgentCreatePinsResolvedWriter(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	handler.config.ResolveClient = func(_ Authorization, direction, sessionID string) (string, error) {
		if direction != "pbh_to_pb" || sessionID != "ses_1" {
			return "", errors.New("invalid")
		}
		return "cli_writer", nil
	}
	handler.config.AllowDirection = func(_ Authorization, direction string) bool { return direction == "pbh_to_pb" }
	input, _ := json.Marshal(createFileTransferRequest{BatchID: "fb_agent", Direction: "pbh_to_pb", SessionID: "ses_1", Files: []filetransfer.File{{Basename: "x", Size: 0, SHA256: transferDigest(nil)}}})
	request := transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", input)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if bytes.Contains(response.Body.Bytes(), []byte("client_id")) {
		t.Fatalf("response exposed internal recipient: %s", response.Body.String())
	}
	var result createFileTransferResponse
	_ = json.Unmarshal(response.Body.Bytes(), &result)
	persisted, err := handler.config.Service.Get(context.Background(), result.Transfers[0].ID)
	if err != nil || persisted.ClientID != "cli_writer" {
		t.Fatalf("persisted=%#v err=%v", persisted, err)
	}
	handler.config.ResolveClient = func(Authorization, string, string) (string, error) {
		return "", errors.New("writer changed")
	}
	request = transferRequest(http.MethodPost, "http://helper.test/v1/file-transfers", input)
	request.Header.Set(HeaderOperationID, "operation_ft_recovered")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated || response.Header().Get(HeaderReplayed) != "true" {
		t.Fatalf("recovery status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	var recovered createFileTransferResponse
	_ = json.Unmarshal(response.Body.Bytes(), &recovered)
	if recovered.Transfers[0].ID != result.Transfers[0].ID {
		t.Fatalf("recovered=%#v", recovered)
	}
}

func TestFileTransferHTTPRevocationInterruptsBlockedUploadAndReleasesWatcher(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	data := []byte("x")
	created, err := handler.config.Service.Create(context.Background(), filetransfer.CreateRequest{BatchID: "fb_revoke", Direction: "pb_to_pbh", SessionID: "ses_1", ClientID: "cli_1", Files: []filetransfer.File{{Basename: "x", Size: 1, SHA256: transferDigest(data)}}})
	if err != nil {
		t.Fatal(err)
	}
	revoked := &atomic.Bool{}
	signal := make(chan struct{})
	closed := &atomic.Int32{}
	handler.config.Authorizer = func(string) (Authorizer, error) {
		return &closingFileAuthorizer{authorization: Authorization{JournalBinding: "env:1:cli:1", EnvironmentID: "env_1", ClientID: "cli_1", SessionID: "ses_1", Revoked: revoked, RevokedSignal: signal}, closed: closed}, nil
	}
	body := &revocableBody{started: make(chan struct{}), closed: make(chan struct{})}
	request := transferRequest(http.MethodPatch, "http://helper.test/v1/file-transfers/"+created[0].ID+"/content", nil)
	request.Header.Set("Content-Type", "application/offset+octet-stream")
	request.Header.Set(HeaderUploadOffset, "0")
	request.Body = body
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("upload body was not read")
	}
	revoked.Store(true)
	close(signal)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("revocation did not interrupt upload")
	}
	if response.Code != 499 || closed.Load() != 1 {
		t.Fatalf("status=%d watcher closes=%d body=%s", response.Code, closed.Load(), response.Body.String())
	}
	manifest, err := handler.config.Service.Get(context.Background(), created[0].ID)
	if err != nil || manifest.CommittedOffset != 0 {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
}

func TestFileTransferHTTPDeleteInterruptsBlockedUpload(t *testing.T) {
	handler, _ := fileTransferTestHandler(t)
	created, err := handler.config.Service.Create(context.Background(), filetransfer.CreateRequest{BatchID: "fb_cancel_active", Direction: "pb_to_pbh", SessionID: "ses_1", ClientID: "cli_1", Files: []filetransfer.File{{Basename: "x", Size: 1, SHA256: transferDigest([]byte("x"))}}})
	if err != nil {
		t.Fatal(err)
	}
	id := created[0].ID
	body := &revocableBody{started: make(chan struct{}), closed: make(chan struct{})}
	patchRequest := transferRequest(http.MethodPatch, "http://helper.test/v1/file-transfers/"+id+"/content", nil)
	patchRequest.Header.Set("Content-Type", "application/offset+octet-stream")
	patchRequest.Header.Set(HeaderUploadOffset, "0")
	patchRequest.Body = body
	patchResponse := httptest.NewRecorder()
	patchDone := make(chan struct{})
	go func() {
		handler.ServeHTTP(patchResponse, patchRequest)
		close(patchDone)
	}()
	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("upload body was not read")
	}
	deleteResponse := httptest.NewRecorder()
	handler.ServeHTTP(deleteResponse, transferRequest(http.MethodDelete, "http://helper.test/v1/file-transfers/"+id, nil))
	if deleteResponse.Code != http.StatusNoContent {
		t.Fatalf("delete status=%d body=%s", deleteResponse.Code, deleteResponse.Body.String())
	}
	select {
	case <-patchDone:
	case <-time.After(time.Second):
		t.Fatal("delete did not interrupt upload")
	}
	if patchResponse.Code != 499 {
		t.Fatalf("patch status=%d body=%s", patchResponse.Code, patchResponse.Body.String())
	}
}
