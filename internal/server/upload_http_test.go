package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/upload"
)

func uploadTestHandler(t *testing.T, authorize authorizerFunc) (*UploadHandler, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "uploads")
	stager, err := upload.New(upload.Config{Root: root, Random: bytes.NewReader(make([]byte, 512)), MaxConcurrent: 2})
	if err != nil {
		t.Fatal(err)
	}
	journal, _ := operation.NewJournal(32)
	handler, err := NewUploadHandler(UploadHandlerConfig{Stager: stager, Journal: journal, Authorizer: func(token string) (Authorizer, error) {
		if token != "token" {
			return nil, errors.New("bad token")
		}
		return authorize, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return handler, root
}

func multipartUpload(t *testing.T, operationID, name, mime string, data []byte, extra bool) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	header := make(textproto.MIMEHeader)
	header["Content-Disposition"] = []string{`form-data; name="file"; filename="` + name + `"`}
	header["Content-Type"] = []string{mime}
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(data)
	if extra {
		part, _ := writer.CreateFormField("extra")
		_, _ = part.Write([]byte("unexpected"))
	}
	_ = writer.Close()
	digest := sha256.Sum256(data)
	request := httptest.NewRequest(http.MethodPost, "http://helper.test/upload", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("Authorization", "Bearer token")
	request.Header.Set(HeaderRequestID, "req_upload")
	request.Header.Set(HeaderOperationID, operationID)
	request.Header.Set(HeaderDeadlineMS, "30000")
	request.Header.Set(HeaderFileName, name)
	request.Header.Set(HeaderFileMIME, mime)
	request.Header.Set(HeaderFileSize, stringInt64(int64(len(data))))
	request.Header.Set(HeaderFileSHA256, hex.EncodeToString(digest[:]))
	return request
}

func stringInt64(value int64) string { return strconv.FormatInt(value, 10) }

func uploadAuthorization(context.Context, protocol.Frame) (Authorization, error) {
	return Authorization{JournalBinding: "env:env_1:user:usr_1", EnvironmentID: "env_1", UserID: "usr_1", ClientID: "cli_1", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func TestUploadHTTPStagesExactlyOneAuthenticatedFile(t *testing.T) {
	handler, root := uploadTestHandler(t, uploadAuthorization)
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, multipartUpload(t, "op_upload_0001", "diagram.png", "image/png", png, false))
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result upload.Result
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.SHA256 == "" || !filepath.IsAbs(result.Path) {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	if _, err := os.Stat(result.Path); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(result.Path, root+string(filepath.Separator)) {
		t.Fatalf("path escaped upload root: %q", result.Path)
	}
}

func TestUploadHTTPStagesNonImageFile(t *testing.T) {
	handler, _ := uploadTestHandler(t, uploadAuthorization)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, multipartUpload(t, "op_upload_text_01", "notes.txt", "text/plain", []byte("notes\n"), false))
	if response.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result upload.Result
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.MIME != "text/plain" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

type readProbe struct{ reads int }

func (r *readProbe) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }
func (*readProbe) Close() error               { return nil }

func TestUploadHTTPReplayAndConflictDoNotReadBody(t *testing.T) {
	handler, _ := uploadTestHandler(t, uploadAuthorization)
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	first := multipartUpload(t, "op_upload_0001", "diagram.png", "image/png", png, false)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, first)
	if response.Code != http.StatusCreated {
		t.Fatalf("first=%d %s", response.Code, response.Body.String())
	}
	for _, testCase := range []struct {
		name     string
		conflict bool
	}{{"replay", false}, {"conflict", true}} {
		probe := &readProbe{}
		request := multipartUpload(t, "op_upload_0001", "diagram.png", "image/png", png, false)
		request.Body = probe
		if testCase.conflict {
			request.Header.Set(HeaderFileName, "other.png")
		}
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if probe.reads != 0 {
			t.Fatalf("%s read body %d times", testCase.name, probe.reads)
		}
		if testCase.conflict && response.Code != http.StatusConflict {
			t.Fatalf("conflict status=%d body=%s", response.Code, response.Body.String())
		}
		if !testCase.conflict && (response.Code != http.StatusCreated || response.Header().Get(HeaderReplayed) != "true") {
			t.Fatalf("replay status=%d headers=%v", response.Code, response.Header())
		}
	}
}

func TestUploadHTTPRejectsExtraPartAndRemovesPublication(t *testing.T) {
	handler, root := uploadTestHandler(t, uploadAuthorization)
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, multipartUpload(t, "op_upload_0001", "diagram.png", "image/png", png, true))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if !bytes.Contains(response.Body.Bytes(), []byte(`"stage":"multipart_extra"`)) {
		t.Fatalf("missing bounded failure stage: %s", response.Body.String())
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
}

func TestUploadHTTPReportsBoundedMetadataFailureStage(t *testing.T) {
	handler, _ := uploadTestHandler(t, uploadAuthorization)
	request := httptest.NewRequest(http.MethodPost, "http://helper.test/upload", nil)
	request.Header.Set(HeaderRequestID, "req_upload")
	request.Header.Set(HeaderOperationID, "op_upload_0001")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || !bytes.Contains(response.Body.Bytes(), []byte(`"stage":"metadata"`)) {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestUploadHTTPAuthenticatesAndAdmitsBeforeReading(t *testing.T) {
	handler, _ := uploadTestHandler(t, func(context.Context, protocol.Frame) (Authorization, error) {
		return Authorization{}, errors.New("revoked")
	})
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	request := multipartUpload(t, "op_upload_0001", "diagram.png", "image/png", png, false)
	probe := &readProbe{}
	request.Body = probe
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound || probe.reads != 0 {
		t.Fatalf("status=%d reads=%d", response.Code, probe.reads)
	}
}
