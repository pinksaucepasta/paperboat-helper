package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	for _, argument := range []string{"version", "--version", "-v"} {
		var stdout, stderr bytes.Buffer
		if code := run([]string{argument}, &stdout, &stderr); code != 0 {
			t.Fatalf("run %s exit code = %d, want 0; stderr = %q", argument, code, stderr.String())
		}
		if !strings.HasPrefix(stdout.String(), "pbh ") {
			t.Fatalf("%s version output = %q", argument, stdout.String())
		}
	}
}

func TestPreviewCreatePrintsPublicURLAndAcknowledgement(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer local-agent-token-01234567890123456789" {
			t.Errorf("authorization=%q", request.Header.Get("Authorization"))
		}
		var payload map[string]any
		_ = json.NewDecoder(request.Body).Decode(&payload)
		if payload["action"] != "create" || payload["logical_name"] != "web" || payload["target_port"] != float64(3000) || payload["public_acknowledgement"] != true || payload["duration_seconds"] != float64(7200) {
			t.Errorf("payload=%v", payload)
		}
		_, _ = writer.Write([]byte(`{"data":{"id":"prv_1","environment_id":"env_1","logical_name":"web","preview_key":"p-abcdefghijklmnopqrstuvwxyz","url":"https://p-abcdefghijklmnopqrstuvwxyz.preview.test","target_port":3000,"state":"registering"}}`))
	}))
	defer server.Close()
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("local-agent-token-01234567890123456789\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PAPERBOAT_PREVIEW_REGISTRATION_ENDPOINT", server.URL+"/v1/preview-registrations")
	t.Setenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE", tokenFile)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"preview", "create", "--name", "web", "--port", "3000", "--public", "--duration", "2h"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "https://p-abcdefghijklmnopqrstuvwxyz.preview.test") || !strings.Contains(stdout.String(), "anyone with this URL can access it") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestPreviewRejectsInsecureTokenFile(t *testing.T) {
	tokenFile := t.TempDir() + "/token"
	if err := os.WriteFile(tokenFile, []byte("local-agent-token-01234567890123456789\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PAPERBOAT_PREVIEW_REGISTRATION_ENDPOINT", "http://127.0.0.1:38080/v1/preview-registrations")
	t.Setenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE", tokenFile)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"preview", "list"}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "authorization is unavailable") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestBootstrapPromptsForDashboardCommandInputs(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("Studio machine\n"))
	var output bytes.Buffer
	name := ""
	if err := promptBootstrapValue(reader, &output, "Machine name", &name); err != nil {
		t.Fatal(err)
	}
	if name != "Studio machine" {
		t.Fatalf("name=%q", name)
	}
	if output.String() != "Machine name: " {
		t.Fatalf("prompts=%q", output.String())
	}
}

func TestBootstrapFlagsDoNotConsumePromptInput(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("unused\n"))
	var output bytes.Buffer
	name := "Studio"
	if err := promptBootstrapValue(reader, &output, "Machine name", &name); err != nil {
		t.Fatal(err)
	}
	if name != "Studio" || output.Len() != 0 {
		t.Fatalf("name=%q prompts=%q", name, output.String())
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve"}, &stdout, &stderr); code != 2 {
		t.Fatalf("run unknown exit code = %d, want 2", code)
	}
	if got := stderr.String(); got != "pbh: unknown command \"serve\"\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestRemovedTransitionalCommandsAreUnknown(t *testing.T) {
	for _, arguments := range [][]string{{"enroll", "/tmp/config"}} {
		var stdout, stderr bytes.Buffer
		if code := run(arguments, &stdout, &stderr); code != 2 {
			t.Fatalf("arguments=%v code=%d stderr=%q", arguments, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "unknown command") {
			t.Fatalf("stderr=%q", stderr.String())
		}
	}
}

func TestHelpAdvertisesOnlyProductionEnrollmentPath(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if strings.Contains(stdout.String(), "helper enroll") || !strings.Contains(stdout.String(), "pbh run") || !strings.Contains(stdout.String(), "dashboard-started enrollment") {
		t.Fatalf("help=%q", stdout.String())
	}
}

func TestSendRejectsInvalidInvocationWithExitTwo(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"send"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "one through ten") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestSendEmptyFileReportsJSONReceipt(t *testing.T) {
	const token = "local-agent-token-01234567890123456789"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer "+token {
			t.Errorf("authorization=%q", request.Header.Get("Authorization"))
		}
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/file-transfers":
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte(`{"batch_id":"send_batch","transfers":[{"transfer_id":"ft_empty","batch_id":"send_batch","direction":"pbh_to_pb","session_id":"ses_1","basename":"empty.bin","size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","committed_offset":0,"state":"created","created_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:10:00Z"}]}`))
		case request.Method == http.MethodHead && request.URL.Path == "/v1/file-transfers/ft_empty/content":
			writer.Header().Set("Upload-Offset", "0")
			writer.WriteHeader(http.StatusNoContent)
		case request.Method == http.MethodPost && request.URL.Path == "/v1/file-transfers/ft_empty/complete":
			_, _ = writer.Write([]byte(`{"transfer":{"transfer_id":"ft_empty","batch_id":"send_batch","direction":"pbh_to_pb","session_id":"ses_1","basename":"empty.bin","size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","committed_offset":0,"state":"pending","created_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:10:00Z"},"result":{"code":"pending"}}`))
		case request.Method == http.MethodGet && request.URL.Path == "/v1/file-transfers/ft_empty":
			_, _ = writer.Write([]byte(`{"transfer_id":"ft_empty","batch_id":"send_batch","direction":"pbh_to_pb","session_id":"ses_1","basename":"empty.bin","size":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","committed_offset":0,"state":"delivered","result_code":"stored","receipt_path":"Paperboat Inbox/empty.bin","created_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:10:00Z"}`))
		default:
			http.Error(writer, "unexpected", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	workspace := t.TempDir()
	if err := os.WriteFile(workspace+"/empty.bin", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	tokenFile := workspace + "/token"
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous, _ := os.Getwd()
	if err := os.Chdir(workspace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
	t.Setenv("PAPERBOAT_FILE_TRANSFER_ENDPOINT", server.URL+"/v1/file-transfers")
	t.Setenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE", tokenFile)
	t.Setenv("PAPERBOAT_WORKSPACE_ROOT", workspace)
	t.Setenv("PAPERBOAT_TERMINAL_SESSION_ID", "ses_1")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"send", "--json", "empty.bin"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var output struct {
		Files []sendResult `json:"files"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || len(output.Files) != 1 || output.Files[0].Path != "Paperboat Inbox/empty.bin" {
		t.Fatalf("output=%q parsed=%#v err=%v", stdout.String(), output, err)
	}
}

func TestSendReportsEveryFileWhenCreateFails(t *testing.T) {
	const token = "local-agent-token-01234567890123456789"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusTooManyRequests)
		_, _ = writer.Write([]byte(`{"code":"resource_limit","message":"busy"}`))
	}))
	defer server.Close()
	workspace := t.TempDir()
	for _, name := range []string{"first.txt", "second.bin"} {
		if err := os.WriteFile(filepath.Join(workspace, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tokenFile := filepath.Join(workspace, "token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	previous, _ := os.Getwd()
	if err := os.Chdir(workspace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(previous) })
	t.Setenv("PAPERBOAT_FILE_TRANSFER_ENDPOINT", server.URL+"/v1/file-transfers")
	t.Setenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE", tokenFile)
	t.Setenv("PAPERBOAT_WORKSPACE_ROOT", workspace)
	t.Setenv("PAPERBOAT_TERMINAL_SESSION_ID", "ses_1")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"send", "--json", "first.txt", "second.bin"}, &stdout, &stderr); code != 1 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var output struct {
		Files []sendResult `json:"files"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil || len(output.Files) != 2 {
		t.Fatalf("output=%q parsed=%#v err=%v", stdout.String(), output, err)
	}
	for _, result := range output.Files {
		if result.State != "failed" || result.Code != "resource_limit" {
			t.Fatalf("result=%#v", result)
		}
	}
}
