package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
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
		if payload["action"] != "create" || payload["logical_name"] != "web" || payload["target_port"] != float64(3000) || payload["public_acknowledgement"] != true {
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
	if code := run([]string{"preview", "create", "--name", "web", "--port", "3000", "--public"}, &stdout, &stderr); code != 0 {
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
