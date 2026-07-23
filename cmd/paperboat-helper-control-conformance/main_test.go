package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRequiresPrivateCompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{"action":"enroll","control_url":"https://control.test","control_ca_file":"/tmp/ca.pem","state_root":"/tmp/helper-state","issuer":"https://control.test","enrollment_credential":"credential-0123456789012345678901"}`
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("public config accepted")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"action":"enroll","control_url":"https://control.test","control_ca_file":"/tmp/ca.pem","state_root":"/tmp/helper-state","issuer":"https://control.test","enrollment_credential":"short"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("short enrollment credential accepted")
	}
}
