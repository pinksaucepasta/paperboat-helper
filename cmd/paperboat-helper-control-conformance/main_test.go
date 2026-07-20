package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigRequiresPrivateCompleteFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	body := `{"control_url":"https://control.test","control_ca_file":"/tmp/ca.pem","state_root":"/tmp/helper-state","issuer":"https://control.test"}`
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
}
