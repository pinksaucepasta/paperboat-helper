package hosted

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestFromEnvLoadsCatalogPresetsAndNamedSetupSecret(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "codex.sh"), []byte("echo codex\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{
		"PAPERBOAT_WORKSPACE": filepath.Join(t.TempDir(), "volume"), "PAPERBOAT_PROJECT_ID": "prj_1",
		"PAPERBOAT_REPOSITORY_URL": "https://github.com/paperboat/example.git", "PAPERBOAT_DEFAULT_BRANCH": "main",
		"PAPERBOAT_PRESET_DIR": dir, "PAPERBOAT_PRESET_CODES": "codex", "PAPERBOAT_SETUP_SCRIPT_ENV": "PAPERBOAT_SETUP_SCRIPT",
		"PAPERBOAT_SETUP_SCRIPT": "echo setup",
	}
	config, err := FromEnv(func(name string) string { return values[name] })
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(config.CheckoutRoot) != "example" || len(config.Presets) != 1 || config.Presets[0].Name != "codex" || config.SetupScript != "echo setup" {
		t.Fatalf("config=%#v", config)
	}
}

func TestFromEnvRejectsPresetSymlinkWritableFileAndSecretIndirection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		prepare  func(string) error
		setupEnv string
	}{
		{name: "symlink", prepare: func(dir string) error {
			target := filepath.Join(dir, "target")
			if err := os.WriteFile(target, []byte("echo x"), 0o644); err != nil {
				return err
			}
			return os.Symlink(target, filepath.Join(dir, "codex.sh"))
		}},
		{name: "writable", prepare: func(dir string) error {
			path := filepath.Join(dir, "codex.sh")
			if err := os.WriteFile(path, []byte("echo x"), 0o644); err != nil {
				return err
			}
			return os.Chmod(path, 0o666)
		}},
		{name: "secret name", prepare: func(dir string) error { return os.WriteFile(filepath.Join(dir, "codex.sh"), []byte("echo x"), 0o644) }, setupEnv: "bad-name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := tc.prepare(dir); err != nil {
				t.Fatal(err)
			}
			values := map[string]string{
				"PAPERBOAT_WORKSPACE": filepath.Join(t.TempDir(), "volume"), "PAPERBOAT_PROJECT_ID": "prj_1", "PAPERBOAT_REPOSITORY_URL": "https://github.com/paperboat/example.git",
				"PAPERBOAT_PRESET_DIR": dir, "PAPERBOAT_PRESET_CODES": "codex", "PAPERBOAT_SETUP_SCRIPT_ENV": tc.setupEnv,
			}
			_, err := FromEnv(func(name string) string { return values[name] })
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
