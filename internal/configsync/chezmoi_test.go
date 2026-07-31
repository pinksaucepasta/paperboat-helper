package configsync

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPlaintextRepositoryRejectsUnsafeChezmoiSource(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "run_once_bad"), []byte("unsafe"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfigRepository(root); err == nil {
		t.Fatal("executable chezmoi source accepted")
	}
}

func TestChezmoiSourceConfiguresPlaintextAndAddWithoutEncryption(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runtimeRoot := filepath.Join(root, "runtime")
	sourceRoot := filepath.Join(root, "source")
	homeRoot := filepath.Join(root, "home")
	for _, path := range []string{runtimeRoot, sourceRoot, homeRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	argumentsPath := filepath.Join(root, "arguments")
	binary := filepath.Join(root, "chezmoi")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > " + argumentsPath + "\n"
	if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	source, err := NewChezmoiSource(ChezmoiSourceConfig{
		Binary: binary, RuntimeRoot: runtimeRoot, SourceRoot: sourceRoot, HomeRoot: homeRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Add(t.Context(), []string{"config.txt"}); err != nil {
		t.Fatal(err)
	}
	config, err := os.ReadFile(filepath.Join(runtimeRoot, "chezmoi.toml"))
	if err != nil || strings.Contains(string(config), "encryption") || strings.Contains(string(config), "age") {
		t.Fatalf("chezmoi config = %q, %v", config, err)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil || strings.Contains(string(arguments), "encrypt") {
		t.Fatalf("chezmoi arguments = %q, %v", arguments, err)
	}
}
