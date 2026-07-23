package service

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type controller struct {
	applied   []bool
	removed   int
	applyErr  error
	removeErr error
}

func (c *controller) Apply(_ context.Context, _ string, upgrading bool) error {
	c.applied = append(c.applied, upgrading)
	return c.applyErr
}
func (c *controller) Remove(context.Context, string) error { c.removed++; return c.removeErr }

func executable(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "paperboat-helper")
	if err := os.WriteFile(path, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestSystemdInstallUpgradeAndUninstallAreDeterministic(t *testing.T) {
	control := &controller{}
	installer, err := New(Config{Platform: "linux", ConfigRoot: t.TempDir(), Executable: executable(t), Arguments: []string{"run", "--state", "/var/lib/paperboat"}, Environment: map[string]string{"HOME": "/home/test", "PATH": "/usr/bin:/bin"}, Controller: control})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(installer.DefinitionPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(first), "ExecStart=") || !strings.Contains(string(first), "NoNewPrivileges=true") || strings.Contains(string(first), "ProtectHome") {
		t.Fatalf("definition=%s", first)
	}
	info, _ := os.Stat(installer.DefinitionPath())
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(installer.DefinitionPath())
	if string(first) != string(second) || len(control.applied) != 2 || control.applied[0] || !control.applied[1] {
		t.Fatalf("applied=%v", control.applied)
	}
	if err := installer.Uninstall(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(installer.DefinitionPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("definition remains: %v", err)
	}
}

func TestServiceDefinitionsSafelyPreservePathsWithSpaces(t *testing.T) {
	control := &controller{}
	executable := executable(t)
	installer, err := New(Config{Platform: "linux", ConfigRoot: t.TempDir(), Executable: executable, Arguments: []string{"run", "--state", "/home/test/Application Support/Paperboat"}, Environment: map[string]string{"HOME": "/home/test/Application Support"}, Controller: control})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	definition, err := os.ReadFile(installer.DefinitionPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(definition), `Environment="HOME=/home/test/Application Support"`) || !strings.Contains(string(definition), `"/home/test/Application Support/Paperboat"`) {
		t.Fatalf("definition did not quote spaced values: %s", definition)
	}
}

func TestServiceDefinitionRejectsControlCharacters(t *testing.T) {
	_, err := New(Config{Platform: "linux", ConfigRoot: t.TempDir(), Executable: executable(t), Arguments: []string{"run\nExecStart=/tmp/other"}, Environment: map[string]string{"HOME": "/home/test"}, Controller: &controller{}})
	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("error=%v", err)
	}
}

func TestLaunchdDefinitionIsEscapedValidXML(t *testing.T) {
	root := t.TempDir()
	installer, err := New(Config{Platform: "darwin", ConfigRoot: root, Executable: executable(t), Arguments: []string{"run"}, Environment: map[string]string{"HOME": "/Users/test"}, Controller: &controller{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(installer.DefinitionPath())
	decoder := xml.NewDecoder(strings.NewReader(string(data)))
	for {
		if _, err := decoder.Token(); err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			break
		}
	}
	if !strings.Contains(string(data), "<key>ProgramArguments</key>") || !strings.Contains(string(data), Label) {
		t.Fatalf("plist=%s", data)
	}
	if info, err := os.Stat(filepath.Join(root, "Library", "Logs", "Paperboat")); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o700 {
		t.Fatalf("logs mode=%v", info.Mode().Perm())
	}
}

func TestControllerFailureIsNotReportedAsSuccess(t *testing.T) {
	control := &controller{applyErr: errors.New("manager failed")}
	installer, err := New(Config{Platform: "linux", ConfigRoot: t.TempDir(), Executable: executable(t), Arguments: []string{"run"}, Controller: control})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); !errors.Is(err, control.applyErr) {
		t.Fatalf("install err=%v", err)
	}
	control.applyErr = nil
	if err := installer.Install(context.Background()); err != nil {
		t.Fatalf("install retry: %v", err)
	}
	if len(control.applied) != 2 || control.applied[0] || !control.applied[1] {
		t.Fatalf("apply modes=%v", control.applied)
	}
	control.removeErr = errors.New("stop failed")
	if err := installer.Uninstall(context.Background()); !errors.Is(err, control.removeErr) {
		t.Fatalf("uninstall err=%v", err)
	}
	if _, err := os.Stat(installer.DefinitionPath()); err != nil {
		t.Fatalf("definition removed after controller failure: %v", err)
	}
}

func TestDefinitionQuotesExecutablePathWithSpaces(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "with space")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "paperboat-helper")
	if err := os.WriteFile(path, []byte("binary"), 0o700); err != nil {
		t.Fatal(err)
	}
	installer, err := New(Config{Platform: "linux", ConfigRoot: t.TempDir(), Executable: path, Arguments: []string{"run"}, Controller: &controller{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	definition, err := os.ReadFile(installer.DefinitionPath())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(definition), `ExecStart="`+path+`" "run"`) {
		t.Fatalf("definition=%s", definition)
	}
}

func TestInstallDoesNotRewriteExistingConfigRootPermissions(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o750); err != nil {
		t.Fatal(err)
	}
	installer, err := New(Config{Platform: "linux", ConfigRoot: root, Executable: executable(t), Arguments: []string{"run"}, Controller: &controller{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil || info.Mode().Perm() != 0o750 {
		t.Fatalf("mode=%v err=%v", info.Mode().Perm(), err)
	}
}

func TestInstallSecuresExistingServiceDefinitionDirectory(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "systemd", "user")
	if err := os.MkdirAll(directory, 0o775); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o775); err != nil {
		t.Fatal(err)
	}
	installer, err := New(Config{Platform: "linux", ConfigRoot: root, Executable: executable(t), Arguments: []string{"run"}, Controller: &controller{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := installer.Install(context.Background()); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o700 {
		t.Fatalf("service definition directory mode = %o, want 700", got)
	}
}

type commandRunner struct {
	calls [][]string
	errAt int
}

func (r *commandRunner) Run(_ context.Context, name string, args ...string) error {
	call := append([]string{name}, args...)
	r.calls = append(r.calls, call)
	if r.errAt == len(r.calls) {
		return errors.New("command failed")
	}
	return nil
}

func TestNativeControllerCommandSequences(t *testing.T) {
	runner := &commandRunner{}
	systemd := SystemdController{Runner: runner}
	if err := systemd.Apply(context.Background(), "", false); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 3 || strings.Join(runner.calls[1], " ") != "systemctl --user enable --now paperboat-helper.service" || strings.Join(runner.calls[2], " ") != "systemctl --user is-active --quiet paperboat-helper.service" {
		t.Fatalf("calls=%v", runner.calls)
	}
	runner = &commandRunner{}
	systemd.Runner = runner
	if err := systemd.Apply(context.Background(), "", true); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 4 || strings.Join(runner.calls[2], " ") != "systemctl --user restart paperboat-helper.service" || strings.Join(runner.calls[3], " ") != "systemctl --user is-active --quiet paperboat-helper.service" {
		t.Fatalf("upgrade calls=%v", runner.calls)
	}
	runner = &commandRunner{}
	launchd := LaunchdController{Runner: runner, UID: 501}
	if err := launchd.Apply(context.Background(), "/tmp/helper.plist", true); err != nil {
		t.Fatal(err)
	}
	if len(runner.calls) != 4 || strings.Join(runner.calls[0], " ") != "launchctl bootout gui/501/"+Label || strings.Join(runner.calls[3], " ") != "launchctl print gui/501/"+Label {
		t.Fatalf("calls=%v", runner.calls)
	}
	runner = &commandRunner{errAt: 3}
	if err := (SystemdController{Runner: runner}).Apply(context.Background(), "", false); err == nil {
		t.Fatal("inactive systemd service reported success")
	}
	runner = &commandRunner{errAt: 3}
	if err := (LaunchdController{Runner: runner, UID: 501}).Apply(context.Background(), "/tmp/helper.plist", false); err == nil {
		t.Fatal("inactive launchd service reported success")
	}
}

func TestExecRunnerReturnsBoundedNativeDiagnostics(t *testing.T) {
	err := (ExecRunner{}).Run(context.Background(), "/bin/sh", "-c", "printf native-diagnostic >&2; exit 7")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Tool != "/bin/sh" || !strings.Contains(commandErr.Output, "native-diagnostic") {
		t.Fatalf("err=%v", err)
	}
	output := &boundedCommandOutput{limit: 8}
	data := []byte("0123456789abcdef")
	if written, err := output.Write(data); err != nil || written != len(data) || output.String() != "01234567" {
		t.Fatalf("written=%d output=%q err=%v", written, output.String(), err)
	}
}
