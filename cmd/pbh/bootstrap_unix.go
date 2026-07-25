//go:build darwin || linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
	"github.com/pinksaucepasta/paperboat-helper/internal/buildinfo"
	"github.com/pinksaucepasta/paperboat-helper/internal/enrollment"
	"github.com/pinksaucepasta/paperboat-helper/internal/service"
)

func runBootstrap(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("bootstrap", flag.ContinueOnError)
	flags.SetOutput(stderr)
	serverURL := flags.String("server", "", "Paperboat server URL")
	token := flags.String("enrollment-token", "", "Dashboard enrollment token")
	name := flags.String("name", "", "User machine name")
	shell := flags.String("shell", "", "Absolute login shell (default: auto-detect)")
	stateRoot := flags.String("state-root", "", "Helper state directory")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("bootstrap accepts flags only")
	}
	reader := bufio.NewReader(stdin)
	if err := promptBootstrapValue(reader, stderr, "User machine name", name); err != nil {
		return err
	}
	resolvedShell, err := resolveUserShell(*shell, os.Getenv)
	if err != nil {
		return err
	}
	workspace, err := canonicalUserHome()
	if err != nil {
		return err
	}
	if *stateRoot == "" {
		root, err := os.UserConfigDir()
		if err != nil {
			return err
		}
		*stateRoot = filepath.Join(root, "paperboat", "helper")
	}
	verifierBytes := make([]byte, 32)
	if _, err := rand.Read(verifierBytes); err != nil {
		return err
	}
	config := bootstrap.Config{ServerURL: *serverURL, EnrollmentToken: *token, DisplayName: *name, WorkspaceRoot: workspace, Verifier: base64.RawURLEncoding.EncodeToString(verifierBytes), RuntimeVersions: map[string]string{"helper": buildinfo.Version}}
	pairing, err := bootstrap.CreatePairing(ctx, config)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "Pairing code: %s\n", pairing.UserCode)
	fmt.Fprintln(stderr, "Waiting for approval in the Paperboat dashboard...")
	material, err := bootstrap.WaitForMaterial(ctx, config, pairing.ExpiresAt, 2*time.Second)
	if err != nil {
		return err
	}
	fmt.Fprintln(stderr, "Pairing approved. Setting up the managed helper service...")
	client, err := enrollment.NewClient(nil, 15*time.Second)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return err
	}
	artifactHTTP := artifactHTTPClient()
	artifactPath, err := prepareInstallation(ctx, &material, *stateRoot, artifactHTTP, client)
	if err != nil {
		if !material.ReuseIdentity {
			return errors.Join(err, reportInstallationFailureWithEnrollmentCredential(ctx, material, "artifact_verification"))
		}
		return err
	}
	executable = artifactPath
	herdrPath, err := installHerdr(ctx, *stateRoot, runtime.GOOS, runtime.GOARCH, herdrHTTPClient())
	if err != nil {
		return errors.Join(err, reportInstallationFailure(ctx, material, *stateRoot, "artifact_verification"))
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return err
	}
	commandDirectory := filepath.Join(home, ".local", "bin")
	if err := installHelperCommand(commandDirectory, executable); err != nil {
		return errors.Join(err, reportInstallationFailure(ctx, material, *stateRoot, "service_install"))
	}
	servicePath := os.Getenv("PATH")
	if !pathListContains(servicePath, commandDirectory) {
		servicePath = commandDirectory + string(os.PathListSeparator) + servicePath
	}
	configRoot := filepath.Join(home, ".config")
	controller := service.Controller(service.SystemdController{Runner: service.ExecRunner{}})
	if runtime.GOOS == "darwin" {
		configRoot, controller = home, service.LaunchdController{Runner: service.ExecRunner{}, UID: os.Getuid()}
	}
	installer, err := service.New(service.Config{Platform: runtime.GOOS, ConfigRoot: configRoot, Executable: executable, Arguments: []string{"run"}, Environment: map[string]string{
		"HOME": home, "PATH": servicePath, "PAPERBOAT_HELPER_PROFILE": "byod", "PAPERBOAT_HELPER_STATE_ROOT": *stateRoot,
		"PAPERBOAT_WORKSPACE_ROOT": workspace, "PAPERBOAT_CONTROL_URL": material.ControlURL, "PAPERBOAT_USER_MACHINE_ID": material.UserMachineID,
		"PAPERBOAT_SHELL":                 resolvedShell,
		"PAPERBOAT_HELPER_LISTEN_ADDRESS": material.HelperListenAddress, "PAPERBOAT_HERDR_PATH": herdrPath, "PAPERBOAT_HERDR_VERSION": herdrVersion,
	}, Controller: controller})
	if err != nil {
		return err
	}
	installCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := installService(installCtx, installer, 3, 2*time.Second); err != nil {
		reportErr := reportInstallationFailure(ctx, material, *stateRoot, "service_install")
		return errors.Join(err, reportErr)
	}
	readyCtx, readyCancel := context.WithTimeout(ctx, 45*time.Second)
	defer readyCancel()
	healthClient := &http.Client{Timeout: 2 * time.Second}
	for {
		request, _ := http.NewRequestWithContext(readyCtx, http.MethodGet, "http://"+material.HelperListenAddress+"/healthz", nil)
		response, requestErr := healthClient.Do(request)
		if requestErr == nil && response.StatusCode == http.StatusOK {
			response.Body.Close()
			fmt.Fprintln(stdout, "Paperboat helper is ready.")
			return nil
		}
		if response != nil {
			response.Body.Close()
		}
		select {
		case <-readyCtx.Done():
			failureErr := errors.New("helper service did not become ready")
			return errors.Join(failureErr, reportInstallationFailure(ctx, material, *stateRoot, "service_readiness"))
		case <-time.After(time.Second):
		}
	}
}

func artifactHTTPClient() *http.Client {
	return &http.Client{Timeout: 2 * time.Minute, CheckRedirect: func(request *http.Request, via []*http.Request) error {
		if len(via) != 1 || request.Method != http.MethodGet || request.URL.Scheme != "https" || request.URL.User != nil ||
			!strings.EqualFold(via[0].URL.Hostname(), "github.com") ||
			!strings.EqualFold(request.URL.Hostname(), "release-assets.githubusercontent.com") {
			return bootstrap.ErrArtifactManifest
		}
		return nil
	}}
}

func canonicalUserHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || !filepath.IsAbs(home) {
		return "", errors.New("could not resolve an absolute user home directory")
	}
	home = filepath.Clean(home)
	resolved, err := filepath.EvalSymlinks(home)
	if err != nil || resolved != home {
		return "", errors.New("user home directory must be canonical and non-symlinked")
	}
	info, err := os.Stat(home)
	if err != nil || !info.IsDir() {
		return "", errors.New("user home directory must exist")
	}
	return home, nil
}

func resolveUserShell(explicit string, getenv func(string) string) (string, error) {
	candidates := []string{strings.TrimSpace(explicit)}
	if candidates[0] == "" && getenv != nil {
		candidates = append(candidates, strings.TrimSpace(getenv("SHELL")))
	}
	if candidates[0] == "" {
		candidates = append(candidates, accountLoginShell())
	}
	if candidates[0] == "" {
		candidates = append(candidates, "/bin/sh")
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		resolved, err := validateUserShell(candidate)
		if err == nil {
			return resolved, nil
		}
		if explicit != "" {
			return "", err
		}
	}
	return "", errors.New("could not detect an executable login shell; use --shell")
}

func validateUserShell(path string) (string, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.New("shell must be an absolute canonical path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("shell must be an executable regular file")
	}
	return path, nil
}

func accountLoginShell() string {
	current, err := user.Current()
	if err != nil || current.Username == "" {
		return ""
	}
	if runtime.GOOS == "darwin" {
		output, err := exec.Command("dscl", ".", "-read", "/Users/"+current.Username, "UserShell").Output()
		if err == nil {
			return strings.TrimSpace(strings.TrimPrefix(string(output), "UserShell:"))
		}
	}
	output, err := exec.Command("getent", "passwd", current.Username).Output()
	if err != nil {
		return ""
	}
	fields := strings.Split(strings.TrimSpace(string(output)), ":")
	if len(fields) != 7 {
		return ""
	}
	return fields[6]
}

func installHelperCommand(directory, artifact string) error {
	if !filepath.IsAbs(directory) || !filepath.IsAbs(artifact) {
		return bootstrap.ErrInvalid
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return bootstrap.ErrInvalid
	}
	temporary := filepath.Join(directory, fmt.Sprintf(".pbh-%d", time.Now().UnixNano()))
	if err := os.Symlink(artifact, temporary); err != nil {
		return err
	}
	defer os.Remove(temporary)
	return os.Rename(temporary, filepath.Join(directory, "pbh"))
}

func pathListContains(value, want string) bool {
	for _, entry := range filepath.SplitList(value) {
		if entry == want {
			return true
		}
	}
	return false
}

func reportInstallationFailure(ctx context.Context, material bootstrap.Material, stateRoot, stage string) error {
	return reportInstallationFailureWithClient(ctx, material, stateRoot, stage, &http.Client{Timeout: 5 * time.Second})
}

func reportInstallationFailureWithClient(ctx context.Context, material bootstrap.Material, stateRoot, stage string, client *http.Client) error {
	identity, err := enrollment.LoadRuntimeIdentity(stateRoot, time.Now().UTC())
	if err != nil {
		return err
	}
	if identity.HelperID != material.HelperID || identity.EnvironmentID != material.EnvironmentID {
		return bootstrap.ErrInvalid
	}
	body, err := json.Marshal(struct {
		EnrollmentID       string `json:"enrollment_id"`
		HelperID           string `json:"helper_id"`
		HelperEnrollmentID string `json:"helper_enrollment_id"`
		Stage              string `json:"stage"`
	}{material.UserMachineEnrollmentID, material.HelperID, material.EnrollmentID, stage})
	if err != nil {
		return err
	}
	operationID := "install-failure-" + material.UserMachineEnrollmentID + "-" + stage
	proof, err := (enrollment.ProofSource{StateRoot: stateRoot}).Proof(ctx, operationID, http.MethodPost, "/v1/user-machine-installation-failures", body)
	if err != nil {
		return err
	}
	base := strings.TrimRight(material.ControlURL, "/") + "/v1/user-machine-installation-failures"
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, base, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+identity.Credential)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("installation failure report returned HTTP %d", response.StatusCode)
	}
	return nil
}

func reportInstallationFailureWithEnrollmentCredential(ctx context.Context, material bootstrap.Material, stage string) error {
	return reportInstallationFailureWithEnrollmentCredentialClient(ctx, material, stage, &http.Client{Timeout: 5 * time.Second})
}

func reportInstallationFailureWithEnrollmentCredentialClient(ctx context.Context, material bootstrap.Material, stage string, client *http.Client) error {
	body, err := json.Marshal(struct {
		EnrollmentID       string `json:"enrollment_id"`
		HelperID           string `json:"helper_id"`
		HelperEnrollmentID string `json:"helper_enrollment_id"`
		Stage              string `json:"stage"`
	}{material.UserMachineEnrollmentID, material.HelperID, material.EnrollmentID, stage})
	if err != nil {
		return err
	}
	base := strings.TrimRight(material.ControlURL, "/") + "/v1/user-machine-installation-failures"
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, base, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+material.EnrollmentCredential)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("installation failure report returned HTTP %d", response.StatusCode)
	}
	return nil
}

type enrollmentClient interface {
	Enroll(context.Context, enrollment.Config) (enrollment.RuntimeIdentity, error)
}

type serviceInstaller interface {
	Install(context.Context) error
}

func installService(ctx context.Context, installer serviceInstaller, attempts int, delay time.Duration) error {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if lastErr = installer.Install(ctx); lastErr == nil {
			return nil
		}
		if attempt+1 == attempts {
			break
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func prepareInstallation(ctx context.Context, material *bootstrap.Material, stateRoot string, artifactHTTP *http.Client, client enrollmentClient) (string, error) {
	if material.ReuseIdentity {
		identity, err := enrollment.LoadRuntimeIdentity(stateRoot, time.Now().UTC())
		if err != nil || identity.HelperID != material.HelperID || identity.EnvironmentID != material.EnvironmentID {
			return "", bootstrap.ErrInvalid
		}
	}
	artifactPath, err := bootstrap.FetchVerifiedArtifact(ctx, *material.Artifact, material.ArtifactPublicKey, filepath.Join(stateRoot, "artifacts"), artifactHTTP)
	if err != nil {
		return "", err
	}
	if material.ReuseIdentity {
		return artifactPath, nil
	}
	if _, err := client.Enroll(ctx, enrollment.Config{ControlURL: material.ControlURL, StateRoot: stateRoot, EnrollmentCredential: material.EnrollmentCredential}); err != nil {
		return "", err
	}
	material.EnrollmentCredential = ""
	return artifactPath, nil
}

func promptBootstrapValue(reader *bufio.Reader, output io.Writer, label string, value *string) error {
	*value = strings.TrimSpace(*value)
	if *value != "" {
		return nil
	}
	fmt.Fprintf(output, "%s: ", label)
	line, err := reader.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	*value = strings.TrimSpace(line)
	if *value == "" {
		return fmt.Errorf("%s is required", strings.ToLower(label))
	}
	return nil
}
