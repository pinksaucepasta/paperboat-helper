package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

var (
	ErrInvalid                 = errors.New("invalid BYOD bootstrap")
	ErrApprovalPending         = errors.New("BYOD pairing approval is pending")
	ErrPairingDenied           = errors.New("BYOD pairing was denied")
	ErrPairingExpired          = errors.New("BYOD pairing expired")
	ErrInstallationUnavailable = errors.New("BYOD installation material is unavailable")
)

type Config struct {
	ServerURL, EnrollmentToken, DisplayName, WorkspaceRoot, Verifier string
	RuntimeVersions                                                  map[string]string
	HTTP                                                             *http.Client
}

type Pairing struct {
	ID        string    `json:"id"`
	UserCode  string    `json:"user_code"`
	ExpiresAt time.Time `json:"expires_at"`
}

type Material struct {
	Schema               string            `json:"schema"`
	MachineID            string            `json:"machine_id"`
	MachineEnrollmentID  string            `json:"machine_enrollment_id"`
	EnvironmentID        string            `json:"environment_id"`
	ControlURL           string            `json:"control_url"`
	HelperID             string            `json:"helper_id"`
	EnrollmentID         string            `json:"enrollment_id"`
	EnrollmentCredential string            `json:"enrollment_credential"`
	ReuseIdentity        bool              `json:"reuse_identity,omitempty"`
	ExpiresAt            time.Time         `json:"expires_at"`
	Artifact             *ArtifactManifest `json:"artifact,omitempty"`
	ArtifactPublicKey    string            `json:"artifact_public_key,omitempty"`
	HelperListenAddress  string            `json:"helper_listen_address"`
}

func CreatePairing(ctx context.Context, config Config) (Pairing, error) {
	base, err := validate(config)
	if err != nil {
		return Pairing{}, err
	}
	body, err := json.Marshal(map[string]any{
		"enrollment_token": config.EnrollmentToken, "verifier": config.Verifier,
		"display_name": config.DisplayName, "platform": runtime.GOOS, "architecture": runtime.GOARCH,
		"workspace_root": config.WorkspaceRoot, "runtime_versions": config.RuntimeVersions,
	})
	if err != nil {
		return Pairing{}, err
	}
	var pairing Pairing
	if err := request(ctx, client(config), http.MethodPost, base+"/api/connected-machines/pairings", body, &pairing); err != nil {
		return Pairing{}, err
	}
	if pairing.ID == "" || pairing.UserCode == "" || !time.Now().UTC().Before(pairing.ExpiresAt) {
		return Pairing{}, ErrInvalid
	}
	return pairing, nil
}

func WaitForMaterial(ctx context.Context, config Config, expiresAt time.Time, interval time.Duration) (Material, error) {
	base, err := validate(config)
	if err != nil {
		return Material{}, err
	}
	if interval <= 0 {
		interval = 2 * time.Second
	}
	body, _ := json.Marshal(map[string]string{"verifier": config.Verifier})
	for time.Now().UTC().Before(expiresAt) {
		var material Material
		err := request(ctx, client(config), http.MethodPost, base+"/api/connected-machines/pairings/installation", body, &material)
		if err == nil {
			validEnrollment := material.ReuseIdentity && material.EnrollmentID == "" && material.EnrollmentCredential == "" || !material.ReuseIdentity && material.EnrollmentID != "" && len(material.EnrollmentCredential) >= 32
			if material.Schema != "paperboat.byod-installation/v1" || material.MachineID == "" || material.MachineEnrollmentID == "" || material.EnvironmentID == "" || material.HelperID == "" || !validEnrollment || !validLoopbackAddress(material.HelperListenAddress) || !time.Now().UTC().Before(material.ExpiresAt) || material.Artifact == nil || VerifyArtifactManifest(*material.Artifact, material.ArtifactPublicKey) != nil {
				return Material{}, ErrInvalid
			}
			return material, nil
		}
		if !errors.Is(err, ErrApprovalPending) {
			return Material{}, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return Material{}, ctx.Err()
		case <-timer.C:
		}
	}
	return Material{}, ErrPairingExpired
}

func validLoopbackAddress(address string) bool {
	host, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func ValidateWorkspace(root string) error {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return ErrInvalid
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalid
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return ErrInvalid
	}
	return nil
}

func validate(config Config) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.ServerURL))
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || len(config.EnrollmentToken) < 32 || len(config.EnrollmentToken) > 256 || len(config.Verifier) < 32 || strings.TrimSpace(config.DisplayName) == "" || ValidateWorkspace(config.WorkspaceRoot) != nil {
		return "", ErrInvalid
	}
	return strings.TrimRight(parsed.String(), "/"), nil
}

func client(config Config) *http.Client {
	if config.HTTP != nil {
		return config.HTTP
	}
	return &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrInvalid }}
}

func request(ctx context.Context, client *http.Client, method, target string, body []byte, output any) error {
	request, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	if err != nil || len(encoded) > 64<<10 {
		return ErrInvalid
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var envelope struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if json.Unmarshal(encoded, &envelope) != nil {
			return fmt.Errorf("%w: server status %d", ErrInvalid, response.StatusCode)
		}
		switch envelope.Error.Code {
		case "connected_machine_approval_pending":
			return ErrApprovalPending
		case "connected_machine_pairing_denied":
			return ErrPairingDenied
		case "connected_machine_pairing_expired":
			return ErrPairingExpired
		case "connected_machine_installation_unavailable":
			return ErrInstallationUnavailable
		default:
			return fmt.Errorf("%w: server status %d", ErrInvalid, response.StatusCode)
		}
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(encoded, &envelope) != nil || len(envelope.Data) == 0 || json.Unmarshal(envelope.Data, output) != nil {
		return ErrInvalid
	}
	return nil
}
