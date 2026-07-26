package enrollment

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/identity"
)

var (
	ErrInvalid                        = errors.New("invalid helper enrollment")
	ErrFlyWorkloadIdentityUnavailable = errors.New("Fly workload identity is unavailable")
	ErrEnrollmentExchangeRejected     = errors.New("helper enrollment exchange was rejected")
)

type Config struct {
	ControlURL           string `json:"control_url"`
	ControlCAFile        string `json:"control_ca_file,omitempty"`
	StateRoot            string `json:"state_root"`
	EnrollmentCredential string `json:"enrollment_credential"`
}

type RuntimeIdentity struct {
	Version       int       `json:"version"`
	HelperID      string    `json:"helper_id"`
	EnvironmentID string    `json:"environment_id"`
	Credential    string    `json:"credential"`
	ExpiresAt     time.Time `json:"expires_at"`
	KeyID         string    `json:"key_id"`
}

type HostedBootstrap struct {
	SetupScriptRef    string     `json:"setup_script_ref"`
	SetupScript       string     `json:"setup_script"`
	SetupScriptSHA256 string     `json:"setup_script_sha256"`
	SourceUsername    string     `json:"source_username,omitempty"`
	SourcePassword    string     `json:"source_password,omitempty"`
	SourceExpiresAt   *time.Time `json:"source_expires_at,omitempty"`
}

type Client struct {
	transport http.RoundTripper
	timeout   time.Duration
}

type TokenSource struct {
	StateRoot string
	Clock     func() time.Time
}

type ProofSource struct {
	StateRoot string
	Clock     func() time.Time
}

func (s ProofSource) Proof(_ context.Context, operationID, method, path string, body []byte) ([]byte, error) {
	return s.proof(operationID, method, path, body, false)
}

func (s ProofSource) renewalProof(operationID, method, path string, body []byte) ([]byte, error) {
	return s.proof(operationID, method, path, body, true)
}

func (s ProofSource) proof(operationID, method, path string, body []byte, allowExpiredIdentity bool) ([]byte, error) {
	now := time.Now().UTC()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	var value RuntimeIdentity
	var err error
	if allowExpiredIdentity {
		value, err = LoadRuntimeIdentityForRenewal(s.StateRoot, now)
	} else {
		value, err = LoadRuntimeIdentity(s.StateRoot, now)
	}
	if err != nil || len(operationID) < 8 || len(operationID) > 128 || method != http.MethodPost || path == "" || len(body) > 1<<20 {
		return nil, ErrInvalid
	}
	store, err := identity.Open(identity.Config{StateRoot: s.StateRoot})
	if err != nil || store.Current().ID != value.KeyID {
		return nil, ErrInvalid
	}
	bodyHash := sha256.Sum256(body)
	claims := struct {
		HelperID      string    `json:"helper_id"`
		EnvironmentID string    `json:"environment_id"`
		OperationID   string    `json:"operation_id"`
		Method        string    `json:"method"`
		Path          string    `json:"path"`
		BodySHA256    string    `json:"body_sha256"`
		IssuedAt      time.Time `json:"issued_at"`
		ExpiresAt     time.Time `json:"expires_at"`
	}{value.HelperID, value.EnvironmentID, operationID, method, path, base64.RawURLEncoding.EncodeToString(bodyHash[:]), now, now.Add(time.Minute)}
	payload, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}
	key := store.Current()
	return json.Marshal(struct {
		Algorithm string `json:"alg"`
		Payload   string `json:"payload"`
		Signature string `json:"signature"`
	}{"EdDSA", base64.RawURLEncoding.EncodeToString(payload), base64.RawURLEncoding.EncodeToString(key.Sign(payload))})
}

func (s TokenSource) Token(context.Context) (string, error) {
	now := time.Now().UTC()
	if s.Clock != nil {
		now = s.Clock().UTC()
	}
	value, err := LoadRuntimeIdentity(s.StateRoot, now)
	if err != nil {
		return "", err
	}
	return value.Credential, nil
}

func NewClient(transport http.RoundTripper, timeout time.Duration) (*Client, error) {
	if timeout <= 0 || timeout > 30*time.Second {
		return nil, ErrInvalid
	}
	return &Client{transport: transport, timeout: timeout}, nil
}

func LoadConfig(path string) (Config, error) {
	if !filepath.IsAbs(path) {
		return Config{}, ErrInvalid
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 32<<10 {
		return Config{}, ErrInvalid
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if strictJSON(body, &config) != nil || !filepath.IsAbs(config.StateRoot) || config.ControlCAFile != "" && !filepath.IsAbs(config.ControlCAFile) || len(config.EnrollmentCredential) < 32 || len(config.EnrollmentCredential) > 16<<10 {
		return Config{}, ErrInvalid
	}
	return config, nil
}

func (c *Client) Enroll(ctx context.Context, config Config) (RuntimeIdentity, error) {
	if len(config.EnrollmentCredential) < 32 || len(config.EnrollmentCredential) > 16<<10 {
		return RuntimeIdentity{}, ErrInvalid
	}
	return c.enroll(ctx, config, "/v1/helper-enrollments", struct {
		Credential string `json:"credential"`
		PublicKey  string `json:"public_key"`
	}{Credential: config.EnrollmentCredential})
}

func (c *Client) EnrollHosted(ctx context.Context, config Config) (RuntimeIdentity, error) {
	base, err := validControlURL(config.ControlURL)
	if err != nil {
		return RuntimeIdentity{}, err
	}
	audience := strings.TrimRight(base.String(), "/") + "/v1/hosted-helper-enrollments"
	workloadIdentity, err := requestFlyWorkloadIdentity(ctx, audience, c.timeout)
	if err != nil {
		return RuntimeIdentity{}, err
	}
	return c.enroll(ctx, config, "/v1/hosted-helper-enrollments", struct {
		WorkloadIdentity string `json:"workload_identity"`
		PublicKey        string `json:"public_key"`
	}{WorkloadIdentity: workloadIdentity})
}

func (c *Client) HostedBootstrap(ctx context.Context, config Config) (HostedBootstrap, error) {
	base, err := validControlURL(config.ControlURL)
	if err != nil {
		return HostedBootstrap{}, err
	}
	identity, err := LoadRuntimeIdentity(config.StateRoot, time.Now().UTC())
	if err != nil {
		return HostedBootstrap{}, err
	}
	body := []byte("{}")
	var operationBytes [16]byte
	if _, err = rand.Read(operationBytes[:]); err != nil {
		return HostedBootstrap{}, err
	}
	path := "/v1/hosted-helper-bootstrap"
	proof, err := (ProofSource{StateRoot: config.StateRoot}).Proof(
		ctx, "hosted-bootstrap-"+base64.RawURLEncoding.EncodeToString(operationBytes[:]),
		http.MethodPost, path, body,
	)
	if err != nil {
		return HostedBootstrap{}, err
	}
	base.Path = strings.TrimRight(base.Path, "/") + path
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, base.String(), bytes.NewReader(body))
	if err != nil {
		return HostedBootstrap{}, err
	}
	request.Header.Set("Authorization", "Bearer "+identity.Credential)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	transport := c.transport
	if transport == nil {
		transport, err = controlTransport(config.ControlCAFile)
		if err != nil {
			return HostedBootstrap{}, err
		}
	}
	client := &http.Client{
		Transport: transport, Timeout: c.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return ErrInvalid },
	}
	response, err := client.Do(request)
	if err != nil {
		return HostedBootstrap{}, ErrInvalid
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 96<<10+1))
	if err != nil || response.StatusCode != http.StatusOK || len(responseBody) > 96<<10 {
		return HostedBootstrap{}, ErrInvalid
	}
	var envelope struct {
		Data HostedBootstrap `json:"data"`
	}
	if strictJSON(responseBody, &envelope) != nil {
		return HostedBootstrap{}, ErrInvalid
	}
	result := envelope.Data
	if result.SourcePassword == "" {
		if result.SourceUsername != "" || result.SourceExpiresAt != nil {
			return HostedBootstrap{}, ErrInvalid
		}
	} else if result.SourceUsername != "x-access-token" || result.SourceExpiresAt == nil ||
		!result.SourceExpiresAt.After(time.Now().UTC().Add(30*time.Second)) {
		return HostedBootstrap{}, ErrInvalid
	}
	if result.SetupScriptRef == "" {
		if result.SetupScript != "" || result.SetupScriptSHA256 != "" {
			return HostedBootstrap{}, ErrInvalid
		}
		return result, nil
	}
	if len(result.SetupScript) > 64<<10 || len(result.SetupScriptSHA256) != 64 {
		return HostedBootstrap{}, ErrInvalid
	}
	digest := sha256.Sum256([]byte(result.SetupScript))
	if result.SetupScriptSHA256 != hex.EncodeToString(digest[:]) {
		return HostedBootstrap{}, ErrInvalid
	}
	return result, nil
}

func (c *Client) enroll(ctx context.Context, config Config, endpointPath string, payload any) (RuntimeIdentity, error) {
	base, err := url.Parse(config.ControlURL)
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() == "" || base.RawQuery != "" || base.Fragment != "" {
		return RuntimeIdentity{}, ErrInvalid
	}
	base.Path = strings.TrimRight(base.Path, "/") + endpointPath
	store, err := identity.Open(identity.Config{StateRoot: config.StateRoot})
	if err != nil {
		return RuntimeIdentity{}, err
	}
	key := store.Current()
	switch value := payload.(type) {
	case struct {
		Credential string `json:"credential"`
		PublicKey  string `json:"public_key"`
	}:
		value.PublicKey = base64.RawURLEncoding.EncodeToString(key.Public())
		payload = value
	case struct {
		WorkloadIdentity string `json:"workload_identity"`
		PublicKey        string `json:"public_key"`
	}:
		value.PublicKey = base64.RawURLEncoding.EncodeToString(key.Public())
		payload = value
	default:
		return RuntimeIdentity{}, ErrInvalid
	}
	body, _ := json.Marshal(payload)
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, base.String(), bytes.NewReader(body))
	if err != nil {
		return RuntimeIdentity{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	transport := c.transport
	if transport == nil {
		transport, err = controlTransport(config.ControlCAFile)
		if err != nil {
			return RuntimeIdentity{}, err
		}
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrInvalid }}
	response, err := client.Do(request)
	if err != nil {
		return RuntimeIdentity{}, errors.Join(ErrInvalid, ErrEnrollmentExchangeRejected)
	}
	defer response.Body.Close()
	limited := io.LimitReader(response.Body, 64<<10+1)
	responseBody, readErr := io.ReadAll(limited)
	if readErr != nil {
		return RuntimeIdentity{}, readErr
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || len(responseBody) > 64<<10 {
		return RuntimeIdentity{}, errors.Join(ErrInvalid, ErrEnrollmentExchangeRejected)
	}
	var envelope struct {
		Data RuntimeIdentity `json:"data"`
	}
	if strictJSON(responseBody, &envelope) != nil {
		return RuntimeIdentity{}, ErrInvalid
	}
	result := envelope.Data
	result.Version = 1
	result.KeyID = key.ID
	if result.HelperID == "" || result.EnvironmentID == "" || len(result.Credential) < 32 || result.ExpiresAt.Before(time.Now().UTC()) {
		return RuntimeIdentity{}, ErrInvalid
	}
	if err := writeIdentity(config.StateRoot, result); err != nil {
		return RuntimeIdentity{}, err
	}
	return result, nil
}

func validControlURL(raw string) (*url.URL, error) {
	base, err := url.Parse(raw)
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() == "" ||
		base.RawQuery != "" || base.Fragment != "" {
		return nil, ErrInvalid
	}
	return base, nil
}

func requestFlyWorkloadIdentity(ctx context.Context, audience string, timeout time.Duration) (string, error) {
	return requestFlyWorkloadIdentityAt(ctx, audience, timeout, "/.fly/api")
}

func requestFlyWorkloadIdentityAt(ctx context.Context, audience string, timeout time.Duration, socketPath string) (string, error) {
	if audience == "" || timeout <= 0 {
		return "", ErrInvalid
	}
	if !filepath.IsAbs(socketPath) {
		return "", ErrInvalid
	}
	body, err := json.Marshal(struct {
		Audience string `json:"aud"`
	}{Audience: audience})
	if err != nil {
		return "", err
	}
	requestCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, "http://localhost/v1/tokens/oidc", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
		},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return ErrInvalid },
	}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.Join(ErrInvalid, ErrFlyWorkloadIdentityUnavailable)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 32<<10+1))
	token := strings.TrimSpace(string(responseBody))
	if err != nil || response.StatusCode != http.StatusOK || len(token) < 32 || len(token) > 32<<10 {
		return "", errors.Join(ErrInvalid, ErrFlyWorkloadIdentityUnavailable)
	}
	return token, nil
}

func controlTransport(caPath string) (http.RoundTripper, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	if caPath != "" {
		info, err := os.Lstat(caPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() < 1 || info.Size() > 1<<20 {
			return nil, ErrInvalid
		}
		encoded, err := os.ReadFile(caPath)
		if err != nil {
			return nil, err
		}
		roots, err := x509.SystemCertPool()
		if err != nil || roots == nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(encoded) {
			return nil, ErrInvalid
		}
		tlsConfig.RootCAs = roots
	}
	return &http.Transport{TLSClientConfig: tlsConfig}, nil
}

func writeIdentity(root string, value RuntimeIdentity) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(root, ".runtime-identity-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(path, filepath.Join(root, "runtime-identity.json")); err != nil {
		return err
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func LoadRuntimeIdentity(root string, now time.Time) (RuntimeIdentity, error) {
	return loadRuntimeIdentity(root, now, 0)
}

func LoadRuntimeIdentityForRenewal(root string, now time.Time) (RuntimeIdentity, error) {
	return loadRuntimeIdentity(root, now, -1)
}

func loadRuntimeIdentity(root string, now time.Time, expiryGrace time.Duration) (RuntimeIdentity, error) {
	if !filepath.IsAbs(root) || now.IsZero() {
		return RuntimeIdentity{}, ErrInvalid
	}
	path := filepath.Join(root, "runtime-identity.json")
	info, err := os.Lstat(path)
	if err != nil {
		return RuntimeIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || stat.Nlink != 1 || info.Size() > 32<<10 {
		return RuntimeIdentity{}, ErrInvalid
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return RuntimeIdentity{}, err
	}
	var value RuntimeIdentity
	if strictJSON(body, &value) != nil {
		return RuntimeIdentity{}, ErrInvalid
	}
	validExpiry := expiryGrace < 0 || value.ExpiresAt.After(now.Add(-expiryGrace))
	if value.Version != 1 || value.HelperID == "" || value.EnvironmentID == "" || len(value.Credential) < 32 || !validExpiry || value.KeyID == "" {
		return RuntimeIdentity{}, ErrInvalid
	}
	store, err := identity.Open(identity.Config{StateRoot: root})
	if err != nil || store.Current().ID != value.KeyID {
		return RuntimeIdentity{}, ErrInvalid
	}
	return value, nil
}

func strictJSON(data []byte, target any) error {
	if err := rejectDuplicateJSON(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrInvalid
	}
	return nil
}

func rejectDuplicateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok {
					return ErrInvalid
				}
				if _, exists := seen[key]; exists {
					return ErrInvalid
				}
				seen[key] = struct{}{}
				if err := walk(); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
		default:
			return ErrInvalid
		}
		_, err = decoder.Token()
		return err
	}
	if err := walk(); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrInvalid
	}
	return nil
}
