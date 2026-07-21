//go:build darwin || linux

package runtime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
	"github.com/pinksaucepasta/paperboat-helper/internal/enrollment"
	"github.com/pinksaucepasta/paperboat-helper/internal/hosted"
)

var ErrProductionInvalid = errors.New("invalid production helper configuration")

type productionClock struct{}

func (productionClock) Now() time.Time { return time.Now().UTC() }

func NewProductionHelper(ctx context.Context, version string, environ func(string) string) (*Helper, error) {
	if environ == nil {
		return nil, ErrProductionInvalid
	}
	runtimeConfig, err := helperconfig.FromEnv(version, environ)
	if err != nil || runtimeConfig.Profile != helperconfig.Hosted {
		return nil, errors.Join(ErrProductionInvalid, err)
	}
	hostedConfig, err := hosted.FromEnv(environ)
	if err != nil {
		return nil, err
	}
	if setupName := environ("PAPERBOAT_SETUP_SCRIPT_ENV"); setupName != "" && safeProductionEnvironmentName(setupName) {
		_ = os.Unsetenv(setupName)
	}
	if err := materializeConfigIdentity(environ); err != nil {
		return nil, err
	}
	controlURL, err := validatedControlURL(environ("PAPERBOAT_CONTROL_URL"))
	if err != nil {
		return nil, err
	}
	issuer := strings.TrimRight(valueOrRuntime(environ("PAPERBOAT_CONTROL_ISSUER"), controlURL.String()), "/")
	transport, err := productionTransport(environ("PAPERBOAT_CONTROL_CA_FILE"))
	if err != nil {
		return nil, err
	}
	if _, loadErr := enrollment.LoadRuntimeIdentity(runtimeConfig.StateRoot, time.Now().UTC()); loadErr != nil {
		grantName := valueOrRuntime(environ("PAPERBOAT_ENROLLMENT_CREDENTIAL_ENV"), "PAPERBOAT_ENROLLMENT_CREDENTIAL")
		if !safeProductionEnvironmentName(grantName) {
			return nil, ErrProductionInvalid
		}
		grant := environ(grantName)
		if grant == "" {
			return nil, loadErr
		}
		client, clientErr := enrollment.NewClient(transport, 15*time.Second)
		if clientErr != nil {
			return nil, clientErr
		}
		_, err = client.Enroll(ctx, enrollment.Config{ControlURL: controlURL.String(), ControlCAFile: environ("PAPERBOAT_CONTROL_CA_FILE"), StateRoot: runtimeConfig.StateRoot, EnrollmentCredential: grant})
		_ = os.Unsetenv(grantName)
		if err != nil {
			return nil, err
		}
	}
	identity, err := enrollment.LoadRuntimeIdentity(runtimeConfig.StateRoot, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	fetcher, err := auth.NewHTTPJWKSFetcher(controlURL.ResolveReference(&url.URL{Path: "/.well-known/jwks.json"}).String(), []string{controlURL.Hostname()}, transport)
	if err != nil {
		return nil, err
	}
	cache, err := auth.NewJWKSCache(auth.JWKSConfig{Fetcher: fetcher, Clock: productionClock{}, TTL: 5 * time.Minute, RetainMissing: auth.DefaultRetainMissing})
	if err != nil {
		return nil, err
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = cache.Refresh(refreshCtx)
	cancel()
	if err != nil {
		return nil, err
	}
	verifier := auth.Verifier{Keys: cache, Clock: productionClock{}, Replays: auth.NewReplayCache(4096, productionClock{}), ClockSkew: 30 * time.Second, RefreshTimeout: 2 * time.Second}
	authorizer, err := NewCredentialAuthorizer(CredentialAuthConfig{Issuer: issuer, EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, Verifier: verifier})
	if err != nil {
		return nil, err
	}
	operationID := func() (string, error) {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		return "op_admit_" + hex.EncodeToString(bytes), nil
	}
	renewingTokens, err := enrollment.NewRenewingTokenSource(enrollment.RenewingTokenConfig{ControlURL: controlURL.String(), StateRoot: runtimeConfig.StateRoot, Transport: transport, RenewBefore: 10 * time.Minute, Timeout: 15 * time.Second, Clock: func() time.Time { return time.Now().UTC() }, OperationID: operationID})
	if err != nil {
		return nil, err
	}
	source, err := connector.NewHTTPSAdmissionSource(connector.AdmissionSourceConfig{
		Endpoint: controlURL.ResolveReference(&url.URL{Path: "/v1/connectors/admission"}).String(), AllowedHosts: []string{controlURL.Hostname()},
		Tokens: renewingTokens, Proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, Verifier: verifier,
		Clock: productionClock{}, Issuer: issuer, EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, EdgePool: valueOrRuntime(environ("PAPERBOAT_EDGE_POOL"), "default"), OperationID: operationID, Transport: transport,
	})
	if err != nil {
		return nil, err
	}
	dialer, err := connector.NewFRPDialer(connector.FRPDialerConfig{ReadyTimeout: durationRuntime(environ("PAPERBOAT_CONNECTOR_READY_TIMEOUT_SECONDS"), 15*time.Second)})
	if err != nil {
		return nil, err
	}
	manager, err := connector.New(connector.Config{EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, EdgePool: valueOrRuntime(environ("PAPERBOAT_EDGE_POOL"), "default"), Dialer: dialer, DrainTimeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}
	supervisor, err := connector.NewSupervisor(connector.SupervisorConfig{Manager: manager, Admissions: source, InitialBackoff: time.Second, MaxBackoff: time.Minute})
	if err != nil {
		return nil, err
	}
	connectorService := &connectorReadinessService{supervisor: supervisor, manager: manager, timeout: durationRuntime(environ("PAPERBOAT_CONNECTOR_READY_TIMEOUT_SECONDS"), 30*time.Second)}
	collector, err := activity.New(activity.Config{MaxQueued: runtimeConfig.Resources.MaxActivityEvents, MaxDiagnostics: 128})
	if err != nil {
		return nil, err
	}
	var activityDelivery *activity.Delivery
	machineID := valueOrRuntime(environ("FLY_MACHINE_ID"), environ("PAPERBOAT_MACHINE_ID"))
	if machineID == "" {
		return nil, ErrProductionInvalid
	}
	{
		activityEndpoint := controlURL.ResolveReference(&url.URL{Path: "/api/machine/activity-heartbeat"}).String()
		sender := &heartbeatSender{endpoint: activityEndpoint, tokens: renewingTokens, proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, operationID: operationID, projectID: identity.EnvironmentID, machineID: machineID, reporterVersion: version, client: &http.Client{Transport: transport, Timeout: 10 * time.Second}, lastActivity: time.Now().UTC()}
		activityDelivery, err = activity.NewDelivery(activity.DeliveryConfig{Collector: collector, Sender: sender, Interval: runtimeConfig.Limits.HeartbeatInterval, Timeout: 10 * time.Second})
		if err != nil {
			return nil, err
		}
	}
	configSyncHooks := hosted.ConfigSyncHooks(hostedConfig, environ)
	hostedLifecycle, err := hosted.New(hostedConfig, configSyncHooks, nil)
	if err != nil {
		return nil, err
	}
	// Prepare the checkout before constructing the PTY adapter. The adapter
	// validates its root eagerly, while hosted lifecycle owns creating/cloning it.
	if err := hostedLifecycle.Start(ctx); err != nil {
		return nil, err
	}
	if tokenName := environ("PAPERBOAT_GITHUB_TOKEN_ENV"); safeProductionEnvironmentName(tokenName) {
		_ = os.Unsetenv(tokenName)
	}
	listen := valueOrRuntime(environ("PAPERBOAT_HELPER_LISTEN_ADDRESS"), "127.0.0.1:8080")
	shell := valueOrRuntime(environ("PAPERBOAT_SHELL_PATH"), "/bin/bash")
	return NewHelper(ctx, HelperConfig{Runtime: runtimeConfig, ListenAddress: listen, WorkspaceRoot: hostedConfig.VolumeRoot, ShellPath: shell, ShellArgs: []string{"-l"}, ShellEnvironment: []string{"HOME=" + hostedConfig.VolumeRoot, "PATH=" + os.Getenv("PATH")}, EnvironmentID: identity.EnvironmentID, ShutdownTimeout: hostedConfig.FlushTimeout + 15*time.Second}, HelperDependencies{
		Authorizer: authorizer, Connector: connectorService, Activity: collector, ActivityService: activityDelivery, HostedLifecycle: hostedLifecycle,
		ConfigApply: configapply.SyncHandler{Apply: func(ctx context.Context) error { return configSyncHooks.Restore(ctx, hostedConfig.CheckoutRoot) }}, ConfigApplyProof: true,
	})
}

type heartbeatSender struct {
	endpoint, projectID, machineID, reporterVersion string
	tokens                                          interface {
		Token(context.Context) (string, error)
	}
	proofs interface {
		Proof(context.Context, string, string, string, []byte) ([]byte, error)
	}
	operationID  func() (string, error)
	client       *http.Client
	mu           sync.Mutex
	lastActivity time.Time
}

func (s *heartbeatSender) Send(ctx context.Context, batch activity.Batch) error {
	if len(batch.Events) == 0 {
		return errors.New("activity batch is empty")
	}
	return s.send(ctx, batch.Events)
}

func (s *heartbeatSender) Heartbeat(ctx context.Context) error { return s.send(ctx, nil) }

func (s *heartbeatSender) send(ctx context.Context, events []activity.Event) error {
	now := time.Now().UTC()
	signals := make(map[string]string)
	s.mu.Lock()
	for _, event := range events {
		occurred := event.OccurredAt.UTC()
		if occurred.After(s.lastActivity) {
			s.lastActivity = occurred
		}
		key := string(event.Source)
		if previous, ok := signals[key]; !ok || occurred.After(parseActivityTime(previous)) {
			signals[key] = occurred.Format(time.RFC3339Nano)
		}
	}
	last := s.lastActivity
	s.mu.Unlock()
	body, err := json.Marshal(struct {
		ProjectID       string            `json:"project_id"`
		MachineID       string            `json:"machine_id"`
		LastActivityAt  time.Time         `json:"last_activity_at"`
		Signals         map[string]string `json:"signals"`
		ReporterVersion string            `json:"reporter_version"`
		SampledAt       time.Time         `json:"sampled_at"`
	}{s.projectID, s.machineID, last, signals, s.reporterVersion, now})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	token, err := s.tokens.Token(ctx)
	if err != nil {
		return err
	}
	operationID, err := s.operationID()
	if err != nil {
		return err
	}
	proof, err := s.proofs.Proof(ctx, operationID, http.MethodPost, "/api/machine/activity-heartbeat", body)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("activity heartbeat rejected with status %d", response.StatusCode)
	}
	return nil
}

func parseActivityTime(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

type connectorReadinessService struct {
	supervisor *connector.Supervisor
	manager    *connector.Manager
	timeout    time.Duration
}

func (s *connectorReadinessService) Start(ctx context.Context) error {
	if err := s.supervisor.Start(ctx); err != nil {
		return err
	}
	readyCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if s.manager.Status().Connected {
			return nil
		}
		select {
		case <-readyCtx.Done():
			_ = s.supervisor.Shutdown(context.Background())
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}
func (s *connectorReadinessService) Shutdown(ctx context.Context) error {
	return s.supervisor.Shutdown(ctx)
}

func validatedControlURL(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, ErrProductionInvalid
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return parsed, nil
}
func productionTransport(caPath string) (http.RoundTripper, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13}
	if caPath != "" {
		if !filepath.IsAbs(caPath) {
			return nil, ErrProductionInvalid
		}
		info, err := os.Lstat(caPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || info.Size() < 1 || info.Size() > 1<<20 {
			return nil, ErrProductionInvalid
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
			return nil, ErrProductionInvalid
		}
		tlsConfig.RootCAs = roots
	}
	return &http.Transport{TLSClientConfig: tlsConfig}, nil
}
func valueOrRuntime(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
func durationRuntime(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value + "s")
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
func safeProductionEnvironmentName(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if !(r >= 'A' && r <= 'Z' || r == '_' || index > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func materializeConfigIdentity(environ func(string) string) error {
	identity := environ("PAPERBOAT_CONFIG_AGE_IDENTITY")
	if identity == "" {
		return nil
	}
	path := valueOrRuntime(environ("PAPERBOAT_CONFIG_AGE_IDENTITY_FILE"), "/var/lib/paperboat/config-age-identity.txt")
	if !filepath.IsAbs(path) {
		return ErrProductionInvalid
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(path), ".config-identity-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	defer os.Remove(temporary)
	if err = file.Chmod(0o600); err == nil {
		_, err = file.WriteString(identity + "\n")
	}
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return err
	}
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	_ = os.Unsetenv("PAPERBOAT_CONFIG_AGE_IDENTITY")
	return nil
}
