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
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
	"github.com/pinksaucepasta/paperboat-helper/internal/availability"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/connector"
	"github.com/pinksaucepasta/paperboat-helper/internal/enrollment"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/hosted"
	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
	"github.com/pinksaucepasta/paperboat-helper/internal/observability"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
)

var ErrProductionInvalid = errors.New("invalid production helper configuration")

type productionClock struct{}

func (productionClock) Now() time.Time { return time.Now().UTC() }

func NewProductionHelper(ctx context.Context, version string, environ func(string) string) (*Helper, error) {
	if environ == nil {
		return nil, ErrProductionInvalid
	}
	runtimeConfig, err := helperconfig.FromEnv(version, environ)
	if err != nil {
		return nil, errors.Join(ErrProductionInvalid, err)
	}
	bootState, recoveryExitSignal, err := recordWorkerBoot(runtimeConfig.StateRoot)
	if err != nil {
		return nil, err
	}
	metrics, err := observability.NewRegistry(observability.DefaultDescriptors())
	if err != nil {
		return nil, err
	}
	_ = metrics.Record("paperboat_helper_restart_total", float64(bootState.Generation), nil)
	var hostedConfig hosted.Config
	if runtimeConfig.Profile == helperconfig.Hosted {
		hostedConfig, err = hosted.FromEnv(environ)
		if err != nil {
			return nil, err
		}
		if setupName := environ("PAPERBOAT_SETUP_SCRIPT_ENV"); setupName != "" && safeProductionEnvironmentName(setupName) {
			_ = os.Unsetenv(setupName)
		}
		_ = os.Unsetenv("PAPERBOAT_CONFIG_AGE_IDENTITY")
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
	operationID := func() (string, error) {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return "", err
		}
		return "op_admit_" + hex.EncodeToString(bytes), nil
	}
	renewingTokens, err := enrollment.NewRenewingTokenSource(enrollment.RenewingTokenConfig{ControlURL: controlURL.String(), StateRoot: runtimeConfig.StateRoot, Transport: transport, RenewBefore: 10 * time.Minute, Timeout: 15 * time.Second, Clock: func() time.Time { return time.Now().UTC() }, OperationID: operationID, Metrics: metrics})
	if err != nil {
		return nil, err
	}
	var enrollmentClient *enrollment.Client
	if _, loadErr := enrollment.LoadRuntimeIdentityForRenewal(runtimeConfig.StateRoot, time.Now().UTC()); loadErr != nil {
		var clientErr error
		enrollmentClient, clientErr = enrollment.NewClient(transport, 15*time.Second)
		if clientErr != nil {
			return nil, clientErr
		}
		enrollmentConfig := enrollment.Config{
			ControlURL: controlURL.String(), ControlCAFile: environ("PAPERBOAT_CONTROL_CA_FILE"),
			StateRoot: runtimeConfig.StateRoot,
		}
		if runtimeConfig.Profile == helperconfig.Hosted {
			_, err = retryHostedControl(ctx, func(attemptCtx context.Context) (enrollment.RuntimeIdentity, error) {
				return enrollmentClient.EnrollHosted(attemptCtx, enrollmentConfig)
			})
		} else {
			grantName := valueOrRuntime(environ("PAPERBOAT_ENROLLMENT_CREDENTIAL_ENV"), "PAPERBOAT_ENROLLMENT_CREDENTIAL")
			if !safeProductionEnvironmentName(grantName) {
				return nil, ErrProductionInvalid
			}
			enrollmentConfig.EnrollmentCredential = environ(grantName)
			if enrollmentConfig.EnrollmentCredential == "" {
				return nil, loadErr
			}
			_, err = enrollmentClient.Enroll(ctx, enrollmentConfig)
			_ = os.Unsetenv(grantName)
		}
		if err != nil {
			if runtimeConfig.Profile == helperconfig.Hosted {
				return nil, fmt.Errorf("hosted identity bootstrap: %w", err)
			}
			return nil, err
		}
	}
	identity, err := enrollment.LoadRuntimeIdentityForRenewal(runtimeConfig.StateRoot, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if runtimeConfig.Profile == helperconfig.Hosted {
		if enrollmentClient == nil {
			enrollmentClient, err = enrollment.NewClient(transport, 15*time.Second)
			if err != nil {
				return nil, err
			}
		}
		bootstrap, bootstrapErr := retryHostedControl(ctx, func(attemptCtx context.Context) (enrollment.HostedBootstrap, error) {
			return enrollmentClient.HostedBootstrap(attemptCtx, enrollment.Config{
				ControlURL: controlURL.String(), ControlCAFile: environ("PAPERBOAT_CONTROL_CA_FILE"),
				StateRoot: runtimeConfig.StateRoot,
			})
		})
		if bootstrapErr != nil {
			return nil, fmt.Errorf("hosted bootstrap: %w", bootstrapErr)
		}
		hostedConfig.SetupScript = bootstrap.SetupScript
		hostedConfig.GitToken = bootstrap.SourcePassword
	}
	fetcher, err := auth.NewHTTPJWKSFetcher(controlURL.ResolveReference(&url.URL{Path: "/.well-known/jwks.json"}).String(), []string{controlURL.Hostname()}, transport)
	if err != nil {
		return nil, err
	}
	cache, err := auth.NewJWKSCache(auth.JWKSConfig{Fetcher: fetcher, Clock: productionClock{}, TTL: 5 * time.Minute, RetainMissing: auth.DefaultRetainMissing, PersistencePath: filepath.Join(runtimeConfig.StateRoot, "authorization", "jwks.json")})
	if err != nil {
		return nil, err
	}
	refreshCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	_ = cache.Refresh(refreshCtx)
	cancel()
	revocations := auth.NewRevocationCache()
	revocationRefresh, err := newRevocationRefreshService(controlURL.ResolveReference(&url.URL{Path: "/v1/helper-trust/revocations"}).String(), renewingTokens, enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, operationID, revocations, transport, 15*time.Second)
	if err != nil {
		return nil, err
	}
	authorizationRefresh := serviceGroup{&jwksRefreshService{cache: cache, interval: time.Minute}, revocationRefresh}
	verifier := auth.Verifier{Keys: cache, Clock: productionClock{}, Replays: auth.NewReplayCache(4096, productionClock{}), Revocations: revocations, ClockSkew: 30 * time.Second, RefreshTimeout: 2 * time.Second}
	authorizer, err := NewCredentialAuthorizer(CredentialAuthConfig{Issuer: issuer, EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, Verifier: verifier, Revocations: revocations})
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
	manager, err := connector.New(connector.Config{EnvironmentID: identity.EnvironmentID, HelperID: identity.HelperID, EdgePool: valueOrRuntime(environ("PAPERBOAT_EDGE_POOL"), "default"), Dialer: dialer, DrainTimeout: 10 * time.Second, Transport: productionConnectorTransport(environ("PAPERBOAT_CONNECTOR_TERMINAL_TRANSPORT"))})
	if err != nil {
		return nil, err
	}
	supervisor, err := connector.NewSupervisor(connector.SupervisorConfig{Manager: manager, Admissions: source, InitialBackoff: time.Second, MaxBackoff: 45 * time.Second, Metrics: metrics})
	if err != nil {
		return nil, err
	}
	networkChanges, err := newNetworkChangeService(2*time.Second, supervisor.NetworkChanged)
	if err != nil {
		return nil, err
	}
	connectorService := &connectorReadinessService{supervisor: supervisor, manager: manager, networkChanges: networkChanges}
	previews, err := preview.New(preview.Config{Prober: preview.TCPProber{Dialer: net.Dialer{Timeout: 2 * time.Second}}, MaxTargets: runtimeConfig.Resources.MaxPreviewTargets, MaxConcurrentProbes: runtimeConfig.Resources.MaxConcurrentProbes})
	if err != nil {
		return nil, err
	}
	previewMonitor, err := preview.NewMonitor(preview.MonitorConfig{Registry: previews})
	if err != nil {
		return nil, err
	}
	previewCredentials, err := preview.NewCredentialSource(preview.CredentialSourceConfig{Endpoint: controlURL.ResolveReference(&url.URL{Path: "/v1/previews/credentials"}).String(), AllowedHosts: []string{controlURL.Hostname()}, Identities: renewingTokens, Proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, OperationID: operationID, Transport: transport})
	if err != nil {
		return nil, err
	}
	previewControl, err := preview.NewControlClient(preview.ControlClientConfig{Endpoint: controlURL.ResolveReference(&url.URL{Path: "/v1/previews/operations"}).String(), AllowedHosts: []string{controlURL.Hostname()}, EnvironmentID: identity.EnvironmentID, Tokens: previewCredentials, Identities: renewingTokens, Proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, Transport: transport})
	if err != nil {
		return nil, err
	}
	previewSender, err := preview.NewHTTPSender(preview.HTTPSenderConfig{Endpoint: controlURL.ResolveReference(&url.URL{Path: "/v1/previews/observations"}).String(), AllowedHosts: []string{controlURL.Hostname()}, Tokens: previewCredentials, Identities: renewingTokens, Proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, OperationID: operationID, Transport: transport})
	if err != nil {
		return nil, err
	}
	previewReporter, err := preview.NewReporter(preview.ReporterConfig{Registry: previews, Sender: previewSender, Interval: runtimeConfig.Limits.HeartbeatInterval, Timeout: 10 * time.Second})
	if err != nil {
		return nil, err
	}
	var runtimeObservation *runtimeObservationService
	var availabilityService *availability.Service
	var updateClient *hostservice.Client
	machineID := environ("PAPERBOAT_USER_MACHINE_ID")
	if runtimeConfig.Profile == helperconfig.Hosted {
		machineID = valueOrRuntime(environ("FLY_MACHINE_ID"), environ("PAPERBOAT_MACHINE_ID"))
	}
	if machineID == "" {
		return nil, ErrProductionInvalid
	}
	if runtimeConfig.Profile == helperconfig.BYOD {
		resolver, resolverErr := availability.NewResolver(controlURL.ResolveReference(&url.URL{Path: "/v1/helper-runtime-policies/resolve"}).String(), renewingTokens, enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, operationID, &http.Client{Transport: transport, Timeout: 10 * time.Second})
		if resolverErr != nil {
			return nil, resolverErr
		}
		hostClient, hostErr := availability.NewHostClient("/var/run/paperboat/host-service.sock", 5*time.Second)
		if hostErr != nil {
			return nil, hostErr
		}
		availabilityService, err = availability.NewService(resolver, hostClient, runtimeConfig.Limits.HeartbeatInterval, metrics)
		if err != nil {
			return nil, err
		}
		updateClient, err = hostservice.NewClient("/var/run/paperboat/host-service.sock", 2*time.Minute)
		if err != nil {
			return nil, err
		}
	}
	{
		runtimeEndpoint := controlURL.ResolveReference(&url.URL{Path: "/v1/runtime-observations"}).String()
		scope := environ("PAPERBOAT_HELPER_SERVICE_SCOPE")
		if scope != "system" && scope != "user" {
			scope = "unknown"
		}
		sender := &runtimeObservationSender{endpoint: runtimeEndpoint, tokens: renewingTokens, proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot}, operationID: operationID, environmentID: identity.EnvironmentID, machineID: machineID, reporterVersion: version, client: &http.Client{Transport: transport, Timeout: 10 * time.Second}, availability: availabilityService, receiptPath: filepath.Join(runtimeConfig.StateRoot, "runtime", "server-heartbeat.json"), workerGeneration: bootState.Generation, osBootID: bootState.OSBootID, serviceScope: scope, connector: manager}
		runtimeObservation = &runtimeObservationService{sender: sender, interval: runtimeConfig.Limits.HeartbeatInterval, timeout: 10 * time.Second}
	}
	var hostedLifecycle *hosted.Lifecycle
	workspaceRoot := environ("PAPERBOAT_WORKSPACE_ROOT")
	agentShell := "/bin/bash"
	if runtimeConfig.Profile == helperconfig.BYOD {
		agentShell, err = validatedBYODShell(environ("PAPERBOAT_SHELL"))
		if err != nil {
			return nil, err
		}
	}
	agentEnvironment := []string{"PATH=" + os.Getenv("PATH"), "SHELL=" + agentShell, "TERM=xterm-256color"}
	if home, homeErr := os.UserHomeDir(); homeErr == nil && filepath.IsAbs(home) {
		agentEnvironment = append(agentEnvironment, "HOME="+home)
	}
	shutdownTimeout := 30 * time.Second
	if runtimeConfig.Profile == helperconfig.Hosted {
		hostedLifecycle, err = hosted.New(hostedConfig, hosted.Hooks{}, nil)
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
		workspaceRoot = hostedConfig.VolumeRoot
		shutdownTimeout = hostedConfig.FlushTimeout + 15*time.Second
	} else {
		if err := validateBYODWorkspace(workspaceRoot); err != nil {
			return nil, err
		}
	}
	configHome, err := productionConfigHome(runtimeConfig.Profile == helperconfig.Hosted, hostedConfig.VolumeRoot)
	if err != nil {
		return nil, err
	}
	repositoryHosts, err := productionRepositoryHosts(environ("PAPERBOAT_CONFIG_REPOSITORY_HOSTS"))
	if err != nil {
		return nil, err
	}
	configSyncService, err := newProductionConfigSync(productionConfigSyncConfig{
		ControlURL: controlURL.String(), ControlHost: controlURL.Hostname(),
		RepositoryHosts: repositoryHosts, HomeRoot: configHome, StateRoot: runtimeConfig.StateRoot,
		ChezmoiBinary: valueOrRuntime(environ("PAPERBOAT_CHEZMOI_PATH"), "/usr/local/bin/chezmoi"),
		Identities:    renewingTokens, Proofs: enrollment.ProofSource{StateRoot: runtimeConfig.StateRoot},
		OperationID: operationID, Transport: transport,
	})
	if err != nil {
		return nil, err
	}
	listen := valueOrRuntime(environ("PAPERBOAT_HELPER_LISTEN_ADDRESS"), "127.0.0.1:8080")
	if err := writeWorkerLocal(runtimeConfig.StateRoot, listen); err != nil {
		return nil, err
	}
	herdrPath := valueOrRuntime(environ("PAPERBOAT_HERDR_PATH"), "/usr/local/bin/herdr")
	herdrVersion := valueOrRuntime(environ("PAPERBOAT_HERDR_VERSION"), "0.7.4")
	runtimeService := Service(runtimeObservation)
	if availabilityService != nil {
		runtimeService = serviceGroup{availabilityService, runtimeObservation}
	}
	dependencies := HelperDependencies{Authorizer: authorizer, AuthorizationService: authorizationRefresh, Connector: connectorService, Previews: previews, PreviewControl: previewControl, PreviewRoutesChanged: supervisor.RoutesChanged, PreviewService: serviceGroup{previewMonitor, previewReporter}, RuntimeObservationService: runtimeService, ConfigSync: configSyncService, Updates: updateClient, Metrics: metrics}
	if runtimeConfig.Profile == helperconfig.Hosted {
		dependencies.HostedLifecycle = hostedLifecycle
		dependencies.ConfigApply = configapply.SyncHandler{Apply: configSyncService.Apply}
		dependencies.ConfigApplyProof = true
	}
	return NewHelper(ctx, HelperConfig{Runtime: runtimeConfig, ListenAddress: listen, WorkspaceRoot: workspaceRoot, HerdrPath: herdrPath, HerdrVersion: herdrVersion, ShellPath: agentShell, AgentEnvironment: agentEnvironment, EnvironmentID: identity.EnvironmentID, ShutdownTimeout: shutdownTimeout, RecoveryExitSignal: recoveryExitSignal}, dependencies)
}

func writeWorkerLocal(stateRoot, listenAddress string) error {
	body, err := json.Marshal(struct {
		Schema        string `json:"schema"`
		ListenAddress string `json:"listen_address"`
	}{"paperboat.worker-local/v1", listenAddress})
	if err != nil {
		return err
	}
	path := filepath.Join(stateRoot, "runtime", "worker-local.json")
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".worker-local-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func retryHostedControl[T any](ctx context.Context, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	if operation == nil {
		return zero, ErrProductionInvalid
	}
	backoff := time.Second
	for {
		result, err := operation(ctx)
		if err == nil {
			return result, nil
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return zero, ctx.Err()
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
		}
	}
}

func validatedBYODShell(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		path = "/bin/sh"
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return "", errors.Join(ErrProductionInvalid, errors.New("BYOD shell must be an absolute canonical path"))
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.Join(ErrProductionInvalid, errors.New("BYOD shell must be an executable regular file"))
	}
	return path, nil
}

func validateBYODWorkspace(root string) error {
	if strings.TrimSpace(root) == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return errors.Join(ErrProductionInvalid, errors.New("BYOD workspace must be an absolute canonical path"))
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrProductionInvalid, errors.New("BYOD workspace must be an existing non-symlink directory"))
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil || resolved != root {
		return errors.Join(ErrProductionInvalid, errors.New("BYOD workspace symlink resolution is not permitted"))
	}
	return nil
}

type runtimeObservationService struct {
	mu       sync.Mutex
	sender   *runtimeObservationSender
	interval time.Duration
	timeout  time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

func (s *runtimeObservationService) Start(ctx context.Context) error {
	if s.sender == nil || s.interval <= 0 || s.timeout <= 0 {
		return ErrProductionInvalid
	}
	initial, cancel := context.WithTimeout(ctx, s.timeout)
	err := s.sender.Send(initial)
	cancel()
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return ErrProductionInvalid
	}
	runCtx, stop := context.WithCancel(context.Background())
	s.cancel, s.done = stop, make(chan struct{})
	go s.loop(runCtx, s.done)
	return nil
}

func (s *runtimeObservationService) loop(ctx context.Context, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendCtx, cancel := context.WithTimeout(ctx, s.timeout)
			_ = s.sender.Send(sendCtx)
			cancel()
		}
	}
}

func (s *runtimeObservationService) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	cancel, done := s.cancel, s.done
	s.cancel, s.done = nil, nil
	s.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	final, stop := context.WithTimeout(ctx, s.timeout)
	err := s.sender.Send(final)
	stop()
	return err
}

type runtimeObservationSender struct {
	endpoint, environmentID, machineID, reporterVersion string
	tokens                                              interface {
		Token(context.Context) (string, error)
	}
	proofs interface {
		Proof(context.Context, string, string, string, []byte) ([]byte, error)
	}
	operationID  func() (string, error)
	client       *http.Client
	availability interface {
		Observation() *availability.Observation
	}
	receiptPath      string
	workerGeneration uint64
	osBootID         string
	serviceScope     string
	connector        interface{ Status() connector.Status }
}

func (s *runtimeObservationSender) Send(ctx context.Context) error {
	now := time.Now().UTC()
	body, err := json.Marshal(struct {
		EnvironmentID      string                         `json:"environment_id"`
		ResourceID         string                         `json:"resource_id"`
		ReporterVersion    string                         `json:"reporter_version"`
		SampledAt          time.Time                      `json:"sampled_at"`
		Availability       *availability.Observation      `json:"availability,omitempty"`
		RuntimeDiagnostics *runtimeDiagnosticsObservation `json:"runtime_diagnostics,omitempty"`
	}{s.environmentID, s.machineID, s.reporterVersion, now, availabilityObservation(s.availability), s.runtimeDiagnostics(now)})
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
	proof, err := s.proofs.Proof(ctx, operationID, http.MethodPost, "/v1/runtime-observations", body)
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
		return fmt.Errorf("runtime observation rejected with status %d", response.StatusCode)
	}
	if s.receiptPath == "" {
		return nil
	}
	return writeServerHeartbeatReceipt(s.receiptPath, serverHeartbeatReceipt{Schema: "paperboat.server-heartbeat/v1", WorkerGeneration: s.workerGeneration, ReporterVersion: s.reporterVersion, AcceptedAt: time.Now().UTC()})
}

type runtimeDiagnosticsObservation struct {
	WorkerGeneration    uint64    `json:"worker_generation"`
	OSBootID            string    `json:"os_boot_id"`
	ConnectorState      string    `json:"connector_state"`
	ConnectorGeneration uint64    `json:"connector_generation"`
	WorkerServiceScope  string    `json:"worker_service_scope"`
	ObservedAt          time.Time `json:"observed_at"`
}

func (s *runtimeObservationSender) runtimeDiagnostics(observedAt time.Time) *runtimeDiagnosticsObservation {
	if s.workerGeneration < 1 || s.osBootID == "" || s.connector == nil {
		return nil
	}
	status := s.connector.Status()
	state := "unavailable"
	if status.Connected {
		state = "ready"
	} else if status.Stopping {
		state = "degraded"
	}
	return &runtimeDiagnosticsObservation{WorkerGeneration: s.workerGeneration, OSBootID: s.osBootID, ConnectorState: state, ConnectorGeneration: status.Generation, WorkerServiceScope: s.serviceScope, ObservedAt: observedAt}
}

type serverHeartbeatReceipt struct {
	Schema           string    `json:"schema"`
	WorkerGeneration uint64    `json:"worker_generation"`
	ReporterVersion  string    `json:"reporter_version"`
	AcceptedAt       time.Time `json:"accepted_at"`
}

func writeServerHeartbeatReceipt(path string, receipt serverHeartbeatReceipt) error {
	if !filepath.IsAbs(path) || receipt.WorkerGeneration < 1 || receipt.ReporterVersion == "" || receipt.AcceptedAt.IsZero() {
		return ErrProductionInvalid
	}
	body, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrProductionInvalid
	}
	temporary, err := os.CreateTemp(directory, ".server-heartbeat-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(body); err != nil {
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
	return os.Rename(name, path)
}

func availabilityObservation(source interface {
	Observation() *availability.Observation
}) *availability.Observation {
	if source == nil {
		return nil
	}
	return source.Observation()
}

type connectorReadinessService struct {
	supervisor     *connector.Supervisor
	manager        *connector.Manager
	networkChanges Service
}

func (s *connectorReadinessService) Start(ctx context.Context) error {
	if err := s.supervisor.Start(ctx); err != nil {
		return err
	}
	if s.networkChanges != nil {
		if err := s.networkChanges.Start(ctx); err != nil {
			return errors.Join(err, s.supervisor.Shutdown(context.Background()))
		}
	}
	return nil
}
func (s *connectorReadinessService) Shutdown(ctx context.Context) error {
	var networkErr error
	if s.networkChanges != nil {
		networkErr = s.networkChanges.Shutdown(ctx)
	}
	return errors.Join(networkErr, s.supervisor.Shutdown(ctx))
}

type jwksRefreshService struct {
	cache    *auth.JWKSCache
	interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

func (s *jwksRefreshService) Start(context.Context) error {
	if s.cache == nil || s.interval <= 0 || s.cancel != nil {
		return ErrProductionInvalid
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go func() {
		defer close(s.done)
		timer := time.NewTimer(0)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				attemptCtx, attemptCancel := context.WithTimeout(ctx, 10*time.Second)
				_ = s.cache.Refresh(attemptCtx)
				attemptCancel()
				timer.Reset(s.interval)
			}
		}
	}()
	return nil
}

func (s *jwksRefreshService) Shutdown(ctx context.Context) error {
	if s.cancel == nil {
		return nil
	}
	s.cancel()
	select {
	case <-s.done:
		s.cancel, s.done = nil, nil
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *connectorReadinessService) CapabilityHealth() health.Capability {
	status := s.manager.Status()
	if status.Stopping {
		return health.Capability{State: health.Unavailable, Reason: "stopped"}
	}
	if status.Connected {
		return health.Capability{State: health.Ready}
	}
	return health.Capability{State: health.Unavailable, Reason: "connector_unavailable", RetryAfterMs: 1000}
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
func productionConnectorTransport(value string) connector.Transport {
	return connector.Transport(valueOrRuntime(value, string(connector.Auto)))
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
