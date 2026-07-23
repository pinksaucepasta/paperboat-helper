//go:build darwin || linux

package runtime

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
	"github.com/pinksaucepasta/paperboat-helper/internal/process"
	"github.com/pinksaucepasta/paperboat-helper/internal/protocol"
	"github.com/pinksaucepasta/paperboat-helper/internal/pty"
	"github.com/pinksaucepasta/paperboat-helper/internal/server"
	"github.com/pinksaucepasta/paperboat-helper/internal/session"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
	"github.com/pinksaucepasta/paperboat-helper/internal/upload"
)

var ErrHelperInvalid = errors.New("invalid helper runtime composition")

type HelperConfig struct {
	Runtime          helperconfig.Config
	ListenAddress    string
	WorkspaceRoot    string
	HerdrPath        string
	HerdrVersion     string
	AgentEnvironment []string
	OriginPatterns   []string
	EnvironmentID    string
	AgentTokenFile   string
	ShutdownTimeout  time.Duration
}

type HelperDependencies struct {
	Authorizer             server.AuthorizerFactory
	Listener               ListenerFactory
	Connector              Service
	Previews               *preview.Registry
	PreviewControl         preview.PreviewControl
	PreviewRoutesChanged   func()
	PreviewService         Service
	Activity               *activity.Collector
	ActivityService        Service
	SignalVerifier         *activity.SignalVerifier
	ConfigApply            configapply.Handler
	ConfigApplyProof       bool
	Random                 io.Reader
	HostedLifecycle        HostedLifecycle
	SessionLauncherFactory func(*session.Manager) (server.SessionLauncher, error)
}

type HostedLifecycle interface {
	Service
	protocol.CapabilityProvider
}

type Helper struct {
	runtime  *Runtime
	http     *HTTPService
	handler  http.Handler
	sessions *session.Manager
}

func NewHelper(ctx context.Context, config HelperConfig, dependencies HelperDependencies) (_ *Helper, resultErr error) {
	if err := config.Runtime.Validate(); err != nil || !LoopbackAddress(config.ListenAddress) || !filepath.IsAbs(config.WorkspaceRoot) || dependencies.Authorizer == nil {
		return nil, errors.Join(ErrHelperInvalid, err)
	}
	if dependencies.SessionLauncherFactory == nil && (config.HerdrPath == "" || config.HerdrVersion == "") {
		return nil, ErrHelperInvalid
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		return nil, ErrHelperInvalid
	}
	if config.AgentTokenFile == "" {
		config.AgentTokenFile = filepath.Join(config.Runtime.StateRoot, "agent", "token")
	}
	if !filepath.IsAbs(config.AgentTokenFile) {
		return nil, ErrHelperInvalid
	}
	config.AgentEnvironment = append(config.AgentEnvironment,
		"PAPERBOAT_HELPER_AGENT_ENDPOINT=http://"+config.ListenAddress+"/v1/agent/previews",
		"PAPERBOAT_HELPER_AGENT_TOKEN_FILE="+config.AgentTokenFile,
	)
	invalidActivity := dependencies.Activity != nil && config.EnvironmentID == "" ||
		dependencies.SignalVerifier != nil && dependencies.Activity == nil ||
		dependencies.ActivityService != nil && dependencies.Activity == nil
	invalidPreview := dependencies.PreviewService != nil && dependencies.Previews == nil
	invalidConfigApply := dependencies.ConfigApplyProof && dependencies.ConfigApply == nil
	invalidHosted := config.Runtime.Profile == helperconfig.Hosted && dependencies.HostedLifecycle == nil ||
		config.Runtime.Profile == helperconfig.BYOD && dependencies.HostedLifecycle != nil
	if invalidActivity {
		return nil, errors.Join(ErrHelperInvalid, errors.New("invalid activity dependencies"))
	}
	if invalidPreview {
		return nil, errors.Join(ErrHelperInvalid, errors.New("invalid preview dependencies"))
	}
	if invalidConfigApply {
		return nil, errors.Join(ErrHelperInvalid, errors.New("invalid config-apply dependencies"))
	}
	if invalidHosted {
		return nil, errors.Join(ErrHelperInvalid, errors.New("invalid hosted lifecycle dependencies"))
	}
	adapter, err := pty.NewAdapter(config.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	resources := config.Runtime.Resources
	if resources == (helperconfig.ResourceLimits{}) {
		resources = helperconfig.DefaultResources
	}
	random := dependencies.Random
	if random == nil {
		random = rand.Reader
	}
	random = &lockedReader{reader: random}
	agentToken, err := writeAgentToken(config.AgentTokenFile, random)
	if err != nil {
		return nil, err
	}

	durable, err := store.Open(ctx, store.Config{Root: config.Runtime.StateRoot})
	if err != nil {
		return nil, err
	}
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, durable.Close())
		}
	}()
	sessions, err := session.NewManager(session.ManagerConfig{
		Launch: func(command pty.Command) (session.PTYProcess, error) { return adapter.Start(command) },
		Random: random, HistoryBytes: resources.HistoryBytes,
		AttachmentBytes: config.Runtime.Limits.PendingOutputBytes,
		MaxSessions:     resources.MaxSessions, MaxAttachments: resources.MaxAttachments,
		MaxInputDecisions:  resources.MaxInputDecisions,
		TerminationTimeout: 10 * time.Second, TerminationGrace: 2 * time.Second,
		Store: durable, Activity: dependencies.Activity, EnvironmentID: config.EnvironmentID,
	})
	if err != nil {
		return nil, err
	}
	var sessionLauncher server.SessionLauncher
	if dependencies.SessionLauncherFactory != nil {
		sessionLauncher, err = dependencies.SessionLauncherFactory(sessions)
	} else {
		sessionLauncher, err = process.NewSupervisor(ctx, process.Config{Executable: config.HerdrPath, ExpectedVersion: config.HerdrVersion, Environment: append([]string(nil), config.AgentEnvironment...), StateRoot: filepath.Join(config.Runtime.StateRoot, "herdr"), Sessions: sessions})
	}
	if err != nil || sessionLauncher == nil {
		return nil, errors.Join(ErrHelperInvalid, err)
	}
	journal, err := operation.NewPersistentJournal(ctx, resources.MaxConcurrentOps*32, durable, time.Hour, nil)
	if err != nil {
		return nil, err
	}

	healthSource := &runtimeHealthSource{}
	dispatcher, err := server.NewDispatcher(server.DispatcherConfig{
		Sessions: sessions, Health: healthSource, SessionLauncher: sessionLauncher,
		WorkspaceRoot: config.WorkspaceRoot, Random: random,
		Previews: dependencies.Previews, PreviewControl: dependencies.PreviewControl, Activity: dependencies.Activity,
		SignalVerifier: dependencies.SignalVerifier, ConfigApply: dependencies.ConfigApply,
	})
	if err != nil {
		return nil, err
	}
	stager, err := upload.New(upload.Config{
		Root:          filepath.Join(config.Runtime.StateRoot, "uploads"),
		MaxConcurrent: resources.MaxConcurrentUploads, Random: random,
	})
	if err != nil {
		return nil, err
	}
	uploadHandler, err := server.NewUploadHandler(server.UploadHandlerConfig{
		Stager: stager, Journal: journal, Authorizer: dependencies.Authorizer,
		MaxConcurrent:    resources.MaxConcurrentUploads,
		MutationDeadline: config.Runtime.Limits.MutationDeadline,
	})
	if err != nil {
		return nil, err
	}
	providers := []protocol.CapabilityProvider{dispatcher, uploadHandler}
	if dependencies.HostedLifecycle != nil {
		providers = append(providers, dependencies.HostedLifecycle)
	}
	available, err := protocol.AvailableCapabilities(providers...)
	if err != nil {
		return nil, err
	}
	protocolServer, err := server.New(server.Config{
		Negotiator: protocol.Negotiator{Profile: config.Runtime.Profile, Available: available, ConfigApplyProof: dependencies.ConfigApplyProof},
		Journal:    journal, Authorizer: nil, Handler: dispatcher,
		MaxConcurrent:     resources.MaxConcurrentOps,
		HeartbeatInterval: config.Runtime.Limits.HeartbeatInterval,
		PeerTimeout:       config.Runtime.Limits.PeerTimeout,
		MutationDeadline:  config.Runtime.Limits.MutationDeadline,
	})
	if err != nil {
		return nil, err
	}
	websocketHandler, err := server.NewWebSocketHandler(server.WebSocketHandlerConfig{
		Server: protocolServer, Authorizer: dependencies.Authorizer,
		OriginPatterns: append([]string(nil), config.OriginPatterns...),
		MaxConnections: resources.MaxAttachments * resources.MaxSessions,
	})
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/v1/runtime", websocketHandler)
	mux.Handle("/v1/uploads", uploadHandler)
	if dependencies.Previews != nil && dependencies.PreviewControl != nil && config.EnvironmentID != "" {
		agentHandler, agentErr := preview.NewAgentHandler(preview.AgentHandlerConfig{Token: agentToken, EnvironmentID: config.EnvironmentID, Registry: dependencies.Previews, Control: dependencies.PreviewControl, RoutesChanged: dependencies.PreviewRoutesChanged})
		if agentErr != nil {
			return nil, agentErr
		}
		mux.Handle("/v1/agent/previews", agentHandler)
	}
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(writer).Encode(healthSource.Snapshot())
	})
	httpService, err := NewHTTPService(HTTPConfig{Address: config.ListenAddress, Handler: mux, Listener: dependencies.Listener})
	if err != nil {
		return nil, err
	}
	components := make([]Component, 0, 8)
	components = append(components,
		Component{Capability: "storage", Required: true, Service: shutdownService{shutdown: func(context.Context) error { return durable.Close() }}},
		Component{Capability: "sessions", Required: true, Service: shutdownService{shutdown: sessions.Shutdown}},
		Component{Capability: "protocol", Required: true, Service: protocolServer},
	)
	if dependencies.PreviewService != nil {
		components = append(components, Component{Capability: "target", Required: false, Service: dependencies.PreviewService})
	}
	if dependencies.ActivityService != nil {
		components = append(components, Component{Capability: "activity_delivery", Required: false, Service: dependencies.ActivityService})
	}
	if dependencies.Connector != nil {
		components = append(components, Component{Capability: "edge", Required: true, Service: dependencies.Connector})
	}
	// Start hosted preparation after transport dependencies. Reverse shutdown then
	// flushes hosted state before connector drain and the final activity report.
	if dependencies.HostedLifecycle != nil {
		components = append(components, Component{Capability: "hosted_lifecycle", Required: true, Service: dependencies.HostedLifecycle})
	}
	components = append(components, Component{Capability: "control_plane", Required: true, Service: httpService})
	runtime, err := NewRuntime(Config{Version: config.Runtime.Version, Components: components, ShutdownTimeout: config.ShutdownTimeout})
	if err != nil {
		return nil, err
	}
	healthSource.set(runtime, components)
	return &Helper{runtime: runtime, http: httpService, handler: mux, sessions: sessions}, nil
}

func writeAgentToken(path string, random io.Reader) (string, error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", ErrHelperInvalid
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", err
	}
	if info, err = os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return "", ErrHelperInvalid
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := io.ReadFull(random, raw); err != nil {
		return "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	temporary, err := os.CreateTemp(directory, ".agent-token-*")
	if err != nil {
		return "", err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return "", err
	}
	if _, err := io.WriteString(temporary, token+"\n"); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return "", err
	}
	return token, nil
}

func (h *Helper) Start(ctx context.Context) error    { return h.runtime.Start(ctx) }
func (h *Helper) Shutdown(ctx context.Context) error { return h.runtime.Shutdown(ctx) }
func (h *Helper) State() State                       { return h.runtime.State() }
func (h *Helper) Handler() http.Handler              { return h.handler }
func (h *Helper) HTTP() *HTTPService                 { return h.http }
func (h *Helper) Sessions() *session.Manager         { return h.sessions }

type shutdownService struct{ shutdown func(context.Context) error }

func (shutdownService) Start(context.Context) error          { return nil }
func (s shutdownService) Shutdown(ctx context.Context) error { return s.shutdown(ctx) }

type serviceGroup []Service

func (g serviceGroup) Start(ctx context.Context) error {
	started := 0
	for i, service := range g {
		if err := service.Start(ctx); err != nil {
			for j := started - 1; j >= 0; j-- {
				_ = g[j].Shutdown(ctx)
			}
			return err
		}
		started = i + 1
	}
	return nil
}

func (g serviceGroup) Shutdown(ctx context.Context) error {
	var result error
	for i := len(g) - 1; i >= 0; i-- {
		result = errors.Join(result, g[i].Shutdown(ctx))
	}
	return result
}

type lockedReader struct {
	mu     sync.Mutex
	reader io.Reader
}

func (r *lockedReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reader.Read(buffer)
}

type runtimeHealthSource struct {
	mu      sync.RWMutex
	runtime *Runtime
	dynamic map[string]capabilityHealthProvider
}

type capabilityHealthProvider interface {
	CapabilityHealth() health.Capability
}

func (s *runtimeHealthSource) set(runtime *Runtime, components []Component) {
	dynamic := make(map[string]capabilityHealthProvider)
	for _, component := range components {
		if provider, ok := component.Service.(capabilityHealthProvider); ok {
			dynamic[component.Capability] = provider
		}
	}
	s.mu.Lock()
	s.runtime, s.dynamic = runtime, dynamic
	s.mu.Unlock()
}
func (s *runtimeHealthSource) Snapshot() (snapshot health.Snapshot) {
	s.mu.RLock()
	runtime := s.runtime
	dynamic := make(map[string]capabilityHealthProvider, len(s.dynamic))
	for capability, provider := range s.dynamic {
		dynamic[capability] = provider
	}
	s.mu.RUnlock()
	if runtime != nil {
		snapshot = runtime.Health()
		for capability, provider := range dynamic {
			snapshot.Capabilities[capability] = provider.CapabilityHealth()
		}
	}
	return snapshot
}
