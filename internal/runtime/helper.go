//go:build darwin || linux

package runtime

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/activity"
	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
	"github.com/pinksaucepasta/paperboat-helper/internal/configapply"
	"github.com/pinksaucepasta/paperboat-helper/internal/health"
	"github.com/pinksaucepasta/paperboat-helper/internal/operation"
	"github.com/pinksaucepasta/paperboat-helper/internal/preview"
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
	ShellPath        string
	ShellArgs        []string
	ShellEnvironment []string
	OriginPatterns   []string
	EnvironmentID    string
	ShutdownTimeout  time.Duration
}

type HelperDependencies struct {
	Authorizer       server.AuthorizerFactory
	Listener         ListenerFactory
	Connector        Service
	Previews         *preview.Registry
	PreviewService   Service
	Activity         *activity.Collector
	ActivityService  Service
	SignalVerifier   *activity.SignalVerifier
	ConfigApply      configapply.Handler
	ConfigApplyProof bool
	Random           io.Reader
	HostedLifecycle  HostedLifecycle
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
	if err := config.Runtime.Validate(); err != nil || !LoopbackAddress(config.ListenAddress) || !filepath.IsAbs(config.WorkspaceRoot) || config.ShellPath == "" || dependencies.Authorizer == nil {
		return nil, errors.Join(ErrHelperInvalid, err)
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = 30 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		return nil, ErrHelperInvalid
	}
	invalidActivity := dependencies.Activity != nil && config.EnvironmentID == "" ||
		dependencies.SignalVerifier != nil && dependencies.Activity == nil ||
		dependencies.ActivityService != nil && dependencies.Activity == nil
	invalidPreview := dependencies.PreviewService != nil && dependencies.Previews == nil
	invalidConfigApply := dependencies.ConfigApplyProof && dependencies.ConfigApply == nil
	invalidHosted := config.Runtime.Profile == helperconfig.Hosted && dependencies.HostedLifecycle == nil ||
		config.Runtime.Profile == helperconfig.BYOD && dependencies.HostedLifecycle != nil
	if invalidActivity || invalidPreview || invalidConfigApply || invalidHosted {
		return nil, ErrHelperInvalid
	}
	if _, err := pty.ValidateProcessPolicy(config.ShellPath, config.ShellArgs, config.ShellEnvironment); err != nil {
		return nil, errors.Join(ErrHelperInvalid, err)
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
	journal, err := operation.NewPersistentJournal(ctx, resources.MaxConcurrentOps*32, durable, time.Hour, nil)
	if err != nil {
		return nil, err
	}

	healthSource := &runtimeHealthSource{}
	dispatcher, err := server.NewDispatcher(server.DispatcherConfig{
		Sessions: sessions, Health: healthSource, ShellPath: config.ShellPath,
		ShellArgs:     append([]string(nil), config.ShellArgs...),
		ShellEnv:      append([]string(nil), config.ShellEnvironment...),
		WorkspaceRoot: config.WorkspaceRoot, Random: random,
		Previews: dependencies.Previews, Activity: dependencies.Activity,
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
		components = append(components, Component{Capability: "edge", Required: config.Runtime.Profile == helperconfig.Hosted, Service: dependencies.Connector})
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
	healthSource.set(runtime)
	return &Helper{runtime: runtime, http: httpService, handler: mux, sessions: sessions}, nil
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
}

func (s *runtimeHealthSource) set(runtime *Runtime) { s.mu.Lock(); s.runtime = runtime; s.mu.Unlock() }
func (s *runtimeHealthSource) Snapshot() (snapshot health.Snapshot) {
	s.mu.RLock()
	runtime := s.runtime
	s.mu.RUnlock()
	if runtime != nil {
		return runtime.Health()
	}
	return snapshot
}
