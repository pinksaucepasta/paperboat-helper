//go:build darwin || linux

package hostservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/bootstrap"
)

const (
	ProtocolV1 = "paperboat.host-service/v1"
	AllowSleep = "allow_sleep"
	KeepAwake  = "keep_awake"
)

var (
	ErrInvalidConfig  = errors.New("invalid host-service configuration")
	ErrInvalidRequest = errors.New("invalid host-service request")
	ErrPeerDenied     = errors.New("host-service peer is not the enrolled user")
	ErrStalePolicy    = errors.New("availability policy version is stale")
)

type Request struct {
	Schema              string                      `json:"schema"`
	Operation           string                      `json:"operation"`
	Mode                string                      `json:"mode,omitempty"`
	Version             int64                       `json:"version,omitempty"`
	WorkerArtifact      *bootstrap.ArtifactManifest `json:"worker_artifact,omitempty"`
	HostServiceArtifact *bootstrap.ArtifactManifest `json:"host_service_artifact,omitempty"`
}

type Response struct {
	Schema             string    `json:"schema"`
	Status             string    `json:"status"`
	DesiredMode        string    `json:"desired_mode"`
	DesiredVersion     int64     `json:"desired_version"`
	ObservedMode       string    `json:"observed_mode,omitempty"`
	ObservedVersion    int64     `json:"observed_version,omitempty"`
	ObservedAt         time.Time `json:"observed_at,omitempty"`
	ErrorCode          string    `json:"error_code,omitempty"`
	HostServiceVersion string    `json:"host_service_version"`
	Scope              string    `json:"scope"`
	UpdateVersion      string    `json:"update_version,omitempty"`
	UpdateRollbacks    uint64    `json:"update_rollbacks"`
}

type State struct {
	Schema          string    `json:"schema"`
	DesiredMode     string    `json:"desired_mode"`
	DesiredVersion  int64     `json:"desired_version"`
	ObservedMode    string    `json:"observed_mode,omitempty"`
	ObservedVersion int64     `json:"observed_version,omitempty"`
	ObservedAt      time.Time `json:"observed_at,omitempty"`
	Status          string    `json:"status"`
	ErrorCode       string    `json:"error_code,omitempty"`
}

type Applier interface {
	Apply(context.Context, string) error
	Close(context.Context) error
}

type UpdateActivator interface {
	Activate(context.Context, bootstrap.ArtifactManifest, bootstrap.ArtifactManifest) (string, error)
}

type UpdateDiagnostics interface {
	RollbackCount() uint64
}

type Config struct {
	SocketPath string
	StatePath  string
	UID        int
	GID        int
	Applier    Applier
	Now        func() time.Time
	Version    string
	Updates    UpdateActivator
}

type Server struct {
	config Config
	mu     sync.Mutex
	state  State
}

func New(config Config) (*Server, error) {
	if !filepath.IsAbs(config.SocketPath) || !filepath.IsAbs(config.StatePath) || config.UID < 1 || config.GID < 1 || config.Applier == nil || config.Version == "" {
		return nil, ErrInvalidConfig
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	server := &Server{config: config, state: State{Schema: ProtocolV1, DesiredMode: KeepAwake, Status: "pending"}}
	if err := server.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return server, nil
}

func (s *Server) Run(ctx context.Context) error {
	if os.Geteuid() != 0 {
		return ErrInvalidConfig
	}
	s.mu.Lock()
	mode := s.state.DesiredMode
	s.mu.Unlock()
	if mode == KeepAwake {
		if err := s.apply(ctx, mode, s.state.DesiredVersion); err != nil {
			return err
		}
	}
	listener, err := s.listen()
	if err != nil {
		return err
	}
	defer func() {
		listener.Close()
		_ = os.Remove(s.config.SocketPath)
	}()
	go func() {
		<-ctx.Done()
		listener.Close()
	}()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				return errors.Join(ctx.Err(), s.config.Applier.Close(context.Background()))
			}
			return err
		}
		_ = s.serve(connection)
		connection.Close()
	}
}

func (s *Server) serve(connection *net.UnixConn) error {
	uid, err := peerUID(connection)
	if err != nil || uid != s.config.UID {
		return ErrPeerDenied
	}
	_ = connection.SetDeadline(time.Now().Add(5 * time.Second))
	decoder := json.NewDecoder(io.LimitReader(connection, 16<<10))
	decoder.DisallowUnknownFields()
	var request Request
	if err := decoder.Decode(&request); err != nil {
		return s.respond(connection, s.errorResponse("invalid_request"))
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF || request.Schema != ProtocolV1 {
		return s.respond(connection, s.errorResponse("invalid_request"))
	}
	if request.Operation == "diagnostics" {
		if request.Mode != "" || request.Version != 0 || request.WorkerArtifact != nil || request.HostServiceArtifact != nil {
			return s.respond(connection, s.errorResponse("invalid_request"))
		}
		s.mu.Lock()
		current := s.state
		s.mu.Unlock()
		return s.respond(connection, s.response(current))
	}
	if request.Operation == "activate_update" {
		if request.Mode != "" || request.Version != 0 || request.WorkerArtifact == nil || request.HostServiceArtifact == nil || s.config.Updates == nil {
			return s.respond(connection, s.errorResponse("invalid_request"))
		}
		version, activateErr := s.config.Updates.Activate(context.Background(), *request.WorkerArtifact, *request.HostServiceArtifact)
		if activateErr != nil {
			return s.respond(connection, s.errorResponse("update_activation_failed"))
		}
		response := s.errorResponse("")
		response.UpdateVersion = version
		return s.respond(connection, response)
	}
	if request.Operation != "apply_availability" || request.WorkerArtifact != nil || request.HostServiceArtifact != nil || !validMode(request.Mode) || request.Version < 0 {
		return s.respond(connection, s.errorResponse("invalid_request"))
	}
	s.mu.Lock()
	current := s.state
	s.mu.Unlock()
	if request.Version < current.DesiredVersion || request.Version == current.DesiredVersion && request.Mode != current.DesiredMode {
		return s.respond(connection, s.errorResponse("stale_policy"))
	}
	if request.Version == current.DesiredVersion && current.Status == "applied" {
		return s.respond(connection, s.response(current))
	}
	err = s.apply(context.Background(), request.Mode, request.Version)
	s.mu.Lock()
	result := s.state
	s.mu.Unlock()
	if err != nil {
		return s.respond(connection, s.response(result))
	}
	return s.respond(connection, s.response(result))
}

func (s *Server) apply(ctx context.Context, mode string, version int64) error {
	s.mu.Lock()
	s.state.DesiredMode, s.state.DesiredVersion, s.state.Status, s.state.ErrorCode = mode, version, "pending", ""
	if err := s.persistLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()
	err := s.config.Applier.Apply(ctx, mode)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.state.ObservedMode, s.state.ObservedVersion = mode, version
		s.state.ObservedAt, s.state.Status, s.state.ErrorCode = s.config.Now().UTC(), "error", "availability_apply_failed"
		return errors.Join(err, s.persistLocked())
	}
	s.state.ObservedMode, s.state.ObservedVersion = mode, version
	s.state.ObservedAt, s.state.Status, s.state.ErrorCode = s.config.Now().UTC(), "applied", ""
	return s.persistLocked()
}

func (s *Server) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *Server) listen() (*net.UnixListener, error) {
	directory := filepath.Dir(s.config.SocketPath)
	if err := secureDirectory(directory, 0o755); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(s.config.SocketPath); err == nil {
		if info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrInvalidConfig
		}
		if err := os.Remove(s.config.SocketPath); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.config.SocketPath, Net: "unix"})
	if err != nil {
		return nil, err
	}
	if err := os.Chown(s.config.SocketPath, s.config.UID, s.config.GID); err != nil {
		listener.Close()
		return nil, err
	}
	if err := os.Chmod(s.config.SocketPath, 0o600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func (s *Server) load() error {
	body, err := os.ReadFile(s.config.StatePath)
	if err != nil {
		return err
	}
	if len(body) > 16<<10 {
		return ErrInvalidConfig
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var state State
	if decoder.Decode(&state) != nil || state.Schema != ProtocolV1 || !validMode(state.DesiredMode) || state.DesiredVersion < 0 || !validStatus(state.Status) {
		return ErrInvalidConfig
	}
	s.state = state
	return nil
}

func (s *Server) persistLocked() error {
	body, err := json.Marshal(s.state)
	if err != nil {
		return err
	}
	directory := filepath.Dir(s.config.StatePath)
	if err := secureDirectory(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".host-policy-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
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
	return os.Rename(path, s.config.StatePath)
}

func (s *Server) respond(writer io.Writer, value Response) error {
	return json.NewEncoder(writer).Encode(value)
}
func (s *Server) errorResponse(code string) Response {
	s.mu.Lock()
	defer s.mu.Unlock()
	value := s.response(s.state)
	value.ErrorCode = code
	return value
}
func (s *Server) response(state State) Response {
	var rollbacks uint64
	if diagnostics, ok := s.config.Updates.(UpdateDiagnostics); ok {
		rollbacks = diagnostics.RollbackCount()
	}
	return Response{Schema: ProtocolV1, Status: state.Status, DesiredMode: state.DesiredMode, DesiredVersion: state.DesiredVersion, ObservedMode: state.ObservedMode, ObservedVersion: state.ObservedVersion, ObservedAt: state.ObservedAt, ErrorCode: state.ErrorCode, HostServiceVersion: s.config.Version, Scope: "system", UpdateRollbacks: rollbacks}
}
func validMode(mode string) bool { return mode == AllowSleep || mode == KeepAwake }
func validStatus(status string) bool {
	return status == "applied" || status == "pending" || status == "error"
}
func secureDirectory(path string, mode os.FileMode) error {
	if err := os.MkdirAll(path, mode); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrInvalidConfig
	}
	return os.Chmod(path, mode)
}
