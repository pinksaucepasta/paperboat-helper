//go:build darwin || linux

package availability

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/hostservice"
)

const PolicySchemaV1 = "paperboat.availability-policy/v1"

var ErrInvalid = errors.New("invalid availability runtime contract")

type IdentitySource interface {
	Token(context.Context) (string, error)
}
type ProofSource interface {
	Proof(context.Context, string, string, string, []byte) ([]byte, error)
}

type Resolution struct {
	Schema        string `json:"schema"`
	UserMachineID string `json:"user_machine_id"`
	Mode          string `json:"mode"`
	Version       int64  `json:"version"`
}

type Observation struct {
	Schema             string    `json:"schema"`
	Mode               string    `json:"mode"`
	Version            int64     `json:"version"`
	Status             string    `json:"status"`
	ObservedAt         time.Time `json:"observed_at"`
	ErrorCode          string    `json:"error_code,omitempty"`
	HostServiceVersion string    `json:"host_service_version"`
	HostServiceScope   string    `json:"host_service_scope"`
	UpdateRollbacks    uint64    `json:"update_rollbacks"`
}

type Resolver struct {
	endpoint    *url.URL
	identities  IdentitySource
	proofs      ProofSource
	operationID func() (string, error)
	client      *http.Client
}

func NewResolver(endpoint string, identities IdentitySource, proofs ProofSource, operationID func() (string, error), client *http.Client) (*Resolver, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.Path != "/v1/helper-runtime-policies/resolve" || parsed.RawQuery != "" || parsed.Fragment != "" || identities == nil || proofs == nil || operationID == nil || client == nil || client.Timeout <= 0 {
		return nil, ErrInvalid
	}
	return &Resolver{endpoint: parsed, identities: identities, proofs: proofs, operationID: operationID, client: client}, nil
}

func (r *Resolver) Resolve(ctx context.Context) (Resolution, error) {
	body := []byte("{}")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return Resolution{}, err
	}
	token, err := r.identities.Token(ctx)
	if err != nil {
		return Resolution{}, err
	}
	operationID, err := r.operationID()
	if err != nil {
		return Resolution{}, err
	}
	proof, err := r.proofs.Proof(ctx, operationID, http.MethodPost, r.endpoint.Path, body)
	if err != nil {
		return Resolution{}, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(request)
	if err != nil {
		return Resolution{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return Resolution{}, fmt.Errorf("availability policy resolve rejected with status %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 16<<10))
	decoder.DisallowUnknownFields()
	var envelope struct {
		Data Resolution `json:"data"`
	}
	var extra any
	if decoder.Decode(&envelope) != nil || decoder.Decode(&extra) != io.EOF || envelope.Data.Schema != PolicySchemaV1 || envelope.Data.UserMachineID == "" || !validMode(envelope.Data.Mode) || envelope.Data.Version < 0 {
		return Resolution{}, ErrInvalid
	}
	return envelope.Data, nil
}

type HostClient struct {
	socketPath string
	timeout    time.Duration
}

func NewHostClient(socketPath string, timeout time.Duration) (*HostClient, error) {
	if !strings.HasPrefix(socketPath, "/") || timeout <= 0 {
		return nil, ErrInvalid
	}
	return &HostClient{socketPath: socketPath, timeout: timeout}, nil
}

func (c *HostClient) Apply(ctx context.Context, policy Resolution) (Observation, error) {
	if policy.Schema != PolicySchemaV1 || !validMode(policy.Mode) || policy.Version < 0 {
		return Observation{}, ErrInvalid
	}
	dialer := net.Dialer{Timeout: c.timeout}
	connection, err := dialer.DialContext(ctx, "unix", c.socketPath)
	if err != nil {
		return Observation{}, err
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(c.timeout))
	request := hostservice.Request{Schema: hostservice.ProtocolV1, Operation: "apply_availability", Mode: policy.Mode, Version: policy.Version}
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return Observation{}, err
	}
	if closer, ok := connection.(interface{ CloseWrite() error }); !ok || closer.CloseWrite() != nil {
		return Observation{}, ErrInvalid
	}
	decoder := json.NewDecoder(io.LimitReader(connection, 16<<10))
	decoder.DisallowUnknownFields()
	var response hostservice.Response
	var extra any
	if decoder.Decode(&response) != nil || decoder.Decode(&extra) != io.EOF || response.Schema != hostservice.ProtocolV1 || response.DesiredMode != policy.Mode || response.DesiredVersion != policy.Version || response.ObservedMode != policy.Mode || response.ObservedVersion != policy.Version || response.ObservedAt.IsZero() || response.HostServiceVersion == "" || response.Scope != "system" || !validStatus(response.Status, response.ErrorCode) {
		return Observation{}, ErrInvalid
	}
	return Observation{Schema: PolicySchemaV1, Mode: response.ObservedMode, Version: response.ObservedVersion, Status: response.Status, ObservedAt: response.ObservedAt.UTC(), ErrorCode: response.ErrorCode, HostServiceVersion: response.HostServiceVersion, HostServiceScope: response.Scope, UpdateRollbacks: response.UpdateRollbacks}, nil
}

type Service struct {
	resolver interface {
		Resolve(context.Context) (Resolution, error)
	}
	host interface {
		Apply(context.Context, Resolution) (Observation, error)
	}
	interval time.Duration
	mu       sync.RWMutex
	current  *Observation
	cancel   context.CancelFunc
	done     chan struct{}
	metrics  interface {
		Record(string, float64, map[string]string) error
	}
	lastRollbacks uint64
}

func NewService(resolver interface {
	Resolve(context.Context) (Resolution, error)
}, host interface {
	Apply(context.Context, Resolution) (Observation, error)
}, interval time.Duration, metrics ...interface {
	Record(string, float64, map[string]string) error
}) (*Service, error) {
	if resolver == nil || host == nil || interval <= 0 || interval > time.Minute {
		return nil, ErrInvalid
	}
	if len(metrics) > 1 {
		return nil, ErrInvalid
	}
	service := &Service{resolver: resolver, host: host, interval: interval}
	if len(metrics) == 1 {
		service.metrics = metrics[0]
	}
	return service, nil
}

func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return ErrInvalid
	}
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel, s.done = cancel, make(chan struct{})
	done := s.done
	s.mu.Unlock()
	go s.run(runCtx, done)
	return nil
}

func (s *Service) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	backoff := time.Second
	for {
		resolution, err := s.resolver.Resolve(ctx)
		if err == nil {
			var observation Observation
			observation, err = s.host.Apply(ctx, resolution)
			if err == nil {
				s.mu.Lock()
				if observation.UpdateRollbacks > s.lastRollbacks && s.metrics != nil {
					_ = s.metrics.Record("paperboat_helper_update_rollbacks_total", float64(observation.UpdateRollbacks-s.lastRollbacks), nil)
				}
				s.lastRollbacks = observation.UpdateRollbacks
				copy := observation
				s.current = &copy
				s.mu.Unlock()
			}
		}
		wait := backoff
		if err == nil {
			wait, backoff = s.interval, time.Second
		} else if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) Shutdown(ctx context.Context) error {
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
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) Observation() *Observation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current == nil {
		return nil
	}
	copy := *s.current
	return &copy
}

func validMode(value string) bool {
	return value == hostservice.AllowSleep || value == hostservice.KeepAwake
}
func validStatus(status, code string) bool {
	return status == "applied" && code == "" || (status == "unsupported" || status == "error") && code != ""
}
