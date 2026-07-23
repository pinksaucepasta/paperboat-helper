package preview

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var ErrCredentialSourceInvalid = errors.New("invalid preview credential source")

type CredentialSourceConfig struct {
	Endpoint      string
	AllowedHosts  []string
	Identities    TokenSource
	Proofs        ProofSource
	OperationID   func() (string, error)
	Transport     http.RoundTripper
	Clock         func() time.Time
	RefreshBefore time.Duration
}

type CredentialSource struct {
	endpoint      *url.URL
	identities    TokenSource
	proofs        ProofSource
	operationID   func() (string, error)
	client        *http.Client
	clock         func() time.Time
	refreshBefore time.Duration
	mu            sync.Mutex
	token         string
	expiresAt     time.Time
}

func NewCredentialSource(config CredentialSourceConfig) (*CredentialSource, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.RefreshBefore == 0 {
		config.RefreshBefore = 30 * time.Second
	}
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.Fragment != "" || config.Identities == nil || config.Proofs == nil || config.OperationID == nil || config.RefreshBefore < 0 || config.RefreshBefore >= 5*time.Minute {
		return nil, ErrCredentialSourceInvalid
	}
	allowed := false
	for _, host := range config.AllowedHosts {
		if strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(endpoint.Hostname(), ".")) {
			allowed = true
		}
	}
	if !allowed {
		return nil, ErrCredentialSourceInvalid
	}
	transport := config.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &CredentialSource{endpoint: endpoint, identities: config.Identities, proofs: config.Proofs, operationID: config.OperationID, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrCredentialSourceInvalid }}, clock: config.Clock, refreshBefore: config.RefreshBefore}, nil
}

func (s *CredentialSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && s.clock().UTC().Add(s.refreshBefore).Before(s.expiresAt) {
		return s.token, nil
	}
	body := []byte("{}")
	identity, err := s.identities.Token(ctx)
	if err != nil || identity == "" || len(identity) > 16<<10 {
		return "", errors.Join(ErrCredentialSourceInvalid, err)
	}
	operationID, err := s.operationID()
	if err != nil {
		return "", errors.Join(ErrCredentialSourceInvalid, err)
	}
	proof, err := s.proofs.Proof(ctx, operationID, http.MethodPost, s.endpoint.Path, body)
	if err != nil || len(proof) == 0 || len(proof) > 16<<10 {
		return "", errors.Join(ErrCredentialSourceInvalid, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+identity)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	if err != nil || len(data) > 64<<10 || response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("%w: status %d", ErrCredentialSourceInvalid, response.StatusCode)
	}
	var envelope struct {
		Data struct {
			Credential string    `json:"credential"`
			ExpiresAt  time.Time `json:"expires_at"`
		} `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF || envelope.Data.Credential == "" || len(envelope.Data.Credential) > 16<<10 || !envelope.Data.ExpiresAt.After(s.clock().UTC().Add(s.refreshBefore)) {
		return "", ErrCredentialSourceInvalid
	}
	s.token, s.expiresAt = envelope.Data.Credential, envelope.Data.ExpiresAt.UTC()
	return s.token, nil
}
