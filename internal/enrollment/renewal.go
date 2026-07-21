package enrollment

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type RenewingTokenConfig struct {
	ControlURL  string
	StateRoot   string
	Transport   http.RoundTripper
	RenewBefore time.Duration
	Timeout     time.Duration
	Clock       func() time.Time
	OperationID func() (string, error)
}

type RenewingTokenSource struct {
	config   RenewingTokenConfig
	endpoint *url.URL
	client   *http.Client
	mu       sync.Mutex
}

func NewRenewingTokenSource(config RenewingTokenConfig) (*RenewingTokenSource, error) {
	base, err := url.Parse(config.ControlURL)
	if err != nil || base.Scheme != "https" || base.User != nil || base.Hostname() == "" || base.RawQuery != "" || base.Fragment != "" || config.StateRoot == "" || config.Transport == nil || config.OperationID == nil {
		return nil, ErrInvalid
	}
	if config.RenewBefore == 0 {
		config.RenewBefore = 10 * time.Minute
	}
	if config.Timeout == 0 {
		config.Timeout = 15 * time.Second
	}
	if config.Clock == nil {
		config.Clock = func() time.Time { return time.Now().UTC() }
	}
	if config.RenewBefore <= 0 || config.RenewBefore >= time.Hour || config.Timeout <= 0 || config.Timeout > 30*time.Second {
		return nil, ErrInvalid
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/v1/helpers/renew"
	return &RenewingTokenSource{config: config, endpoint: base, client: &http.Client{Transport: config.Transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrInvalid }}}, nil
}

func (s *RenewingTokenSource) Token(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.config.Clock().UTC()
	current, err := LoadRuntimeIdentity(s.config.StateRoot, now)
	if err != nil {
		return "", err
	}
	if current.ExpiresAt.After(now.Add(s.config.RenewBefore)) {
		return current.Credential, nil
	}
	operationID, err := s.config.OperationID()
	if err != nil || len(operationID) < 8 || len(operationID) > 128 {
		return "", ErrInvalid
	}
	body, _ := json.Marshal(struct {
		OperationID string `json:"operation_id"`
	}{operationID})
	proof, err := (ProofSource{StateRoot: s.config.StateRoot, Clock: s.config.Clock}).Proof(ctx, operationID, http.MethodPost, s.endpoint.Path, body)
	if err != nil {
		return "", err
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, s.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer "+current.Credential)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 64<<10+1))
	if err != nil {
		return "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || len(encoded) > 64<<10 {
		return "", ErrInvalid
	}
	var envelope struct {
		Data RuntimeIdentity `json:"data"`
	}
	if strictJSON(encoded, &envelope) != nil {
		return "", ErrInvalid
	}
	renewed := envelope.Data
	renewed.Version = 1
	renewed.KeyID = current.KeyID
	if renewed.HelperID != current.HelperID || renewed.EnvironmentID != current.EnvironmentID || len(renewed.Credential) < 32 || !renewed.ExpiresAt.After(current.ExpiresAt) {
		return "", ErrInvalid
	}
	if err := writeIdentity(s.config.StateRoot, renewed); err != nil {
		return "", err
	}
	return renewed.Credential, nil
}

var _ interface {
	Token(context.Context) (string, error)
} = (*RenewingTokenSource)(nil)
