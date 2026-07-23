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
	"strconv"
	"strings"
)

var ErrHTTPSenderInvalid = errors.New("invalid preview HTTPS sender")

type ObservationRejectedError struct{ StatusCode int }

func (e *ObservationRejectedError) Error() string {
	return fmt.Sprintf("preview observation rejected with status %d", e.StatusCode)
}

func IsPermanentObservationError(err error) bool {
	var rejected *ObservationRejectedError
	if !errors.As(err, &rejected) {
		return false
	}
	switch rejected.StatusCode {
	case http.StatusBadRequest, http.StatusNotFound, http.StatusConflict, http.StatusGone, http.StatusUnprocessableEntity:
		return true
	default:
		return false
	}
}

type TokenSource interface {
	Token(context.Context) (string, error)
}

type HTTPSenderConfig struct {
	Endpoint         string
	AllowedHosts     []string
	Tokens           TokenSource
	Identities       TokenSource
	Proofs           ProofSource
	OperationID      func() (string, error)
	Transport        http.RoundTripper
	MaxResponseBytes int64
}

type HTTPSender struct {
	endpoint         *url.URL
	tokens           TokenSource
	identities       TokenSource
	proofs           ProofSource
	operationID      func() (string, error)
	client           *http.Client
	maxResponseBytes int64
}

func NewHTTPSender(config HTTPSenderConfig) (*HTTPSender, error) {
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 64 << 10
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.Fragment != "" || config.Tokens == nil || config.Identities == nil || config.Proofs == nil || config.OperationID == nil || config.MaxResponseBytes < 1 || config.MaxResponseBytes > 64<<10 {
		return nil, ErrHTTPSenderInvalid
	}
	allowed := false
	for _, host := range config.AllowedHosts {
		if strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(endpoint.Hostname(), ".")) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, ErrHTTPSenderInvalid
	}
	transport := config.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &HTTPSender{endpoint: endpoint, tokens: config.Tokens, identities: config.Identities, proofs: config.Proofs, operationID: config.OperationID, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrHTTPSenderInvalid }}, maxResponseBytes: config.MaxResponseBytes}, nil
}

func (s *HTTPSender) Send(ctx context.Context, observation Observation) error {
	if observation.Identity == "" || observation.EnvironmentID == "" || observation.LogicalName == "" || observation.Revision == 0 || observation.UpdatedAt.IsZero() || !validTarget(observation.Target) || !validObservationState(observation.State) {
		return ErrHTTPSenderInvalid
	}
	encoded, err := json.Marshal(observation)
	if err != nil || len(encoded) > 64<<10 {
		return ErrHTTPSenderInvalid
	}
	token, err := s.tokens.Token(ctx)
	if err != nil || token == "" || len(token) > 16<<10 {
		return errors.Join(ErrHTTPSenderInvalid, err)
	}
	identity, err := s.identities.Token(ctx)
	if err != nil || identity == "" || len(identity) > 16<<10 {
		return errors.Join(ErrHTTPSenderInvalid, err)
	}
	operationID, err := s.operationID()
	if err != nil || len(operationID) < 8 || len(operationID) > 128 {
		return errors.Join(ErrHTTPSenderInvalid, err)
	}
	proof, err := s.proofs.Proof(ctx, operationID, http.MethodPost, s.endpoint.Path, encoded)
	if err != nil || len(proof) == 0 || len(proof) > 16<<10 {
		return errors.Join(ErrHTTPSenderInvalid, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint.String(), bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Paperboat-Helper-Identity", identity)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Paperboat-Preview-Identity", observation.Identity)
	request.Header.Set("X-Paperboat-Preview-Revision", strconv.FormatUint(observation.Revision, 10))
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	read, err := io.Copy(io.Discard, io.LimitReader(response.Body, s.maxResponseBytes+1))
	if err != nil {
		return err
	}
	if read > s.maxResponseBytes || response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.Join(ErrHTTPSenderInvalid, &ObservationRejectedError{StatusCode: response.StatusCode})
	}
	return nil
}

func validObservationState(state State) bool {
	switch state {
	case Registering, Ready, Degraded, Offline, Expired, Removed:
		return true
	default:
		return false
	}
}
