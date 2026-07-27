//go:build unix

package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/auth"
)

type revocationTokenSource interface {
	Token(context.Context) (string, error)
}
type revocationProofSource interface {
	Proof(context.Context, string, string, string, []byte) ([]byte, error)
}

type revocationRefreshService struct {
	endpoint    *url.URL
	tokens      revocationTokenSource
	proofs      revocationProofSource
	operationID func() (string, error)
	cache       *auth.RevocationCache
	client      *http.Client
	interval    time.Duration
	cancel      context.CancelFunc
	done        chan struct{}
}

func newRevocationRefreshService(endpoint string, tokens revocationTokenSource, proofs revocationProofSource, operationID func() (string, error), cache *auth.RevocationCache, transport http.RoundTripper, interval time.Duration) (*revocationRefreshService, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.RawQuery != "" || parsed.Fragment != "" || tokens == nil || proofs == nil || operationID == nil || cache == nil || transport == nil || interval <= 0 {
		return nil, ErrProductionInvalid
	}
	return &revocationRefreshService{endpoint: parsed, tokens: tokens, proofs: proofs, operationID: operationID, cache: cache, client: &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrProductionInvalid }}, interval: interval}, nil
}

func (s *revocationRefreshService) refresh(ctx context.Context) error {
	operationID, err := s.operationID()
	if err != nil || len(operationID) < 8 || len(operationID) > 128 {
		return errors.Join(ErrProductionInvalid, err)
	}
	body := []byte("{}")
	token, err := s.tokens.Token(ctx)
	if err != nil || token == "" || len(token) > 16<<10 {
		return errors.Join(ErrProductionInvalid, err)
	}
	proof, err := s.proofs.Proof(ctx, operationID, http.MethodPost, s.endpoint.Path, body)
	if err != nil || len(proof) == 0 || len(proof) > 16<<10 {
		return errors.Join(ErrProductionInvalid, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(io.LimitReader(response.Body, 1<<20+1))
	if err != nil || response.StatusCode < 200 || response.StatusCode >= 300 || len(encoded) > 1<<20 {
		return errors.Join(ErrProductionInvalid, err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var document struct {
		JTIs []string `json:"jtis"`
	}
	var extra any
	if decoder.Decode(&document) != nil || decoder.Decode(&extra) != io.EOF {
		return ErrProductionInvalid
	}
	return s.cache.Replace(document.JTIs)
}

func (s *revocationRefreshService) Start(context.Context) error {
	if s.cancel != nil {
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
				_ = s.refresh(attemptCtx)
				attemptCancel()
				timer.Reset(s.interval)
			}
		}
	}()
	return nil
}

func (s *revocationRefreshService) Shutdown(ctx context.Context) error {
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
