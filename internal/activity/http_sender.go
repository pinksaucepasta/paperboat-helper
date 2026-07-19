package activity

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var ErrHTTPSenderInvalid = errors.New("invalid activity HTTPS sender")

type TokenSource interface {
	Token(context.Context) (string, error)
}

type HTTPSenderConfig struct {
	Endpoint         string
	AllowedHosts     []string
	Tokens           TokenSource
	Transport        http.RoundTripper
	MaxResponseBytes int64
}

type HTTPSender struct {
	endpoint         *url.URL
	tokens           TokenSource
	client           *http.Client
	maxResponseBytes int64
}

func NewHTTPSender(config HTTPSenderConfig) (*HTTPSender, error) {
	if config.MaxResponseBytes == 0 {
		config.MaxResponseBytes = 64 << 10
	}
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.Hostname() == "" || endpoint.Fragment != "" || config.Tokens == nil || config.MaxResponseBytes < 1 || config.MaxResponseBytes > 64<<10 {
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
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrHTTPSenderInvalid }}
	return &HTTPSender{endpoint: endpoint, tokens: config.Tokens, client: client, maxResponseBytes: config.MaxResponseBytes}, nil
}

func (s *HTTPSender) Send(ctx context.Context, batch Batch) error {
	if batch.ID == 0 || len(batch.Events) == 0 || len(batch.Events) > MaxBatchEvents || len(batch.Body) == 0 || len(batch.Body) > MaxBatchBytes {
		return ErrHTTPSenderInvalid
	}
	token, err := s.tokens.Token(ctx)
	if err != nil || token == "" || len(token) > 16<<10 {
		return errors.Join(ErrHTTPSenderInvalid, err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint.String(), bytes.NewReader(batch.Body))
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("X-Paperboat-Activity-Batch-ID", strconv.FormatUint(batch.ID, 10))
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	read, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, s.maxResponseBytes+1))
	if readErr != nil {
		return readErr
	}
	if read > s.maxResponseBytes {
		return ErrHTTPSenderInvalid
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("%w: status %d", ErrHTTPSenderInvalid, response.StatusCode)
	}
	return nil
}
