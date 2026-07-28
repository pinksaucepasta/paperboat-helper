package preview

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

var ErrControlClientInvalid = errors.New("invalid preview control client")

type ProofSource interface {
	Proof(context.Context, string, string, string, []byte) ([]byte, error)
}
type ControlTokenSource interface {
	Token(context.Context) (string, error)
}
type ControlRecord struct {
	ID            string     `json:"id"`
	EnvironmentID string     `json:"environment_id"`
	LogicalName   string     `json:"logical_name"`
	PreviewKey    string     `json:"preview_key"`
	URL           string     `json:"url"`
	TargetPort    int32      `json:"target_port"`
	State         string     `json:"state"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
}
type PreviewControl interface {
	List(context.Context) ([]ControlRecord, error)
	Register(context.Context, string, Target, bool, time.Duration, bool) (ControlRecord, error)
	Remove(context.Context, string) (ControlRecord, error)
}
type ControlClientConfig struct {
	Endpoint      string
	AllowedHosts  []string
	EnvironmentID string
	Tokens        ControlTokenSource
	Identities    ControlTokenSource
	Proofs        ProofSource
	Transport     http.RoundTripper
}
type ControlClient struct {
	endpoint *url.URL
	config   ControlClientConfig
	client   *http.Client
}

func NewControlClient(config ControlClientConfig) (*ControlClient, error) {
	u, err := url.Parse(config.Endpoint)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Hostname() == "" || config.EnvironmentID == "" || config.Tokens == nil || config.Identities == nil || config.Proofs == nil {
		return nil, ErrControlClientInvalid
	}
	ok := false
	for _, host := range config.AllowedHosts {
		if strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(u.Hostname(), ".")) {
			ok = true
		}
	}
	if !ok {
		return nil, ErrControlClientInvalid
	}
	transport := config.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	return &ControlClient{endpoint: u, config: config, client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrControlClientInvalid }}}, nil
}
func (c *ControlClient) List(ctx context.Context) ([]ControlRecord, error) {
	value, err := c.call(ctx, map[string]any{"action": "list"}, true)
	return value, err
}
func (c *ControlClient) Register(ctx context.Context, logical string, target Target, ack bool, lifetime time.Duration, indefinite bool) (ControlRecord, error) {
	payload := map[string]any{"action": "register", "logical_name": logical, "target_host": target.Host, "target_port": target.Port, "public_acknowledgement": ack}
	if lifetime > 0 {
		payload["duration_seconds"] = int64(lifetime / time.Second)
	}
	if indefinite {
		payload["indefinite"] = true
	}
	return c.callOne(ctx, payload)
}
func (c *ControlClient) Remove(ctx context.Context, logical string) (ControlRecord, error) {
	return c.callOne(ctx, map[string]any{"action": "remove", "logical_name": logical})
}
func (c *ControlClient) callOne(ctx context.Context, payload map[string]any) (ControlRecord, error) {
	values, err := c.call(ctx, payload, false)
	if err != nil {
		return ControlRecord{}, err
	}
	if len(values) != 1 {
		return ControlRecord{}, ErrControlClientInvalid
	}
	return values[0], nil
}
func (c *ControlClient) call(ctx context.Context, payload map[string]any, list bool) ([]ControlRecord, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 16)
	if _, err = rand.Read(raw); err != nil {
		return nil, err
	}
	op := "op_preview_" + base64.RawURLEncoding.EncodeToString(raw)
	token, err := c.config.Tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := c.config.Identities.Token(ctx)
	if err != nil {
		return nil, err
	}
	proof, err := c.config.Proofs.Proof(ctx, op, http.MethodPost, c.endpoint.Path, body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Paperboat-Helper-Identity", identity)
	req.Header.Set("X-Paperboat-Helper-Proof", base64.RawURLEncoding.EncodeToString(proof))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	res, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20+1))
	if err != nil || len(data) > 1<<20 || res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, fmt.Errorf("%w: status %d", ErrControlClientInvalid, res.StatusCode)
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if dec.Decode(&envelope) != nil || dec.Decode(&struct{}{}) != io.EOF || len(envelope.Data) == 0 {
		return nil, ErrControlClientInvalid
	}
	var values []ControlRecord
	if list {
		if json.Unmarshal(envelope.Data, &values) != nil {
			return nil, ErrControlClientInvalid
		}
	} else {
		var value ControlRecord
		if json.Unmarshal(envelope.Data, &value) != nil {
			return nil, ErrControlClientInvalid
		}
		values = []ControlRecord{value}
	}
	for _, v := range values {
		if v.EnvironmentID != c.config.EnvironmentID || v.PreviewKey == "" || v.URL == "" {
			return nil, ErrControlClientInvalid
		}
	}
	return values, nil
}
