package auth

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var (
	ErrJWKSInvalid   = errors.New("invalid JWKS")
	ErrJWKSOverLimit = errors.New("JWKS limit exceeded")
)

const (
	DefaultMaxJWKSBytes = 64 << 10
	// The longest frozen credential lifetime is one hour and verifier skew is
	// one minute. Deployments may lower this but must not undercut issued tokens.
	DefaultRetainMissing = 61 * time.Minute
)

type JWKSFetcher interface {
	Fetch(context.Context) (io.ReadCloser, error)
}

type HTTPJWKSFetcher struct {
	endpoint *url.URL
	client   *http.Client
}

func NewHTTPJWKSFetcher(endpoint string, allowedHosts []string, transport http.RoundTripper) (*HTTPJWKSFetcher, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Hostname() == "" || parsed.Fragment != "" {
		return nil, ErrJWKSInvalid
	}
	allowed := false
	for _, host := range allowedHosts {
		if strings.EqualFold(strings.TrimSuffix(host, "."), strings.TrimSuffix(parsed.Hostname(), ".")) {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, ErrJWKSInvalid
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return ErrJWKSInvalid }}
	return &HTTPJWKSFetcher{endpoint: parsed, client: client}, nil
}

func (f *HTTPJWKSFetcher) Fetch(ctx context.Context) (io.ReadCloser, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, f.endpoint.String(), nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	response, err := f.client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, ErrJWKSInvalid
	}
	return response.Body, nil
}

type JWKSConfig struct {
	Fetcher         JWKSFetcher
	Clock           Clock
	TTL             time.Duration
	RetainMissing   time.Duration
	MaxBytes        int64
	MaxKeys         int
	MaxRetainedKeys int
	PersistencePath string
}

type JWKSCache struct {
	mu        sync.RWMutex
	refreshMu sync.Mutex
	config    JWKSConfig
	keys      map[string]cachedKey
}

type cachedKey struct {
	key       ed25519.PublicKey
	expiresAt time.Time
	retired   bool
}

func NewJWKSCache(config JWKSConfig) (*JWKSCache, error) {
	if config.TTL == 0 {
		config.TTL = 5 * time.Minute
	}
	if config.RetainMissing == 0 {
		config.RetainMissing = DefaultRetainMissing
	}
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultMaxJWKSBytes
	}
	if config.MaxKeys == 0 {
		config.MaxKeys = 64
	}
	if config.MaxRetainedKeys == 0 {
		config.MaxRetainedKeys = 128
	}
	if config.Fetcher == nil || config.Clock == nil || config.TTL <= 0 || config.RetainMissing < config.TTL || config.MaxBytes < 1 || config.MaxBytes > DefaultMaxJWKSBytes || config.MaxKeys < 1 || config.MaxKeys > 64 || config.MaxRetainedKeys < config.MaxKeys || config.MaxRetainedKeys > 256 {
		return nil, ErrJWKSInvalid
	}
	cache := &JWKSCache{config: config, keys: make(map[string]cachedKey)}
	cache.loadPersisted()
	return cache, nil
}

func (c *JWKSCache) Lookup(_ context.Context, keyID string) (ed25519.PublicKey, bool, error) {
	if keyID == "" {
		return nil, false, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.keys[keyID]
	if !ok || !c.config.Clock.Now().Before(entry.expiresAt) {
		return nil, false, nil
	}
	return append(ed25519.PublicKey(nil), entry.key...), true, nil
}

func (c *JWKSCache) Refresh(ctx context.Context) error {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	body, err := c.config.Fetcher.Fetch(ctx)
	if err != nil {
		return err
	}
	if body == nil {
		return ErrJWKSInvalid
	}
	defer body.Close()
	limited := io.LimitReader(body, c.config.MaxBytes+1)
	encoded, err := io.ReadAll(limited)
	if err != nil {
		return err
	}
	if int64(len(encoded)) > c.config.MaxBytes {
		return ErrJWKSOverLimit
	}
	refreshed, err := decodeJWKS(encoded, c.config.MaxKeys)
	if err != nil {
		return err
	}
	c.mu.Lock()
	now := c.config.Clock.Now()
	if err := c.persist(encoded, now); err != nil {
		c.mu.Unlock()
		return err
	}
	keys := make(map[string]cachedKey, len(refreshed)+len(c.keys))
	for keyID, key := range refreshed {
		keys[keyID] = cachedKey{key: key, expiresAt: now.Add(c.config.TTL)}
	}
	for keyID, previous := range c.keys {
		if _, current := refreshed[keyID]; current {
			continue
		}
		if previous.retired {
			if now.Before(previous.expiresAt) {
				keys[keyID] = previous
			}
		} else {
			keys[keyID] = cachedKey{key: previous.key, expiresAt: now.Add(c.config.RetainMissing), retired: true}
		}
	}
	if len(keys) > c.config.MaxRetainedKeys {
		c.mu.Unlock()
		return ErrJWKSOverLimit
	}
	c.keys = keys
	c.mu.Unlock()
	return nil
}

type persistedJWKS struct {
	Schema      string          `json:"schema"`
	ValidatedAt time.Time       `json:"validated_at"`
	Document    json.RawMessage `json:"document"`
}

func (c *JWKSCache) loadPersisted() {
	path := c.config.PersistencePath
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || info.Size() < 1 || info.Size() > c.config.MaxBytes+4096 {
		return
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var persisted persistedJWKS
	var extra any
	if decoder.Decode(&persisted) != nil || decoder.Decode(&extra) != io.EOF || persisted.Schema != "paperboat.jwks-cache/v1" || persisted.ValidatedAt.IsZero() {
		return
	}
	now := c.config.Clock.Now()
	if persisted.ValidatedAt.After(now.Add(time.Minute)) || !now.Before(persisted.ValidatedAt.Add(c.config.TTL)) {
		return
	}
	keys, err := decodeJWKS(persisted.Document, c.config.MaxKeys)
	if err != nil {
		return
	}
	expiresAt := persisted.ValidatedAt.Add(c.config.TTL)
	for keyID, key := range keys {
		c.keys[keyID] = cachedKey{key: key, expiresAt: expiresAt}
	}
}

func (c *JWKSCache) persist(document []byte, validatedAt time.Time) error {
	path := c.config.PersistencePath
	if path == "" {
		return nil
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrJWKSInvalid
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return ErrJWKSInvalid
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(persistedJWKS{Schema: "paperboat.jwks-cache/v1", ValidatedAt: validatedAt.UTC(), Document: append(json.RawMessage(nil), document...)})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".jwks-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
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
	return os.Rename(temporaryPath, path)
}

type jwksDocument struct {
	Keys []jwk `json:"keys"`
}
type jwk struct {
	KeyType   string `json:"kty"`
	Curve     string `json:"crv"`
	Use       string `json:"use"`
	Algorithm string `json:"alg"`
	KeyID     string `json:"kid"`
	X         string `json:"x"`
}

func decodeJWKS(encoded []byte, maxKeys int) (map[string]ed25519.PublicKey, error) {
	if err := rejectDuplicateKeys(encoded); err != nil {
		return nil, ErrJWKSInvalid
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var document jwksDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, ErrJWKSInvalid
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, ErrJWKSInvalid
	}
	if len(document.Keys) == 0 || len(document.Keys) > maxKeys {
		return nil, ErrJWKSOverLimit
	}
	keys := make(map[string]ed25519.PublicKey, len(document.Keys))
	for _, item := range document.Keys {
		if item.KeyType != "OKP" || item.Curve != "Ed25519" || item.Use != "sig" || item.Algorithm != "EdDSA" || len(item.KeyID) < 1 || len(item.KeyID) > 128 {
			return nil, ErrJWKSInvalid
		}
		if _, duplicate := keys[item.KeyID]; duplicate {
			return nil, ErrJWKSInvalid
		}
		x, err := base64.RawURLEncoding.DecodeString(item.X)
		if err != nil || len(x) != ed25519.PublicKeySize {
			return nil, ErrJWKSInvalid
		}
		keys[item.KeyID] = append(ed25519.PublicKey(nil), x...)
	}
	return keys, nil
}
