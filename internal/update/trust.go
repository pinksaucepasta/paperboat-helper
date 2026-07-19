package update

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var ErrTrustInvalid = errors.New("update trust bundle invalid")

const maxTrustBundles = 32

type TrustEnvelope struct {
	SignerKeyID string `json:"signer_key_id"`
	Bundle      string `json:"bundle_base64"`
	Signature   string `json:"signature_base64"`
}

type TrustBundle struct {
	Generation      uint64            `json:"generation"`
	IssuedAt        time.Time         `json:"issued_at"`
	Keys            map[string]string `json:"keys"`
	RevokedKeyIDs   []string          `json:"revoked_key_ids"`
	RevokedVersions []string          `json:"revoked_versions"`
}

type trustFile struct {
	Envelopes []TrustEnvelope `json:"envelopes"`
}

type TrustStore struct {
	mu              sync.RWMutex
	path            string
	initial         map[string]ed25519.PublicKey
	keys            map[string]ed25519.PublicKey
	revokedKeys     map[string]bool
	revokedVersions map[string]bool
	generation      uint64
	envelopes       []TrustEnvelope
	now             func() time.Time
}

func newTrustStore(path string, initial map[string]ed25519.PublicKey, now func() time.Time) (*TrustStore, error) {
	if len(initial) == 0 || !filepath.IsAbs(path) {
		return nil, ErrTrustInvalid
	}
	for keyID, key := range initial {
		if keyID == "" || len(keyID) > 128 || len(key) != ed25519.PublicKeySize {
			return nil, ErrTrustInvalid
		}
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	store := &TrustStore{path: path, initial: cloneKeys(initial), now: now}
	store.reset()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return store, nil
	}
	if err != nil {
		return nil, err
	}
	var persisted trustFile
	if strictJSON(data, &persisted) != nil || len(persisted.Envelopes) > maxTrustBundles {
		return nil, ErrTrustInvalid
	}
	for _, envelope := range persisted.Envelopes {
		if err := store.applyLocked(envelope, false); err != nil {
			return nil, err
		}
	}
	return store, nil
}

func (s *TrustStore) Apply(envelopeBytes []byte) error {
	var envelope TrustEnvelope
	if strictJSON(envelopeBytes, &envelope) != nil {
		return ErrTrustInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyLocked(envelope, true)
}

func (s *TrustStore) Lookup(keyID string) (ed25519.PublicKey, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[keyID]
	if !ok || s.revokedKeys[keyID] {
		return nil, false
	}
	return append(ed25519.PublicKey(nil), key...), true
}

func (s *TrustStore) VersionRevoked(version string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revokedVersions[version]
}

func (s *TrustStore) applyLocked(envelope TrustEnvelope, persist bool) error {
	if len(s.envelopes) >= maxTrustBundles || envelope.SignerKeyID == "" || s.revokedKeys[envelope.SignerKeyID] {
		return ErrTrustInvalid
	}
	signer, ok := s.keys[envelope.SignerKeyID]
	if !ok {
		return ErrTrustInvalid
	}
	bundleBytes, err := base64.RawURLEncoding.DecodeString(envelope.Bundle)
	if err != nil || len(bundleBytes) > 64<<10 {
		return ErrTrustInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil || !ed25519.Verify(signer, bundleBytes, signature) {
		return ErrTrustInvalid
	}
	var bundle TrustBundle
	if strictJSON(bundleBytes, &bundle) != nil || bundle.Generation <= s.generation || bundle.IssuedAt.IsZero() || bundle.IssuedAt.After(s.now().Add(time.Minute)) || len(bundle.Keys) == 0 || len(bundle.Keys) > 32 || bundle.RevokedKeyIDs == nil || bundle.RevokedVersions == nil {
		return ErrTrustInvalid
	}
	keys := make(map[string]ed25519.PublicKey, len(bundle.Keys))
	for keyID, encoded := range bundle.Keys {
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if keyID == "" || len(keyID) > 128 || err != nil || len(decoded) != ed25519.PublicKeySize {
			return ErrTrustInvalid
		}
		keys[keyID] = append(ed25519.PublicKey(nil), decoded...)
	}
	revokedKeys, ok := cumulativeSet(bundle.RevokedKeyIDs, s.revokedKeys)
	if !ok {
		return ErrTrustInvalid
	}
	revokedVersions, ok := cumulativeSet(bundle.RevokedVersions, s.revokedVersions)
	if !ok {
		return ErrTrustInvalid
	}
	for version := range revokedVersions {
		if !validVersion(version) {
			return ErrTrustInvalid
		}
	}
	active := false
	for keyID := range keys {
		if !revokedKeys[keyID] {
			active = true
			break
		}
	}
	if !active {
		return ErrTrustInvalid
	}
	previousKeys, previousRevokedKeys, previousRevokedVersions, previousGeneration := s.keys, s.revokedKeys, s.revokedVersions, s.generation
	s.keys, s.revokedKeys, s.revokedVersions, s.generation = keys, revokedKeys, revokedVersions, bundle.Generation
	s.envelopes = append(s.envelopes, envelope)
	if persist {
		if err := s.persistLocked(); err != nil {
			s.keys, s.revokedKeys, s.revokedVersions, s.generation = previousKeys, previousRevokedKeys, previousRevokedVersions, previousGeneration
			s.envelopes = s.envelopes[:len(s.envelopes)-1]
			return err
		}
	}
	return nil
}

func (s *TrustStore) persistLocked() error {
	encoded, err := json.Marshal(trustFile{Envelopes: s.envelopes})
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.path), ".update-trust-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
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
	if err := os.Rename(path, s.path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(s.path))
}

func (s *TrustStore) reset() {
	s.keys = cloneKeys(s.initial)
	s.revokedKeys = make(map[string]bool)
	s.revokedVersions = make(map[string]bool)
	s.generation = 0
	s.envelopes = nil
}

func cloneKeys(values map[string]ed25519.PublicKey) map[string]ed25519.PublicKey {
	result := make(map[string]ed25519.PublicKey, len(values))
	for keyID, key := range values {
		result[keyID] = append(ed25519.PublicKey(nil), key...)
	}
	return result
}

func cumulativeSet(values []string, previous map[string]bool) (map[string]bool, bool) {
	result := make(map[string]bool, len(values))
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	for index, value := range sorted {
		if value == "" || index > 0 && value == sorted[index-1] {
			return nil, false
		}
		result[value] = true
	}
	for value := range previous {
		if !result[value] {
			return nil, false
		}
	}
	return result, true
}
