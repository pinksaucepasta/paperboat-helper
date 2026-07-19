package update

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrManifestInvalid  = errors.New("update manifest invalid")
	ErrSignatureInvalid = errors.New("update signature invalid")
	ErrIncompatible     = errors.New("update incompatible")
	ErrArtifactInvalid  = errors.New("update artifact invalid")
	ErrHealthCheck      = errors.New("update health check failed")
	ErrRollback         = errors.New("update rollback failed")
)

type Envelope struct {
	KeyID     string `json:"key_id"`
	Manifest  string `json:"manifest_base64"`
	Signature string `json:"signature_base64"`
}
type Manifest struct {
	Version     string              `json:"version"`
	Channel     string              `json:"channel"`
	PublishedAt time.Time           `json:"published_at"`
	Revoked     bool                `json:"revoked"`
	MinProtocol string              `json:"min_protocol"`
	MaxProtocol string              `json:"max_protocol"`
	MinStore    int                 `json:"min_store"`
	MaxStore    int                 `json:"max_store"`
	Artifacts   map[string]Artifact `json:"artifacts"`
}
type Artifact struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}
type Fetcher interface {
	Fetch(context.Context, string) (io.ReadCloser, error)
}
type HealthChecker interface {
	Check(context.Context, string, string) error
}
type Config struct {
	StateRoot        string
	InstallPath      string
	CurrentVersion   string
	Channel          string
	ProtocolVersion  string
	StoreVersion     int
	TrustedKeys      map[string]ed25519.PublicKey
	AllowedHosts     map[string]bool
	Fetcher          Fetcher
	Health           HealthChecker
	MaxArtifactBytes int64
	Clock            func() time.Time
}
type Manager struct {
	mu           sync.Mutex
	config       Config
	journalPath  string
	previousPath string
	trust        *TrustStore
}
type journal struct {
	State           string `json:"state"`
	Version         string `json:"version"`
	PreviousVersion string `json:"previous_version,omitempty"`
	StagedPath      string `json:"staged_path,omitempty"`
}

func New(ctx context.Context, config Config) (*Manager, error) {
	if !filepath.IsAbs(config.StateRoot) || !filepath.IsAbs(config.InstallPath) || config.CurrentVersion == "" || config.Channel == "" || config.ProtocolVersion == "" || config.StoreVersion < 1 || len(config.TrustedKeys) == 0 || len(config.AllowedHosts) == 0 || config.Fetcher == nil || config.Health == nil {
		return nil, ErrManifestInvalid
	}
	if config.MaxArtifactBytes == 0 {
		config.MaxArtifactBytes = 256 << 20
	}
	if config.MaxArtifactBytes < 1 {
		return nil, ErrManifestInvalid
	}
	if err := os.MkdirAll(config.StateRoot, 0o700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(config.StateRoot); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrArtifactInvalid
	}
	if err := os.Chmod(config.StateRoot, 0o700); err != nil {
		return nil, err
	}
	installDirectory := filepath.Dir(config.InstallPath)
	if err := os.MkdirAll(installDirectory, 0o700); err != nil {
		return nil, err
	}
	if info, err := os.Lstat(installDirectory); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrArtifactInvalid
	}
	if err := safeOptionalRegular(config.InstallPath); err != nil {
		return nil, err
	}
	trust, err := newTrustStore(filepath.Join(config.StateRoot, "update-trust.json"), config.TrustedKeys, config.Clock)
	if err != nil {
		return nil, err
	}
	manager := &Manager{config: config, journalPath: filepath.Join(config.StateRoot, "update-journal.json"), previousPath: config.InstallPath + ".previous", trust: trust}
	if err := manager.recover(ctx); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) ApplyTrustBundle(envelope []byte) error { return m.trust.Apply(envelope) }

func (m *Manager) FetchAndApplyTrustBundle(ctx context.Context, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || !m.config.AllowedHosts[parsed.Hostname()] {
		return ErrTrustInvalid
	}
	body, err := m.config.Fetcher.Fetch(ctx, rawURL)
	if err != nil {
		return err
	}
	if body == nil {
		return ErrTrustInvalid
	}
	defer body.Close()
	encoded, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: body}, (64<<10)+1))
	if err != nil {
		return err
	}
	if len(encoded) > 64<<10 {
		return ErrTrustInvalid
	}
	return m.ApplyTrustBundle(encoded)
}

func (m *Manager) Apply(ctx context.Context, envelopeBytes []byte) (Manifest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	manifest, artifact, err := m.verify(envelopeBytes)
	if err != nil {
		return Manifest{}, err
	}
	staged, err := m.stage(ctx, artifact)
	if err != nil {
		return Manifest{}, err
	}
	keepStaged := true
	defer func() {
		if keepStaged {
			_ = os.Remove(staged)
		}
	}()
	if err := m.config.Health.Check(ctx, staged, manifest.Version); err != nil {
		return Manifest{}, fmt.Errorf("%w: staged: %v", ErrHealthCheck, err)
	}
	if err := verifyFile(staged, artifact); err != nil {
		return Manifest{}, err
	}
	entry := journal{State: "staged", Version: manifest.Version, PreviousVersion: m.config.CurrentVersion, StagedPath: staged}
	if err := m.writeJournal(entry); err != nil {
		return Manifest{}, err
	}
	entry.State = "backing_up"
	if err := m.writeJournal(entry); err != nil {
		return Manifest{}, err
	}
	if err := safeOptionalRegular(m.previousPath); err != nil {
		return Manifest{}, err
	}
	if err := os.Remove(m.previousPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	if _, err := os.Stat(m.config.InstallPath); err == nil {
		if err := os.Rename(m.config.InstallPath, m.previousPath); err != nil {
			return Manifest{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	if err := syncDirectory(filepath.Dir(m.config.InstallPath)); err != nil {
		_ = m.rollback()
		return Manifest{}, err
	}
	entry.State = "activating"
	if err := m.writeJournal(entry); err != nil {
		_ = m.rollback()
		return Manifest{}, err
	}
	if err := os.Rename(staged, m.config.InstallPath); err != nil {
		_ = m.rollback()
		return Manifest{}, err
	}
	keepStaged = false
	if err := syncDirectory(filepath.Dir(m.config.InstallPath)); err != nil {
		_ = m.rollback()
		return Manifest{}, err
	}
	entry.State = "checking"
	entry.StagedPath = ""
	if err := m.writeJournal(entry); err != nil {
		_ = m.rollback()
		return Manifest{}, err
	}
	if err := m.config.Health.Check(ctx, m.config.InstallPath, manifest.Version); err != nil {
		if rollbackErr := m.rollback(); rollbackErr != nil {
			return Manifest{}, errors.Join(fmt.Errorf("%w: %v", ErrHealthCheck, err), rollbackErr)
		}
		return Manifest{}, fmt.Errorf("%w: %v", ErrHealthCheck, err)
	}
	entry.State = "committed"
	if err := m.writeJournal(entry); err != nil {
		return Manifest{}, err
	}
	m.config.CurrentVersion = manifest.Version
	return manifest, nil
}

func (m *Manager) verify(envelopeBytes []byte) (Manifest, Artifact, error) {
	var envelope Envelope
	if err := strictJSON(envelopeBytes, &envelope); err != nil {
		return Manifest{}, Artifact{}, ErrManifestInvalid
	}
	key, ok := m.trust.Lookup(envelope.KeyID)
	if !ok || len(key) != ed25519.PublicKeySize {
		return Manifest{}, Artifact{}, ErrSignatureInvalid
	}
	manifestBytes, err := base64.RawURLEncoding.DecodeString(envelope.Manifest)
	if err != nil || len(manifestBytes) > 1<<20 {
		return Manifest{}, Artifact{}, ErrManifestInvalid
	}
	signature, err := base64.RawURLEncoding.DecodeString(envelope.Signature)
	if err != nil || !ed25519.Verify(key, manifestBytes, signature) {
		return Manifest{}, Artifact{}, ErrSignatureInvalid
	}
	var manifest Manifest
	if err := strictJSON(manifestBytes, &manifest); err != nil {
		return Manifest{}, Artifact{}, ErrManifestInvalid
	}
	if manifest.PublishedAt.IsZero() || manifest.Revoked || m.trust.VersionRevoked(manifest.Version) || manifest.Channel != m.config.Channel || !validVersion(manifest.Version) || compareVersion(manifest.Version, m.config.CurrentVersion) <= 0 || !versionInRange(m.config.ProtocolVersion, manifest.MinProtocol, manifest.MaxProtocol) || m.config.StoreVersion < manifest.MinStore || m.config.StoreVersion > manifest.MaxStore {
		return Manifest{}, Artifact{}, ErrIncompatible
	}
	artifact, ok := manifest.Artifacts[runtime.GOOS+"-"+runtime.GOARCH]
	if !ok || artifact.Size < 1 || artifact.Size > m.config.MaxArtifactBytes || len(artifact.SHA256) != 64 {
		return Manifest{}, Artifact{}, ErrArtifactInvalid
	}
	parsed, err := url.Parse(artifact.URL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || !m.config.AllowedHosts[parsed.Hostname()] {
		return Manifest{}, Artifact{}, ErrArtifactInvalid
	}
	if _, err := hex.DecodeString(artifact.SHA256); err != nil {
		return Manifest{}, Artifact{}, ErrArtifactInvalid
	}
	return manifest, artifact, nil
}

func (m *Manager) stage(ctx context.Context, artifact Artifact) (string, error) {
	body, err := m.config.Fetcher.Fetch(ctx, artifact.URL)
	if err != nil {
		return "", err
	}
	defer body.Close()
	file, err := os.CreateTemp(filepath.Dir(m.config.InstallPath), ".paperboat-helper-update-*")
	if err != nil {
		return "", err
	}
	path := file.Name()
	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(path)
		}
	}()
	if err := file.Chmod(0o700); err != nil {
		return "", err
	}
	hash := sha256.New()
	written, err := io.CopyBuffer(io.MultiWriter(file, hash), io.LimitReader(&contextReader{ctx: ctx, reader: body}, m.config.MaxArtifactBytes+1), make([]byte, 64<<10))
	if err != nil {
		return "", err
	}
	if written != artifact.Size || written > m.config.MaxArtifactBytes || hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(artifact.SHA256) {
		return "", ErrArtifactInvalid
	}
	if err := file.Sync(); err != nil {
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}
	if err := syncDirectory(filepath.Dir(m.config.InstallPath)); err != nil {
		return "", err
	}
	success = true
	return path, nil
}

func (m *Manager) recover(ctx context.Context) error {
	data, err := os.ReadFile(m.journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var entry journal
	if err := strictJSON(data, &entry); err != nil {
		return ErrManifestInvalid
	}
	if err := validateJournal(entry, m.config.CurrentVersion, filepath.Dir(m.config.InstallPath)); err != nil {
		return err
	}
	switch entry.State {
	case "staged":
		if err := removeStaged(entry.StagedPath, filepath.Dir(m.config.InstallPath)); err != nil {
			return err
		}
		return removeAndSync(m.journalPath, m.config.StateRoot)
	case "backing_up":
		if _, previousErr := os.Stat(m.previousPath); previousErr == nil {
			if err := m.rollback(); err != nil {
				return err
			}
			if err := removeStaged(entry.StagedPath, filepath.Dir(m.config.InstallPath)); err != nil {
				return err
			}
		} else if errors.Is(previousErr, os.ErrNotExist) {
			if err := removeStaged(entry.StagedPath, filepath.Dir(m.config.InstallPath)); err != nil {
				return err
			}
			return removeAndSync(m.journalPath, m.config.StateRoot)
		} else {
			return previousErr
		}
		return nil
	case "activating", "checking":
		if err := m.rollback(); err != nil {
			return err
		}
		if entry.StagedPath != "" {
			if err := removeStaged(entry.StagedPath, filepath.Dir(m.config.InstallPath)); err != nil {
				return err
			}
		}
		return nil
	case "committed":
		if err := safeOptionalRegular(m.config.InstallPath); err != nil {
			return err
		}
		if err := m.config.Health.Check(ctx, m.config.InstallPath, entry.Version); err != nil {
			if rollbackErr := m.rollback(); rollbackErr != nil {
				return errors.Join(fmt.Errorf("%w: startup: %v", ErrHealthCheck, err), rollbackErr)
			}
			m.config.CurrentVersion = entry.PreviousVersion
			return nil
		}
		m.config.CurrentVersion = entry.Version
		return nil
	default:
		return ErrManifestInvalid
	}
}
func (m *Manager) rollback() error {
	if err := safeRequiredRegular(m.previousPath); err != nil {
		return fmt.Errorf("%w: %v", ErrRollback, err)
	}
	if err := os.Remove(m.config.InstallPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %v", ErrRollback, err)
	}
	if _, err := os.Stat(m.previousPath); err == nil {
		if err := os.Rename(m.previousPath, m.config.InstallPath); err != nil {
			return fmt.Errorf("%w: %v", ErrRollback, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: %v", ErrRollback, err)
	} else {
		return ErrRollback
	}
	if err := syncDirectory(filepath.Dir(m.config.InstallPath)); err != nil {
		return fmt.Errorf("%w: %v", ErrRollback, err)
	}
	return removeAndSync(m.journalPath, m.config.StateRoot)
}
func (m *Manager) writeJournal(entry journal) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(m.config.StateRoot, ".update-journal-*")
	if err != nil {
		return err
	}
	path := temporary.Name()
	defer os.Remove(path)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
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
	if err := os.Rename(path, m.journalPath); err != nil {
		return err
	}
	return syncDirectory(m.config.StateRoot)
}

type HTTPFetcher struct {
	Client       *http.Client
	AllowedHosts map[string]bool
}

func (f HTTPFetcher) Fetch(ctx context.Context, rawURL string) (io.ReadCloser, error) {
	if f.Client == nil {
		return nil, ErrArtifactInvalid
	}
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || !f.AllowedHosts[parsed.Hostname()] {
		return nil, ErrArtifactInvalid
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	response, err := f.Client.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		response.Body.Close()
		return nil, ErrArtifactInvalid
	}
	if response.Request == nil || response.Request.URL.Scheme != "https" || !f.AllowedHosts[response.Request.URL.Hostname()] {
		response.Body.Close()
		return nil, ErrArtifactInvalid
	}
	return response.Body, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(buffer)
	}
}
func strictJSON(data []byte, target any) error {
	if err := rejectDuplicateKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrManifestInvalid
	}
	return nil
}

func rejectDuplicateKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			seen := make(map[string]bool)
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return ErrManifestInvalid
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err := decoder.Token()
			return err
		default:
			return ErrManifestInvalid
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return ErrManifestInvalid
	}
	return nil
}
func safeStagedPath(path, directory string) bool {
	relative, err := filepath.Rel(directory, path)
	return err == nil && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && relative != ".." && strings.HasPrefix(filepath.Base(path), ".paperboat-helper-update-")
}

func validateJournal(entry journal, currentVersion, installDirectory string) error {
	if !validVersion(entry.Version) || !validVersion(entry.PreviousVersion) || compareVersion(entry.Version, entry.PreviousVersion) <= 0 || currentVersion != entry.PreviousVersion && currentVersion != entry.Version {
		return ErrManifestInvalid
	}
	switch entry.State {
	case "staged", "backing_up", "activating":
		if !safeStagedPath(entry.StagedPath, installDirectory) {
			return ErrManifestInvalid
		}
	case "checking", "committed":
		if entry.StagedPath != "" {
			return ErrManifestInvalid
		}
	default:
		return ErrManifestInvalid
	}
	return nil
}

func removeStaged(path, directory string) error {
	if !safeStagedPath(path, directory) {
		return ErrManifestInvalid
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(directory)
}

func removeAndSync(path, directory string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(directory)
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func verifyFile(path string, artifact Artifact) error {
	if err := safeRequiredRegular(path); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, artifact.Size+1))
	if err != nil || written != artifact.Size || hex.EncodeToString(hash.Sum(nil)) != strings.ToLower(artifact.SHA256) {
		return ErrArtifactInvalid
	}
	return nil
}

func safeOptionalRegular(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return ErrArtifactInvalid
	}
	return nil
}

func safeRequiredRegular(path string) error {
	if err := safeOptionalRegular(path); err != nil {
		return err
	}
	if _, err := os.Stat(path); err != nil {
		return err
	}
	return nil
}
func validVersion(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}
func compareVersion(left, right string) int {
	l := strings.Split(left, ".")
	r := strings.Split(right, ".")
	if len(l) != 3 || len(r) != 3 {
		return -1
	}
	for i := 0; i < 3; i++ {
		lv, _ := strconv.Atoi(l[i])
		rv, _ := strconv.Atoi(r[i])
		if lv < rv {
			return -1
		}
		if lv > rv {
			return 1
		}
	}
	return 0
}
func versionInRange(version, minVersion, maxVersion string) bool {
	return validDotted(version) && validDotted(minVersion) && validDotted(maxVersion) && compareDotted(version, minVersion) >= 0 && compareDotted(version, maxVersion) <= 0
}

func validDotted(version string) bool {
	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 3 {
		return false
	}
	for _, part := range parts {
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}

func compareDotted(left, right string) int {
	l, r := strings.Split(left, "."), strings.Split(right, ".")
	for len(l) < 3 {
		l = append(l, "0")
	}
	for len(r) < 3 {
		r = append(r, "0")
	}
	return compareVersion(strings.Join(l, "."), strings.Join(r, "."))
}
