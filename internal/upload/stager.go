package upload

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	helperconfig "github.com/pinksaucepasta/paperboat-helper/internal/config"
)

const (
	DefaultMaxBytes  = int64(20 << 20)
	DefaultRetention = 24 * time.Hour
)

type Code string

const (
	InvalidPath        Code = "invalid_path"
	InvalidSize        Code = "invalid_size"
	MIMEMismatch       Code = "mime_mismatch"
	ResourceLimit      Code = "resource_limit"
	StorageUnavailable Code = "storage_unavailable"
)

type Error struct {
	Code  Code
	Cause error
}

func (e *Error) Error() string {
	if e.Cause == nil {
		return string(e.Code)
	}
	return string(e.Code) + ": " + e.Cause.Error()
}
func (e *Error) Unwrap() error { return e.Cause }

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Config struct {
	Root          string
	MaxBytes      int64
	Retention     time.Duration
	MaxConcurrent int
	Clock         Clock
	Random        io.Reader
	Metrics       interface {
		Record(string, float64, map[string]string) error
	}
}

type Stager struct {
	config    Config
	slots     chan struct{}
	cleanupMu sync.Mutex
}

type Request struct {
	EnvironmentID    string
	DisplayName      string
	DeclaredMIME     string
	DeclaredSize     int64
	CredentialExpiry time.Time
	Body             io.Reader
	ExpectedSHA256   string
}

type Result struct {
	Path      string    `json:"path"`
	MIME      string    `json:"mime"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256"`
	ExpiresAt time.Time `json:"expires_at"`
}

func (s *Stager) ResourceCounts() map[string]uint64 {
	return map[string]uint64{"uploads": uint64(len(s.slots))}
}

type metadata struct {
	Path      string    `json:"path"`
	MIME      string    `json:"mime"`
	Bytes     int64     `json:"bytes"`
	SHA256    string    `json:"sha256"`
	ExpiresAt time.Time `json:"expires_at"`
}

func New(config Config) (*Stager, error) {
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultMaxBytes
	}
	if config.Retention == 0 {
		config.Retention = DefaultRetention
	}
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = helperconfig.DefaultResources.MaxConcurrentUploads
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if !filepath.IsAbs(config.Root) || config.MaxBytes < 1 || config.MaxBytes > DefaultMaxBytes || config.Retention <= 0 || config.Retention > DefaultRetention || config.MaxConcurrent < 1 {
		return nil, &Error{Code: InvalidPath}
	}
	if err := ensurePrivateDirectory(config.Root); err != nil {
		return nil, &Error{Code: StorageUnavailable, Cause: err}
	}
	return &Stager{config: config, slots: make(chan struct{}, config.MaxConcurrent)}, nil
}

func (s *Stager) Stage(ctx context.Context, request Request) (result Result, err error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return Result{}, &Error{Code: ResourceLimit}
	}
	if request.Body == nil || request.DeclaredSize < 1 || request.DeclaredSize > s.config.MaxBytes {
		return Result{}, &Error{Code: InvalidSize}
	}
	if !safeSegment(request.EnvironmentID) || !safeName(request.DisplayName) {
		return Result{}, &Error{Code: InvalidPath}
	}
	if _, ok := mimeExtensions[request.DeclaredMIME]; !ok {
		return Result{}, &Error{Code: MIMEMismatch}
	}
	directory := filepath.Join(s.config.Root, request.EnvironmentID)
	if err := ensurePrivateDirectory(directory); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	temporary, err := os.CreateTemp(directory, ".upload-*")
	if err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	tempPath := temporary.Name()
	publishedPath := ""
	metadataPath := ""
	defer func() {
		temporary.Close()
		os.Remove(tempPath)
		if err != nil {
			if publishedPath != "" {
				os.Remove(publishedPath)
			}
			if metadataPath != "" {
				os.Remove(metadataPath)
			}
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	hash := sha256.New()
	header := &prefixWriter{max: 512}
	limited := io.LimitReader(&contextReader{ctx: ctx, reader: request.Body}, s.config.MaxBytes+1)
	written, copyErr := io.CopyBuffer(io.MultiWriter(temporary, hash, header), limited, make([]byte, 32<<10))
	if copyErr != nil {
		return Result{}, classifyIO(copyErr)
	}
	if written != request.DeclaredSize || written > s.config.MaxBytes {
		return Result{}, &Error{Code: InvalidSize}
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if request.ExpectedSHA256 != "" && (!validSHA256(request.ExpectedSHA256) || digest != request.ExpectedSHA256) {
		return Result{}, &Error{Code: InvalidSize, Cause: errors.New("content digest mismatch")}
	}
	detected := http.DetectContentType(header.bytes)
	if detected != request.DeclaredMIME {
		return Result{}, &Error{Code: MIMEMismatch}
	}
	if err = temporary.Sync(); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	if err = temporary.Close(); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	var random [16]byte
	if _, err = io.ReadFull(s.config.Random, random[:]); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	filename := hex.EncodeToString(random[:]) + mimeExtensions[detected]
	publishedPath = filepath.Join(directory, filename)
	if err = os.Link(tempPath, publishedPath); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	expires := s.config.Clock.Now().Add(s.config.Retention)
	if !request.CredentialExpiry.IsZero() && request.CredentialExpiry.Before(expires) {
		expires = request.CredentialExpiry
	}
	relative := filepath.Join(request.EnvironmentID, filename)
	result = Result{Path: relative, MIME: detected, Bytes: written, SHA256: digest, ExpiresAt: expires}
	meta := metadata{Path: relative, MIME: detected, Bytes: written, SHA256: result.SHA256, ExpiresAt: expires}
	metaBytes, marshalErr := json.Marshal(meta)
	if marshalErr != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: marshalErr}
	}
	metaTemp, createErr := os.CreateTemp(directory, ".metadata-*")
	if createErr != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: createErr}
	}
	metaTempPath := metaTemp.Name()
	defer os.Remove(metaTempPath)
	if chmodErr := metaTemp.Chmod(0o600); chmodErr != nil {
		metaTemp.Close()
		return Result{}, &Error{Code: StorageUnavailable, Cause: chmodErr}
	}
	if _, writeErr := metaTemp.Write(metaBytes); writeErr != nil {
		metaTemp.Close()
		return Result{}, &Error{Code: StorageUnavailable, Cause: writeErr}
	}
	if syncErr := metaTemp.Sync(); syncErr != nil {
		metaTemp.Close()
		return Result{}, &Error{Code: StorageUnavailable, Cause: syncErr}
	}
	if closeErr := metaTemp.Close(); closeErr != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: closeErr}
	}
	metadataPath = publishedPath + ".meta"
	if err = os.Link(metaTempPath, metadataPath); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	if err = syncDirectory(directory); err != nil {
		return Result{}, &Error{Code: StorageUnavailable, Cause: err}
	}
	err = nil
	return result, nil
}

func (s *Stager) Remove(relativePath string) error {
	if filepath.IsAbs(relativePath) || strings.ContainsRune(relativePath, 0) {
		return &Error{Code: InvalidPath}
	}
	clean := filepath.Clean(relativePath)
	environmentID, filename := filepath.Dir(clean), filepath.Base(clean)
	if !safeSegment(environmentID) || filename == "." || filename == ".." || clean != filepath.Join(environmentID, filename) {
		return &Error{Code: InvalidPath}
	}
	directory := filepath.Join(s.config.Root, environmentID)
	resolved, err := filepath.EvalSymlinks(directory)
	resolvedRoot, rootErr := filepath.EvalSymlinks(s.config.Root)
	if err != nil || rootErr != nil || resolved != filepath.Join(resolvedRoot, environmentID) {
		return &Error{Code: InvalidPath, Cause: err}
	}
	path := filepath.Join(directory, filename)
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return &Error{Code: StorageUnavailable, Cause: err}
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || stat.Nlink != 1 {
		return &Error{Code: InvalidPath}
	}
	if err := os.Remove(path); err != nil {
		return &Error{Code: StorageUnavailable, Cause: err}
	}
	if err := os.Remove(path + ".meta"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &Error{Code: StorageUnavailable, Cause: err}
	}
	if err := syncDirectory(directory); err != nil {
		return &Error{Code: StorageUnavailable, Cause: err}
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func (s *Stager) Cleanup(ctx context.Context, max int) (removed int, resultErr error) {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	defer func() {
		if s.config.Metrics == nil {
			return
		}
		result, value := "preserved", float64(1)
		if resultErr != nil {
			result = "failed"
		} else if removed > 0 {
			result, value = "removed", float64(removed)
		}
		_ = s.config.Metrics.Record("paperboat_helper_cleanup_total", value, map[string]string{"kind": "upload", "result": result})
	}()
	if max < 1 {
		return 0, &Error{Code: ResourceLimit}
	}
	entries, err := os.ReadDir(s.config.Root)
	if err != nil {
		return 0, err
	}
	for _, environment := range entries {
		if removed >= max {
			break
		}
		if !environment.IsDir() || !safeSegment(environment.Name()) {
			continue
		}
		directory := filepath.Join(s.config.Root, environment.Name())
		files, readErr := os.ReadDir(directory)
		if readErr != nil {
			return removed, readErr
		}
		removedFromDirectory := false
		for _, entry := range files {
			if removed >= max {
				break
			}
			select {
			case <-ctx.Done():
				return removed, ctx.Err()
			default:
			}
			isMetadata := strings.HasSuffix(entry.Name(), ".meta")
			isCleanup := strings.HasSuffix(entry.Name(), ".cleanup")
			if !isMetadata && !isCleanup {
				continue
			}
			metaPath := filepath.Join(directory, entry.Name())
			if err := safeRegularFile(metaPath); err != nil {
				continue
			}
			data, readErr := os.ReadFile(metaPath)
			if readErr != nil {
				return removed, readErr
			}
			var meta metadata
			if decodeMetadata(data, &meta) != nil {
				continue
			}
			expectedName := filepath.Base(meta.Path) + ".meta"
			if isCleanup {
				expectedName = filepath.Base(meta.Path) + ".cleanup"
			}
			if filepath.Dir(meta.Path) != environment.Name() || expectedName != entry.Name() {
				continue
			}
			if s.config.Clock.Now().Before(meta.ExpiresAt) {
				continue
			}
			imagePath := filepath.Join(s.config.Root, meta.Path)
			if isCleanup {
				if _, statErr := os.Lstat(imagePath); errors.Is(statErr, os.ErrNotExist) {
					if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
						return removed, err
					}
					removed++
					removedFromDirectory = true
					continue
				}
			}
			if err := safeRegularFile(imagePath); err != nil || !s.validMetadataFile(meta) {
				continue
			}
			cleanupPath := metaPath
			if isMetadata {
				cleanupPath = imagePath + ".cleanup"
				if err := os.Rename(metaPath, cleanupPath); err != nil {
					return removed, err
				}
				if err := syncDirectory(directory); err != nil {
					return removed, err
				}
			}
			if err := os.Remove(imagePath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return removed, err
			}
			if err := os.Remove(cleanupPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return removed, err
			}
			removed++
			removedFromDirectory = true
		}
		if removedFromDirectory {
			if err := syncDirectory(directory); err != nil {
				return removed, err
			}
		}
	}
	return removed, nil
}

func decodeMetadata(data []byte, target *metadata) error {
	if err := rejectDuplicateJSON(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing metadata")
	}
	return nil
}

func rejectDuplicateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func() error
	walk = func() error {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
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
					return errors.New("duplicate metadata key")
				}
				seen[key] = true
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("invalid metadata delimiter")
		}
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing metadata")
	}
	return nil
}

func (s *Stager) validMetadataFile(meta metadata) bool {
	extension, supported := mimeExtensions[meta.MIME]
	if !supported || filepath.Ext(meta.Path) != extension || meta.Bytes < 1 || meta.Bytes > s.config.MaxBytes || !validSHA256(meta.SHA256) || meta.ExpiresAt.IsZero() {
		return false
	}
	path := filepath.Join(s.config.Root, meta.Path)
	pathInfo, err := os.Lstat(path)
	if err != nil || !pathInfo.Mode().IsRegular() || pathInfo.Mode()&os.ModeSymlink != 0 {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil || !fileInfo.Mode().IsRegular() || !os.SameFile(pathInfo, fileInfo) {
		return false
	}
	if stat, ok := fileInfo.Sys().(*syscall.Stat_t); !ok || stat.Nlink != 1 {
		return false
	}
	hash := sha256.New()
	header := &prefixWriter{max: 512}
	read, err := io.Copy(io.MultiWriter(hash, header), io.LimitReader(file, s.config.MaxBytes+1))
	return err == nil && read == meta.Bytes && read <= s.config.MaxBytes && hex.EncodeToString(hash.Sum(nil)) == meta.SHA256 && http.DetectContentType(header.bytes) == meta.MIME
}

var mimeExtensions = map[string]string{"image/png": ".png", "image/jpeg": ".jpg", "image/gif": ".gif", "image/webp": ".webp"}

func safeName(name string) bool {
	return name != "" && !strings.ContainsRune(name, '\x00') && !filepath.IsAbs(name) && filepath.Base(name) == name && !strings.ContainsAny(name, "/\\") && name != "." && name != ".."
}
func safeSegment(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}
func ensurePrivateDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe directory")
	}
	return os.Chmod(path, 0o700)
}
func safeRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("unsafe file")
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok && stat.Nlink != 1 {
		return errors.New("hard-linked file")
	}
	return nil
}
func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

type prefixWriter struct {
	max   int
	bytes []byte
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	remaining := w.max - len(w.bytes)
	if remaining > 0 {
		if remaining > len(p) {
			remaining = len(p)
		}
		w.bytes = append(w.bytes, p[:remaining]...)
	}
	return len(p), nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(p)
	}
}
func classifyIO(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return &Error{Code: StorageUnavailable, Cause: err}
}
