package filetransfer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

const (
	MaxFileBytes  = int64(50 << 20)
	MaxBatchBytes = int64(500 << 20)
	MaxBatchFiles = 10
	Retention     = 7 * 24 * time.Hour
)

type Policy struct {
	Revision               string `json:"revision"`
	MaxFileBytes           int64  `json:"max_file_bytes"`
	MaxBatchFiles          int    `json:"max_batch_files"`
	MaxBatchBytes          int64  `json:"max_batch_bytes"`
	MaxConcurrentTransfers int    `json:"max_concurrent_transfers"`
	RetentionSeconds       int64  `json:"retention_seconds"`
	DeliveryTimeoutSeconds int64  `json:"delivery_timeout_seconds"`
	MaxPendingSpoolBytes   int64  `json:"max_pending_spool_bytes"`
}

var DefaultPolicy = Policy{Revision: "file-transfer-v1", MaxFileBytes: MaxFileBytes, MaxBatchFiles: MaxBatchFiles, MaxBatchBytes: MaxBatchBytes, MaxConcurrentTransfers: 2, RetentionSeconds: int64(Retention / time.Second), DeliveryTimeoutSeconds: 600, MaxPendingSpoolBytes: 1 << 30}

type PolicyStore struct {
	mu     sync.RWMutex
	policy Policy
}

func NewPolicyStore(policy Policy) *PolicyStore {
	if !policy.Valid() {
		policy = DefaultPolicy
	}
	return &PolicyStore{policy: policy}
}
func (s *PolicyStore) Current() Policy { s.mu.RLock(); defer s.mu.RUnlock(); return s.policy }
func (s *PolicyStore) Update(policy Policy) error {
	if !policy.Valid() {
		return &Error{Code: InvalidSize}
	}
	s.mu.Lock()
	s.policy = policy
	s.mu.Unlock()
	return nil
}
func (p Policy) Valid() bool {
	return p.Revision != "" && p.MaxFileBytes > 0 && p.MaxFileBytes <= MaxFileBytes && p.MaxBatchFiles > 0 && p.MaxBatchFiles <= MaxBatchFiles && p.MaxBatchBytes >= p.MaxFileBytes && p.MaxBatchBytes <= MaxBatchBytes && p.MaxConcurrentTransfers > 0 && p.MaxConcurrentTransfers <= 2 && p.RetentionSeconds > 0 && p.DeliveryTimeoutSeconds > 0 && p.MaxPendingSpoolBytes >= p.MaxBatchBytes
}

type Code string

const (
	InvalidPath        Code = "invalid_path"
	InvalidSize        Code = "invalid_size"
	BatchLimit         Code = "batch_limit"
	OffsetConflict     Code = "offset_conflict"
	DigestMismatch     Code = "digest_mismatch"
	StorageUnavailable Code = "storage_unavailable"
	ResourceLimit      Code = "resource_limit"
	Canceled           Code = "canceled"
	DeliveryTimeout    Code = "delivery_timeout"
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

type File struct {
	Basename string `json:"basename"`
	Size     int64  `json:"size"`
	SHA256   string `json:"sha256"`
}
type CreateRequest struct {
	BatchID, Direction, SessionID, ClientID string
	Files                                   []File
}

type Config struct {
	Root   string
	Store  *store.Store
	Now    func() time.Time
	Random io.Reader
	Policy *PolicyStore
}

type Service struct {
	config  Config
	slotMu  sync.Mutex
	active  int
	locks   sync.Map
	cancels sync.Map
}

type transferCancellation struct {
	once sync.Once
	done chan struct{}
}

func New(config Config) (*Service, error) {
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.Policy == nil {
		config.Policy = NewPolicyStore(DefaultPolicy)
	}
	if !filepath.IsAbs(config.Root) || config.Store == nil {
		return nil, &Error{Code: InvalidPath}
	}
	if err := os.MkdirAll(config.Root, 0o700); err != nil {
		return nil, &Error{Code: StorageUnavailable, Cause: err}
	}
	if err := os.Chmod(config.Root, 0o700); err != nil {
		return nil, &Error{Code: StorageUnavailable, Cause: err}
	}
	return &Service{config: config}, nil
}

func (s *Service) Create(ctx context.Context, request CreateRequest) ([]store.FileTransfer, error) {
	policy := s.config.Policy.Current()
	if request.BatchID == "" || request.SessionID == "" || request.ClientID == "" || request.Direction != "pb_to_pbh" && request.Direction != "pbh_to_pb" {
		return nil, &Error{Code: InvalidPath}
	}
	if len(request.Files) < 1 || len(request.Files) > policy.MaxBatchFiles {
		return nil, &Error{Code: BatchLimit}
	}
	now := s.config.Now()
	transfers := make([]store.FileTransfer, len(request.Files))
	var total int64
	for i, file := range request.Files {
		if !validBasename(file.Basename) {
			return nil, &Error{Code: InvalidPath}
		}
		if file.Size < 0 || file.Size > policy.MaxFileBytes {
			return nil, &Error{Code: InvalidSize}
		}
		if !validDigest(file.SHA256) {
			return nil, &Error{Code: DigestMismatch}
		}
		total += file.Size
		if total > policy.MaxBatchBytes {
			return nil, &Error{Code: BatchLimit}
		}
		id, err := s.newID("ft_")
		if err != nil {
			return nil, &Error{Code: StorageUnavailable, Cause: err}
		}
		expiresAt := now.Add(time.Duration(policy.RetentionSeconds) * time.Second)
		if request.Direction == "pbh_to_pb" {
			expiresAt = now.Add(time.Duration(policy.DeliveryTimeoutSeconds) * time.Second)
		}
		transfers[i] = store.FileTransfer{ID: id, BatchID: request.BatchID, Direction: request.Direction, SessionID: request.SessionID, ClientID: request.ClientID, Basename: file.Basename, Size: file.Size, SHA256: file.SHA256, State: "created", CreatedAt: now, ExpiresAt: expiresAt}
	}
	if err := s.config.Store.CreateFileTransfersWithinLimits(ctx, transfers, policy.MaxPendingSpoolBytes, policy.MaxConcurrentTransfers); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil, &Error{Code: ResourceLimit, Cause: err}
		}
		return nil, &Error{Code: StorageUnavailable, Cause: err}
	}
	for _, transfer := range transfers {
		s.cancelSignal(transfer.ID)
		file, err := os.OpenFile(s.partialPath(transfer.ID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = s.cancelBatch(context.Background(), transfers)
			return nil, &Error{Code: StorageUnavailable, Cause: err}
		}
		if err := file.Close(); err != nil {
			_ = s.cancelBatch(context.Background(), transfers)
			return nil, &Error{Code: StorageUnavailable, Cause: err}
		}
	}
	if err := syncDir(s.config.Root); err != nil {
		_ = s.cancelBatch(context.Background(), transfers)
		return nil, &Error{Code: StorageUnavailable, Cause: err}
	}
	return transfers, nil
}

// CleanupExpired removes transfer records and content after their independent retention deadline.
func (s *Service) CleanupExpired(ctx context.Context) error {
	transfers, err := s.config.Store.ExpiredFileTransfers(ctx, s.config.Now())
	if err != nil {
		return err
	}
	for _, transfer := range transfers {
		_ = os.Remove(s.partialPath(transfer.ID))
		_ = os.Remove(s.contentPath(transfer.ID))
		_ = os.Remove(s.publishedPath(transfer))
		s.cancels.Delete(transfer.ID)
	}
	return s.config.Store.ExpireFileTransfers(ctx, transfers)
}

type CleanupWorker struct {
	Service  *Service
	Interval time.Duration
	cancel   context.CancelFunc
	done     chan struct{}
}

func (w *CleanupWorker) Start(context.Context) error {
	interval := w.Interval
	if interval <= 0 {
		interval = time.Minute
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel, w.done = cancel, make(chan struct{})
	go func() {
		defer close(w.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = w.Service.CleanupExpired(context.Background())
			}
		}
	}()
	return nil
}
func (w *CleanupWorker) Shutdown(ctx context.Context) error {
	if w.cancel == nil {
		return nil
	}
	w.cancel()
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Service) Append(ctx context.Context, id string, offset int64, body io.Reader) (store.FileTransfer, error) {
	if err := s.acquire(ctx); err != nil {
		return store.FileTransfer{}, err
	}
	defer s.release()
	lock := s.lock(id)
	lock.Lock()
	defer lock.Unlock()
	transfer, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return store.FileTransfer{}, &Error{Code: InvalidPath, Cause: err}
	}
	if transfer.State == "canceled" {
		return store.FileTransfer{}, &Error{Code: Canceled}
	}
	if offset != transfer.CommittedOffset {
		return transfer, &Error{Code: OffsetConflict}
	}
	file, err := os.OpenFile(s.partialPath(id), os.O_WRONLY, 0)
	if err != nil {
		return transfer, &Error{Code: StorageUnavailable, Cause: err}
	}
	defer file.Close()
	if err := file.Truncate(offset); err != nil {
		return transfer, &Error{Code: StorageUnavailable, Cause: err}
	}
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return transfer, &Error{Code: StorageUnavailable, Cause: err}
	}
	remaining := transfer.Size - offset
	canceled := s.cancelSignal(id)
	written, err := io.Copy(file, io.LimitReader(&contextReader{ctx: ctx, reader: body, canceled: canceled}, remaining+1))
	if err != nil {
		if ctx.Err() != nil {
			return transfer, ctx.Err()
		}
		select {
		case <-canceled:
			return transfer, &Error{Code: Canceled}
		default:
		}
		return transfer, classifyIO(err)
	}
	if written > remaining {
		_ = file.Truncate(offset)
		return transfer, &Error{Code: InvalidSize}
	}
	if err := file.Sync(); err != nil {
		return transfer, &Error{Code: StorageUnavailable, Cause: err}
	}
	if err := s.config.Store.CommitFileTransferOffset(ctx, id, offset, offset+written); err != nil {
		return transfer, &Error{Code: OffsetConflict, Cause: err}
	}
	transfer.CommittedOffset += written
	transfer.State = "uploading"
	return transfer, nil
}

func (s *Service) acquire(ctx context.Context) error {
	for {
		s.slotMu.Lock()
		limit := s.config.Policy.Current().MaxConcurrentTransfers
		if s.active < limit {
			s.active++
			s.slotMu.Unlock()
			return nil
		}
		s.slotMu.Unlock()
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func (s *Service) release() { s.slotMu.Lock(); s.active--; s.slotMu.Unlock() }

func (s *Service) Complete(ctx context.Context, id string) (store.FileTransfer, error) {
	requested, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return store.FileTransfer{}, &Error{Code: InvalidPath, Cause: err}
	}
	lock := s.lock("batch:" + requested.BatchID)
	lock.Lock()
	defer lock.Unlock()
	transfers, err := s.config.Store.FileTransfersByBatch(ctx, requested.BatchID)
	if err != nil {
		return store.FileTransfer{}, &Error{Code: InvalidPath, Cause: err}
	}
	allTerminal := true
	for _, transfer := range transfers {
		if transfer.State != "published" && transfer.State != "pending" && transfer.State != "delivered" {
			allTerminal = false
		}
	}
	if allTerminal {
		return s.config.Store.FileTransfer(ctx, id)
	}
	for _, transfer := range transfers {
		if transfer.CommittedOffset != transfer.Size || transfer.State != "created" && transfer.State != "uploading" {
			return requested, &Error{Code: InvalidSize}
		}
		contentPath := s.partialPath(transfer.ID)
		if _, statErr := os.Stat(contentPath); errors.Is(statErr, os.ErrNotExist) {
			contentPath = s.contentPath(transfer.ID)
		}
		file, openErr := os.Open(contentPath)
		if openErr != nil {
			return requested, &Error{Code: StorageUnavailable, Cause: openErr}
		}
		hash := sha256.New()
		written, copyErr := io.Copy(hash, &contextReader{ctx: ctx, reader: file, canceled: s.cancelSignal(transfer.ID)})
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			return requested, classifyIO(errors.Join(copyErr, closeErr))
		}
		if written != transfer.Size || hex.EncodeToString(hash.Sum(nil)) != transfer.SHA256 {
			_ = s.cancelBatchLocked(context.Background(), transfers)
			return requested, &Error{Code: DigestMismatch}
		}
	}
	var renamed []store.FileTransfer
	for _, transfer := range transfers {
		if _, statErr := os.Stat(s.contentPath(transfer.ID)); statErr == nil {
			continue
		}
		if renameErr := os.Rename(s.partialPath(transfer.ID), s.contentPath(transfer.ID)); renameErr != nil {
			for index := len(renamed) - 1; index >= 0; index-- {
				_ = os.Rename(s.contentPath(renamed[index].ID), s.partialPath(renamed[index].ID))
			}
			return requested, &Error{Code: StorageUnavailable, Cause: renameErr}
		}
		renamed = append(renamed, transfer)
	}
	if err := syncDir(s.config.Root); err != nil {
		return requested, &Error{Code: StorageUnavailable, Cause: err}
	}
	if err := s.config.Store.CompleteFileTransferBatch(ctx, requested.BatchID); err != nil {
		return requested, &Error{Code: StorageUnavailable, Cause: err}
	}
	for _, transfer := range transfers {
		if transfer.Direction == "pb_to_pbh" {
			s.cancels.Delete(transfer.ID)
		}
	}
	return s.config.Store.FileTransfer(ctx, id)
}

func (s *Service) Get(ctx context.Context, id string) (store.FileTransfer, error) {
	return s.config.Store.FileTransfer(ctx, id)
}
func (s *Service) Batch(ctx context.Context, batchID string) ([]store.FileTransfer, error) {
	return s.config.Store.FileTransfersByBatch(ctx, batchID)
}
func (s *Service) Pending(ctx context.Context, clientID, sessionID string, limit int) ([]store.FileTransfer, error) {
	return s.config.Store.PendingFileTransfers(ctx, clientID, sessionID, s.config.Now(), limit)
}
func (s *Service) Receipt(ctx context.Context, id, clientID, resultCode, receiptPath string) error {
	if err := s.config.Store.ReceiptFileTransfer(ctx, id, clientID, resultCode, receiptPath); err != nil {
		return err
	}
	if err := os.Remove(s.contentPath(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return &Error{Code: StorageUnavailable, Cause: err}
	}
	s.cancels.Delete(id)
	return syncDir(s.config.Root)
}
func (s *Service) OpenContent(ctx context.Context, id string) (*os.File, store.FileTransfer, error) {
	transfer, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return nil, transfer, &Error{Code: InvalidPath, Cause: err}
	}
	if transfer.State != "published" && transfer.State != "pending" && transfer.State != "delivered" {
		return nil, transfer, &Error{Code: InvalidPath}
	}
	file, err := os.Open(s.contentPath(id))
	if err != nil {
		return nil, transfer, &Error{Code: StorageUnavailable, Cause: err}
	}
	return file, transfer, nil
}

func (s *Service) PublishedPath(ctx context.Context, id string) (string, error) {
	transfer, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return "", &Error{Code: InvalidPath, Cause: err}
	}
	if transfer.Direction != "pb_to_pbh" || transfer.State != "published" {
		return "", &Error{Code: InvalidPath}
	}
	contentPath := s.contentPath(id)
	path := s.publishedPath(transfer)
	if err := os.Link(contentPath, path); err != nil && !errors.Is(err, os.ErrExist) {
		return "", &Error{Code: StorageUnavailable, Cause: err}
	}
	contentInfo, contentErr := os.Stat(contentPath)
	info, err := os.Lstat(path)
	if contentErr != nil || err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || !os.SameFile(contentInfo, info) {
		return "", &Error{Code: StorageUnavailable, Cause: errors.Join(contentErr, err)}
	}
	if err := syncDir(s.config.Root); err != nil {
		return "", &Error{Code: StorageUnavailable, Cause: err}
	}
	return path, nil
}
func (s *Service) Cancel(ctx context.Context, id string) error {
	transfer, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return err
	}
	lock := s.lock("batch:" + transfer.BatchID)
	lock.Lock()
	defer lock.Unlock()
	transfers, err := s.config.Store.FileTransfersByBatch(ctx, transfer.BatchID)
	if err != nil {
		return err
	}
	return s.cancelBatchLocked(ctx, transfers)
}

func (s *Service) cancelLocked(ctx context.Context, id string) error {
	transfer, err := s.config.Store.FileTransfer(ctx, id)
	if err != nil {
		return err
	}
	if err := s.config.Store.CancelFileTransfer(ctx, id); err != nil {
		return err
	}
	for _, path := range []string{s.partialPath(id), s.contentPath(id), s.publishedPath(transfer)} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return &Error{Code: StorageUnavailable, Cause: err}
		}
	}
	return syncDir(s.config.Root)
}
func (s *Service) cancelBatch(ctx context.Context, transfers []store.FileTransfer) error {
	if len(transfers) == 0 {
		return nil
	}
	lock := s.lock("batch:" + transfers[0].BatchID)
	lock.Lock()
	defer lock.Unlock()
	return s.cancelBatchLocked(ctx, transfers)
}

func (s *Service) cancelBatchLocked(ctx context.Context, transfers []store.FileTransfer) error {
	var result error
	for _, transfer := range transfers {
		s.signalCancel(transfer.ID)
	}
	if len(transfers) > 0 {
		result = s.config.Store.CancelFileTransferBatch(ctx, transfers[0].BatchID)
	}
	for _, transfer := range transfers {
		for _, path := range []string{s.partialPath(transfer.ID), s.contentPath(transfer.ID), s.publishedPath(transfer)} {
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				result = errors.Join(result, err)
			}
		}
		s.cancels.Delete(transfer.ID)
	}
	return errors.Join(result, syncDir(s.config.Root))
}
func (s *Service) partialPath(id string) string { return filepath.Join(s.config.Root, id+".part") }
func (s *Service) contentPath(id string) string { return filepath.Join(s.config.Root, id+".content") }
func (s *Service) publishedPath(transfer store.FileTransfer) string {
	return filepath.Join(s.config.Root, transfer.ID+"-"+transfer.Basename)
}
func (s *Service) lock(id string) *sync.Mutex {
	value, _ := s.locks.LoadOrStore(id, &sync.Mutex{})
	return value.(*sync.Mutex)
}
func (s *Service) cancelSignal(id string) <-chan struct{} {
	value, _ := s.cancels.LoadOrStore(id, &transferCancellation{done: make(chan struct{})})
	return value.(*transferCancellation).done
}
func (s *Service) signalCancel(id string) {
	value, _ := s.cancels.LoadOrStore(id, &transferCancellation{done: make(chan struct{})})
	cancel := value.(*transferCancellation)
	cancel.once.Do(func() { close(cancel.done) })
}
func (s *Service) CancellationSignal(id string) <-chan struct{} { return s.cancelSignal(id) }
func (s *Service) newID(prefix string) (string, error) {
	var value [16]byte
	if _, err := io.ReadFull(s.config.Random, value[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(value[:]), nil
}
func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32
}
func validBasename(value string) bool {
	return value != "" && utf8.ValidString(value) && len(value) <= 255 && value == filepath.Base(value) && value != "." && value != ".." && !strings.ContainsAny(value, "/\\\x00")
}
func syncDir(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

type contextReader struct {
	ctx      context.Context
	reader   io.Reader
	canceled <-chan struct{}
}

func (r *contextReader) Read(p []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	case <-r.canceled:
		return 0, context.Canceled
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
func (e *Error) Format(state fmt.State, verb rune) { _, _ = fmt.Fprint(state, e.Error()) }
