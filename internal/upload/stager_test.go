package upload

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

func newStager(t *testing.T, clock Clock) (*Stager, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "uploads")
	s, err := New(Config{Root: root, Clock: clock, Random: bytes.NewReader(make([]byte, 64)), MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	return s, root
}

func TestStagePublishesPrivateScopedImage(t *testing.T) {
	clock := &fixedClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s, root := newStager(t, clock)
	data := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	result, err := s.Stage(context.Background(), Request{EnvironmentID: "env_test_01", DisplayName: "diagram.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(data)), CredentialExpiry: clock.now.Add(time.Hour), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	if filepath.IsAbs(result.Path) || filepath.Dir(result.Path) != "." || result.MIME != "image/png" || result.Bytes != int64(len(data)) || result.ExpiresAt != clock.now.Add(time.Hour) {
		t.Fatalf("result=%#v", result)
	}
	info, err := os.Stat(filepath.Join(root, result.Path))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(root, result.Path) + ".meta"); err != nil {
		t.Fatal(err)
	}
	absolute, err := s.AbsolutePath(result.Path)
	if err != nil || absolute != filepath.Join(root, result.Path) {
		t.Fatalf("absolute=%q err=%v", absolute, err)
	}
	for _, invalid := range []string{"../image.png", filepath.Join("env_test_01", "missing.png"), absolute} {
		if _, err := s.AbsolutePath(invalid); err == nil {
			t.Fatalf("AbsolutePath accepted %q", invalid)
		}
	}
}

func TestStagePublishesTIFFClipboardImage(t *testing.T) {
	clock := &fixedClock{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	s, root := newStager(t, clock)
	data := append([]byte{'I', 'I', 0x2a, 0x00}, make([]byte, 16)...)
	result, err := s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "clipboard.tiff", DeclaredMIME: "image/tiff", DeclaredSize: int64(len(data)), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	if result.MIME != "image/tiff" || filepath.Ext(result.Path) != ".tiff" {
		t.Fatalf("result=%#v", result)
	}
	if _, err := os.Stat(filepath.Join(root, result.Path)); err != nil {
		t.Fatal(err)
	}
}

func TestDetectImageMIMETIFFByteOrders(t *testing.T) {
	for _, header := range [][]byte{{'I', 'I', 0x2a, 0x00}, {'M', 'M', 0x00, 0x2a}} {
		if got := detectImageMIME(header); got != "image/tiff" {
			t.Fatalf("detectImageMIME(%x) = %q", header, got)
		}
	}
}

func TestStageAcceptsUnknownImageSubtypeWithoutAllowlist(t *testing.T) {
	s, _ := newStager(t, &fixedClock{time.Now()})
	data := []byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p', 'a', 'v', 'i', 'f'}
	result, err := s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "photo.avif", DeclaredMIME: "image/avif", DeclaredSize: int64(len(data)), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	if result.MIME != "image/avif" || filepath.Ext(result.Path) != ".avif" {
		t.Fatalf("result=%#v", result)
	}
}

func TestStageAcceptsNonImageFile(t *testing.T) {
	s, _ := newStager(t, &fixedClock{time.Now()})
	data := []byte("hello from paperboat\n")
	result, err := s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "notes.txt", DeclaredMIME: "text/plain", DeclaredSize: int64(len(data)), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	if result.MIME != "text/plain" || filepath.Ext(result.Path) != ".txt" {
		t.Fatalf("result=%#v", result)
	}
}

func TestStageRejectsPathMimeAndSize(t *testing.T) {
	s, _ := newStager(t, &fixedClock{time.Now()})
	png := []byte("\x89PNG\r\n\x1a\n")
	cases := []struct {
		name    string
		request Request
		code    Code
	}{
		{"traversal", Request{EnvironmentID: "env", DisplayName: "../x.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(png)), Body: bytes.NewReader(png)}, InvalidPath},
		{"mime", Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "not-a-mime", DeclaredSize: int64(len(png)), Body: bytes.NewReader(png)}, MIMEMismatch},
		{"size", Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(png)) + 1, Body: bytes.NewReader(png)}, InvalidSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := s.Stage(context.Background(), tc.request)
			var uploadErr *Error
			if !errors.As(err, &uploadErr) || uploadErr.Code != tc.code {
				t.Fatalf("err=%v want=%s", err, tc.code)
			}
		})
	}
}

func TestStageVerifiesExpectedDigestBeforePublicationAndRemoveIsIdempotent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "uploads")
	stager, err := New(Config{Root: root, Random: bytes.NewReader(make([]byte, 64))})
	if err != nil {
		t.Fatal(err)
	}
	png := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	wrong := strings.Repeat("0", 64)
	if _, err := stager.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "image.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(png)), ExpectedSHA256: wrong, Body: bytes.NewReader(png)}); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	digest := sha256.Sum256(png)
	result, err := stager.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "image.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(png)), ExpectedSHA256: hex.EncodeToString(digest[:]), Body: bytes.NewReader(png)})
	if err != nil {
		t.Fatal(err)
	}
	if err := stager.Remove(result.Path); err != nil {
		t.Fatal(err)
	}
	if err := stager.Remove(result.Path); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledStageRemovesPartialFiles(t *testing.T) {
	s, root := newStager(t, &fixedClock{time.Now()})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Stage(ctx, Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "image/png", DeclaredSize: 8, Body: bytes.NewReader([]byte("\x89PNG\r\n\x1a\n"))})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("partial entries=%v", entries)
	}
}

func TestCleanupDeletesExpiredPairsAndRejectsSymlinkPublication(t *testing.T) {
	clock := &fixedClock{time.Now().UTC()}
	s, root := newStager(t, clock)
	data := []byte("GIF89a")
	result, err := s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "x.gif", DeclaredMIME: "image/gif", DeclaredSize: int64(len(data)), CredentialExpiry: clock.now.Add(time.Minute), Body: bytes.NewReader(data)})
	if err != nil {
		t.Fatal(err)
	}
	clock.now = clock.now.Add(2 * time.Minute)
	removed, err := s.Cleanup(context.Background(), 10)
	if err != nil || removed != 1 {
		t.Fatalf("removed=%d err=%v", removed, err)
	}
	if _, err := os.Stat(filepath.Join(root, result.Path)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat err=%v", err)
	}
	outside := filepath.Join(t.TempDir(), "outside.gif")
	if err := os.WriteFile(outside, data, 0o600); err != nil {
		t.Fatal(err)
	}
	symlinkName := strings.Repeat("a", 32) + ".gif"
	if err := os.Symlink(outside, filepath.Join(root, symlinkName)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AbsolutePath(symlinkName); err == nil {
		t.Fatal("symlink publication accepted")
	}
}

func TestCleanupPreservesAmbiguousHardLinkedDeviceAndDuplicateMetadata(t *testing.T) {
	data := append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 16)...)
	stageExpired := func(t *testing.T) (*Stager, string, Result, *fixedClock) {
		t.Helper()
		clock := &fixedClock{time.Now().UTC()}
		stager, root := newStager(t, clock)
		result, err := stager.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "image/png", DeclaredSize: int64(len(data)), CredentialExpiry: clock.now.Add(time.Minute), Body: bytes.NewReader(data)})
		if err != nil {
			t.Fatal(err)
		}
		clock.now = clock.now.Add(2 * time.Minute)
		return stager, root, result, clock
	}
	t.Run("hard_link", func(t *testing.T) {
		stager, root, result, _ := stageExpired(t)
		path := filepath.Join(root, result.Path)
		link := path + ".external-link"
		if err := os.Link(path, link); err != nil {
			t.Fatal(err)
		}
		removed, err := stager.Cleanup(context.Background(), 10)
		if err != nil || removed != 0 {
			t.Fatalf("removed=%d err=%v", removed, err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("device", func(t *testing.T) {
		stager, root, result, _ := stageExpired(t)
		path := filepath.Join(root, result.Path)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		removed, err := stager.Cleanup(context.Background(), 10)
		if err != nil || removed != 0 {
			t.Fatalf("removed=%d err=%v", removed, err)
		}
	})
	t.Run("duplicate_metadata", func(t *testing.T) {
		stager, root, result, _ := stageExpired(t)
		metaPath := filepath.Join(root, result.Path) + ".meta"
		encoded, err := os.ReadFile(metaPath)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(bytes.TrimSuffix(encoded, []byte("}")), []byte(`,"expires_at":"2000-01-01T00:00:00Z"}`)...)
		if err := os.WriteFile(metaPath, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		removed, err := stager.Cleanup(context.Background(), 10)
		if err != nil || removed != 0 {
			t.Fatalf("removed=%d err=%v", removed, err)
		}
		if _, err := os.Stat(filepath.Join(root, result.Path)); err != nil {
			t.Fatal(err)
		}
	})
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestStageDiskFailureIsTypedAndLeavesNoPartialFile(t *testing.T) {
	stager, root := newStager(t, &fixedClock{time.Now().UTC()})
	_, err := stager.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "image/png", DeclaredSize: 8, Body: failingReader{err: syscall.ENOSPC}})
	var uploadErr *Error
	if !errors.As(err, &uploadErr) || uploadErr.Code != StorageUnavailable || !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("err=%v", err)
	}
	entries, readErr := os.ReadDir(root)
	if readErr != nil || len(entries) != 0 {
		t.Fatalf("entries=%v err=%v", entries, readErr)
	}
}

func TestCleanupResumesInterruptedTombstoneStates(t *testing.T) {
	for _, imageAlreadyRemoved := range []bool{false, true} {
		t.Run(fmt.Sprintf("image_removed_%v", imageAlreadyRemoved), func(t *testing.T) {
			clock := &fixedClock{time.Now().UTC()}
			stager, root := newStager(t, clock)
			data := []byte("GIF89a")
			result, err := stager.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "x.gif", DeclaredMIME: "image/gif", DeclaredSize: int64(len(data)), CredentialExpiry: clock.now.Add(time.Minute), Body: bytes.NewReader(data)})
			if err != nil {
				t.Fatal(err)
			}
			clock.now = clock.now.Add(2 * time.Minute)
			imagePath := filepath.Join(root, result.Path)
			if err := os.Rename(imagePath+".meta", imagePath+".cleanup"); err != nil {
				t.Fatal(err)
			}
			if imageAlreadyRemoved {
				if err := os.Remove(imagePath); err != nil {
					t.Fatal(err)
				}
			}
			removed, err := stager.Cleanup(context.Background(), 1)
			if err != nil || removed != 1 {
				t.Fatalf("removed=%d err=%v", removed, err)
			}
			for _, path := range []string{imagePath, imagePath + ".cleanup", imagePath + ".meta"} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("path=%s err=%v", path, err)
				}
			}
		})
	}
}

type blockingReader struct {
	started chan struct{}
	release chan struct{}
	once    bool
}

func (r *blockingReader) Read([]byte) (int, error) {
	if !r.once {
		r.once = true
		close(r.started)
	}
	<-r.release
	return 0, io.EOF
}

func TestConcurrentAdmissionRejectsBeforeReadingBody(t *testing.T) {
	s, _ := newStager(t, &fixedClock{time.Now()})
	reader := &blockingReader{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "x.png", DeclaredMIME: "image/png", DeclaredSize: 8, Body: reader})
	}()
	<-reader.started
	if got := s.ResourceCounts()["uploads"]; got != 1 {
		t.Fatalf("active uploads = %d", got)
	}
	probe := strings.NewReader("not read")
	_, err := s.Stage(context.Background(), Request{EnvironmentID: "env", DisplayName: "y.png", DeclaredMIME: "image/png", DeclaredSize: 8, Body: probe})
	var uploadErr *Error
	if !errors.As(err, &uploadErr) || uploadErr.Code != ResourceLimit {
		t.Fatalf("err=%v", err)
	}
	if probe.Len() != 8 {
		t.Fatalf("body was read: remaining=%d", probe.Len())
	}
	close(reader.release)
	<-done
	if got := s.ResourceCounts()["uploads"]; got != 0 {
		t.Fatalf("active uploads after completion = %d", got)
	}
}
