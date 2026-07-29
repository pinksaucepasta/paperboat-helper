package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/filetransfer"
	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

const sendDeliveryTimeout = 10 * time.Minute

type sendInvocationError struct{ message string }

func (e *sendInvocationError) Error() string { return e.message }

type sendResult struct {
	Basename string `json:"basename"`
	State    string `json:"state"`
	Code     string `json:"result_code"`
	Path     string `json:"path,omitempty"`
}

func runSend(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("send", flag.ContinueOnError)
	flags.SetOutput(stderr)
	jsonOutput := flags.Bool("json", false, "Write machine-readable results")
	if err := flags.Parse(args); err != nil {
		return &sendInvocationError{message: "send accepts [--json] <path>..."}
	}
	paths := flags.Args()
	if len(paths) < 1 || len(paths) > filetransfer.MaxBatchFiles {
		return &sendInvocationError{message: "send requires one through ten paths"}
	}

	endpoint, token, workspace, sessionID, err := sendConfiguration()
	if err != nil {
		return err
	}
	sources, files, err := prepareSendSources(workspace, paths)
	if err != nil {
		return err
	}
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()

	client := &filetransfer.LoopbackClient{
		Endpoint: endpoint,
		Token:    token,
		HTTPClient: &http.Client{
			Timeout: sendDeliveryTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return errors.New("file transfer endpoint redirected")
			},
		},
	}
	operationID, err := newSendOperationID()
	if err != nil {
		return err
	}
	transfers, err := client.SendBatch(ctx, operationID, sessionID, sources)
	if err == nil {
		waitCtx, cancel := context.WithTimeout(ctx, sendDeliveryTimeout)
		defer cancel()
		transfers, err = client.WaitReceipt(waitCtx, operationID, transfers)
	}
	results := make([]sendResult, len(sources))
	failed := err != nil
	failureCode := ""
	if err != nil {
		failureCode = filetransfer.ResultCode(err)
	}
	for i, source := range sources {
		result := sendResult{Basename: source.Basename, State: "failed", Code: failureCode}
		if i >= len(transfers) {
			results[i] = result
			continue
		}
		transfer := transfers[i]
		code := transfer.ResultCode
		if code == "" && transfer.State == "delivered" {
			code = "delivered"
		}
		if code == "" && failed {
			code = failureCode
		}
		state := transfer.State
		if failed && state != "delivered" {
			state = "failed"
		}
		results[i] = sendResult{Basename: transfer.Basename, State: state, Code: code, Path: transfer.ReceiptPath}
		if transfer.State != "delivered" {
			failed = true
		}
	}
	if *jsonOutput {
		if encodeErr := json.NewEncoder(stdout).Encode(struct {
			Files []sendResult `json:"files"`
		}{Files: results}); encodeErr != nil {
			return encodeErr
		}
	} else {
		for _, result := range results {
			if result.State == "delivered" {
				fmt.Fprintf(stdout, "%s: delivered to %s\n", result.Basename, result.Path)
			} else {
				fmt.Fprintf(stdout, "%s: %s\n", result.Basename, result.Code)
			}
		}
	}
	if failed {
		if err != nil {
			return err
		}
		return errors.New("one or more files were not delivered")
	}
	return nil
}

func prepareSendSources(workspace string, paths []string) ([]filetransfer.LoopbackSource, []*os.File, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, nil, err
	}
	workspace, err = filepath.Abs(workspace)
	if err != nil {
		return nil, nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: err}
	}
	if resolved, resolveErr := filepath.EvalSymlinks(workspace); resolveErr == nil {
		workspace = resolved
	}
	if resolved, resolveErr := filepath.EvalSymlinks(cwd); resolveErr == nil {
		cwd = resolved
	}
	if err := requireBeneath(workspace, cwd); err != nil {
		return nil, nil, err
	}

	sources := make([]filetransfer.LoopbackSource, 0, len(paths))
	files := make([]*os.File, 0, len(paths))
	closeFiles := func() {
		for _, file := range files {
			_ = file.Close()
		}
	}
	var total int64
	for _, input := range paths {
		if input == "" || filepath.IsAbs(input) || filepath.Clean(input) == "." {
			closeFiles()
			return nil, nil, &filetransfer.Error{Code: filetransfer.InvalidPath}
		}
		fullPath := filepath.Join(cwd, input)
		if err := requireBeneath(workspace, fullPath); err != nil {
			closeFiles()
			return nil, nil, err
		}
		file, err := secureOpenNoFollow(workspace, fullPath)
		if err != nil {
			closeFiles()
			return nil, nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: err}
		}
		before, err := file.Stat()
		if err != nil || !before.Mode().IsRegular() || before.Size() < 0 || before.Size() > filetransfer.MaxFileBytes {
			_ = file.Close()
			closeFiles()
			return nil, nil, &filetransfer.Error{Code: filetransfer.InvalidSize, Cause: err}
		}
		total += before.Size()
		if total > filetransfer.MaxBatchBytes {
			_ = file.Close()
			closeFiles()
			return nil, nil, &filetransfer.Error{Code: filetransfer.BatchLimit}
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			_ = file.Close()
			closeFiles()
			return nil, nil, err
		}
		after, err := file.Stat()
		if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || before.ModTime() != after.ModTime() {
			_ = file.Close()
			closeFiles()
			return nil, nil, &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: errors.New("file changed while hashing")}
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			_ = file.Close()
			closeFiles()
			return nil, nil, err
		}
		files = append(files, file)
		sources = append(sources, filetransfer.LoopbackSource{Basename: filepath.Base(fullPath), Size: before.Size(), SHA256: hex.EncodeToString(hash.Sum(nil)), Reader: file})
	}
	return sources, files, nil
}

func requireBeneath(root, path string) error {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return &filetransfer.Error{Code: filetransfer.InvalidPath, Cause: err}
	}
	return nil
}

func newSendOperationID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "send_" + hex.EncodeToString(value[:]), nil
}

func sendConfiguration() (string, string, string, string, error) {
	endpoint := os.Getenv("PAPERBOAT_FILE_TRANSFER_ENDPOINT")
	tokenFile := os.Getenv("PAPERBOAT_HELPER_AGENT_TOKEN_FILE")
	workspace := os.Getenv("PAPERBOAT_WORKSPACE_ROOT")
	sessionID := os.Getenv("PAPERBOAT_TERMINAL_SESSION_ID")
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "/v1/file-transfers" || u.RawQuery != "" || u.Fragment != "" {
		return "", "", "", "", errors.New("local helper file transfer endpoint is unavailable")
	}
	host, port, err := net.SplitHostPort(u.Host)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() || port == "" {
		return "", "", "", "", errors.New("local helper file transfer endpoint is unavailable")
	}
	if _, err := strconv.Atoi(port); err != nil || workspace == "" || !filepath.IsAbs(workspace) || sessionID == "" {
		return "", "", "", "", errors.New("local helper file transfer configuration is unavailable")
	}
	if tokenFile == "" || !filepath.IsAbs(tokenFile) || filepath.Clean(tokenFile) != tokenFile {
		return "", "", "", "", errors.New("local helper file transfer authorization is unavailable")
	}
	info, err := os.Lstat(tokenFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 || info.Size() > 4096 {
		return "", "", "", "", errors.New("local helper file transfer authorization is unavailable")
	}
	data, err := os.ReadFile(tokenFile)
	token := strings.TrimSpace(string(data))
	if err != nil || len(token) < 32 || len(token) > 512 {
		return "", "", "", "", errors.New("local helper file transfer authorization is unavailable")
	}
	return u.String(), token, filepath.Clean(workspace), sessionID, nil
}

func transferResults(transfers []store.FileTransfer) []sendResult {
	results := make([]sendResult, len(transfers))
	for i, transfer := range transfers {
		results[i] = sendResult{Basename: transfer.Basename, State: transfer.State, Code: transfer.ResultCode, Path: transfer.ReceiptPath}
	}
	return results
}
