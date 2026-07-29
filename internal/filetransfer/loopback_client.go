package filetransfer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/pinksaucepasta/paperboat-helper/internal/store"
)

const loopbackContentType = "application/offset+octet-stream"

const loopbackRecoveryWindow = 10 * time.Second

type LoopbackSource struct {
	Basename string
	Size     int64
	SHA256   string
	Reader   io.ReadSeeker
}
type LoopbackClient struct {
	Endpoint, Token string
	HTTPClient      *http.Client
}

func (c *LoopbackClient) SendBatch(ctx context.Context, operationID, sessionID string, sources []LoopbackSource) ([]store.FileTransfer, error) {
	if len(sources) < 1 || len(sources) > MaxBatchFiles {
		return nil, &Error{Code: BatchLimit}
	}
	files := make([]File, len(sources))
	for i, source := range sources {
		files[i] = File{Basename: source.Basename, Size: source.Size, SHA256: source.SHA256}
	}
	payload, _ := json.Marshal(map[string]any{"batch_id": operationID, "direction": "pbh_to_pb", "session_id": sessionID, "files": files})
	var created struct {
		Transfers []store.FileTransfer `json:"transfers"`
	}
	if err := c.retryJSON(ctx, http.MethodPost, c.Endpoint, operationID, "application/json", 0, payload, &created); err != nil {
		return nil, err
	}
	if len(created.Transfers) != len(sources) {
		return nil, errors.New("helper returned incomplete transfer batch")
	}
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var group sync.WaitGroup
	slots := make(chan struct{}, 2)
	errorsByIndex := make([]error, len(created.Transfers))
	for i, transfer := range created.Transfers {
		group.Add(1)
		go func(index int, item store.FileTransfer) {
			defer group.Done()
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			case <-uploadCtx.Done():
				errorsByIndex[index] = uploadCtx.Err()
				return
			}
			if err := c.upload(uploadCtx, operationID+"_"+strconv.Itoa(index), item, sources[index]); err != nil {
				errorsByIndex[index] = err
				cancel()
			}
		}(i, transfer)
	}
	group.Wait()
	for _, uploadErr := range errorsByIndex {
		if uploadErr != nil {
			c.cancelBatch(created.Transfers, operationID)
			return created.Transfers, uploadErr
		}
	}
	for i, transfer := range created.Transfers {
		var completed struct {
			Transfer store.FileTransfer `json:"transfer"`
			Result   struct {
				Code string `json:"code"`
			} `json:"result"`
		}
		if err := c.retryJSON(ctx, http.MethodPost, c.Endpoint+"/"+transfer.ID+"/complete", operationID+"_complete_"+strconv.Itoa(i), "", 0, nil, &completed); err != nil {
			c.cancelBatch(created.Transfers, operationID)
			return created.Transfers, err
		}
		if completed.Result.Code != "pending" {
			c.cancelBatch(created.Transfers, operationID)
			return created.Transfers, errors.New("helper did not queue transfer")
		}
		created.Transfers[i] = completed.Transfer
	}
	return created.Transfers, nil
}

func (c *LoopbackClient) WaitReceipt(ctx context.Context, operationID string, transfers []store.FileTransfer) ([]store.FileTransfer, error) {
	results := append([]store.FileTransfer(nil), transfers...)
	pending := make([]bool, len(transfers))
	for i := range pending {
		pending[i] = true
	}
	var terminalErr error
	for {
		remaining := 0
		for i, transfer := range transfers {
			if !pending[i] {
				continue
			}
			remaining++
			if err := c.json(ctx, http.MethodGet, c.Endpoint+"/"+transfer.ID, operationID+"_status_"+strconv.Itoa(i), "", 0, nil, &results[i]); err != nil {
				if ctx.Err() != nil {
					for index := range results {
						if pending[index] {
							results[index].ResultCode = string(DeliveryTimeout)
						}
					}
					return results, &Error{Code: DeliveryTimeout, Cause: ctx.Err()}
				}
				var responseErr *loopbackError
				if errors.As(err, &responseErr) {
					return results, err
				}
				continue
			}
			switch results[i].State {
			case "delivered":
				pending[i] = false
			case "failed":
				pending[i] = false
				terminalErr = errors.Join(terminalErr, fmt.Errorf("%s: %s", results[i].Basename, results[i].ResultCode))
			case "canceled":
				pending[i] = false
				terminalErr = errors.Join(terminalErr, &Error{Code: Canceled})
			}
		}
		if remaining == 0 || !slices.Contains(pending, true) {
			return results, terminalErr
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			for i := range results {
				if pending[i] {
					results[i].ResultCode = string(DeliveryTimeout)
				}
			}
			return results, &Error{Code: DeliveryTimeout, Cause: ctx.Err()}
		case <-timer.C:
		}
	}
}

func (c *LoopbackClient) cancelBatch(transfers []store.FileTransfer, operationID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i, transfer := range transfers {
		_ = c.json(ctx, http.MethodDelete, c.Endpoint+"/"+transfer.ID, operationID+"_cancel_"+strconv.Itoa(i), "", 0, nil, nil)
	}
}

func (c *LoopbackClient) upload(ctx context.Context, operationID string, transfer store.FileTransfer, source LoopbackSource) error {
	for attempts := 0; attempts < 4; attempts++ {
		offset, err := c.offset(ctx, operationID, transfer.ID)
		if err != nil {
			var responseErr *loopbackError
			if errors.As(err, &responseErr) {
				return err
			}
			if waitErr := waitLoopbackRetry(ctx, attempts); waitErr != nil {
				return waitErr
			}
			continue
		}
		if offset == source.Size {
			return nil
		}
		if offset < 0 || offset > source.Size {
			return &Error{Code: OffsetConflict}
		}
		if _, err := source.Reader.Seek(offset, io.SeekStart); err != nil {
			return err
		}
		err = c.json(ctx, http.MethodPatch, c.Endpoint+"/"+transfer.ID+"/content", operationID+"_patch", loopbackContentType, offset, io.LimitReader(source.Reader, source.Size-offset), nil)
		if err == nil {
			continue
		}
		var responseErr *loopbackError
		if errors.As(err, &responseErr) {
			return err
		}
	}
	return &Error{Code: OffsetConflict}
}
func (c *LoopbackClient) offset(ctx context.Context, operationID, id string) (int64, error) {
	response, err := c.request(ctx, http.MethodHead, c.Endpoint+"/"+id+"/content", operationID+"_head", "", 0, nil)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	offset, err := strconv.ParseInt(response.Header.Get("Upload-Offset"), 10, 64)
	return offset, err
}
func (c *LoopbackClient) json(ctx context.Context, method, url, operation, mediaType string, offset int64, body io.Reader, target any) error {
	response, err := c.request(ctx, method, url, operation, mediaType, offset, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if target != nil {
		return json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(target)
	}
	return nil
}

func (c *LoopbackClient) retryJSON(ctx context.Context, method, url, operation, mediaType string, offset int64, body []byte, target any) error {
	retryCtx, cancel := context.WithTimeout(ctx, loopbackRecoveryWindow)
	defer cancel()
	for attempt := 0; ; attempt++ {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		err := c.json(retryCtx, method, url, operation, mediaType, offset, reader, target)
		if err == nil {
			return nil
		}
		var responseErr *loopbackError
		if errors.As(err, &responseErr) {
			return err
		}
		if waitErr := waitLoopbackRetry(retryCtx, attempt); waitErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
}

func waitLoopbackRetry(ctx context.Context, attempt int) error {
	delay := 100 * time.Millisecond
	for index := 0; index < attempt && delay < time.Second; index++ {
		delay *= 2
	}
	if delay > time.Second {
		delay = time.Second
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func (c *LoopbackClient) request(ctx context.Context, method, url, operation, mediaType string, offset int64, body io.Reader) (*http.Response, error) {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 5 * time.Minute}
	}
	request, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+c.Token)
	request.Header.Set("X-Paperboat-Request-ID", operation)
	request.Header.Set("X-Paperboat-Operation-ID", operation)
	if mediaType != "" {
		request.Header.Set("Content-Type", mediaType)
	}
	if method == http.MethodPatch {
		request.Header.Set("Upload-Offset", strconv.FormatInt(offset, 10))
	}
	response, err := c.HTTPClient.Do(request)
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return response, nil
	}
	defer response.Body.Close()
	failure := &loopbackError{Status: response.StatusCode}
	_ = json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(failure)
	if failure.Code == "" {
		failure.Code = "http_error"
	}
	return nil, failure
}

type loopbackError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *loopbackError) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

func ResultCode(err error) string {
	var transferErr *Error
	if errors.As(err, &transferErr) {
		return string(transferErr.Code)
	}
	var responseErr *loopbackError
	if errors.As(err, &responseErr) && responseErr.Code != "" {
		return responseErr.Code
	}
	if errors.Is(err, context.Canceled) {
		return string(Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return string(DeliveryTimeout)
	}
	return string(StorageUnavailable)
}
