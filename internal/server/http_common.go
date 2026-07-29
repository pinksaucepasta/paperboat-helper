package server

import (
	"encoding/json"
	"fmt"
	"net/http"

	pberrors "github.com/pinksaucepasta/paperboat-helper/internal/errors"
)

const (
	HeaderRequestID   = "X-Paperboat-Request-ID"
	HeaderOperationID = "X-Paperboat-Operation-ID"
	HeaderReplayed    = "X-Paperboat-Replayed"
)

func operationHTTPStatus(code string) int {
	switch code {
	case "operation_id_conflict":
		return http.StatusConflict
	case "resource_limit":
		return http.StatusTooManyRequests
	case "deadline_exceeded", "operation_canceled":
		return http.StatusRequestTimeout
	default:
		return http.StatusServiceUnavailable
	}
}

func writeHTTPError(writer http.ResponseWriter, requestID, code string, status int, retryable bool) {
	writeHTTPErrorDetails(writer, requestID, code, status, retryable, nil)
}

func writeHTTPErrorDetails(writer http.ResponseWriter, requestID, code string, status int, retryable bool, details map[string]any) {
	if requestID == "" {
		requestID = "unknown"
	}
	payload, _ := json.Marshal(pberrors.Error{Code: pberrors.Code(code), Message: http.StatusText(status), RequestID: requestID, Retryable: retryable, Details: details})
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_, _ = fmt.Fprintln(writer, string(payload))
}
