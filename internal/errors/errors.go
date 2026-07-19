package errors

type Code string

const (
	Invalid      Code = "invalid_request"
	Unauthorized Code = "unauthorized"
	Forbidden    Code = "forbidden"
	Unavailable  Code = "unavailable"
	Canceled     Code = "canceled"
	Deadline     Code = "deadline_exceeded"
	Conflict     Code = "operation_id_conflict"
)

type Error struct {
	Code       Code           `json:"code"`
	Message    string         `json:"message"`
	RequestID  string         `json:"requestId"`
	Retryable  bool           `json:"retryable"`
	RetryAfter uint64         `json:"retryAfterMs,omitempty"`
	Recovery   string         `json:"recovery,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

func (e *Error) Error() string { return string(e.Code) + ": " + e.Message }
