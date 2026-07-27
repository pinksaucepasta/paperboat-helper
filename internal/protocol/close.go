package protocol

import "errors"

const (
	CloseNormal       = 1000
	CloseUnauthorized = 4401
	CloseForbidden    = 4403
	CloseIncompatible = 4406
	CloseSlowConsumer = 4408
	CloseMalformed    = 4409
	CloseCanceled     = 4410
	CloseUnavailable  = 4503
)

func CloseCode(err error) int {
	if err == nil {
		return CloseNormal
	}
	var protocolError *Error
	if !errors.As(err, &protocolError) {
		return CloseUnavailable
	}
	switch protocolError.Code {
	case CredentialExpired:
		return CloseUnauthorized
	case ProtocolIncompatible, CapabilityRequired:
		return CloseIncompatible
	case Malformed, Oversized, UnsupportedMessage, InvalidFrame, InvalidDeadline, UnsupportedChannel:
		return CloseMalformed
	default:
		return CloseUnavailable
	}
}
