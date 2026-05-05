package tarantool

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-iproto"
)

// ClientErrorCode is the numeric identifier for a client-side error.
// Values are stable across Tarantool client implementations.
type ClientErrorCode uint32

// Client error codes. The numeric values match the legacy uint32
// constants exposed by previous versions of go-tarantool.
const (
	CodeConnectionNotReady ClientErrorCode = 0x4000 + iota
	CodeConnectionClosed
	CodeProtocolError
	CodeTimeouted
	CodeRateLimited
	CodeConnectionShutdown
	CodeIoError
)

// sentinelError is a package-level error value matched via errors.Is.
type sentinelError struct {
	code ClientErrorCode
	msg  string
}

func (s *sentinelError) Error() string         { return s.msg }
func (s *sentinelError) Code() ClientErrorCode { return s.code }

// Sentinel errors for client-side failure modes. Compare with errors.Is.
var (
	ErrConnectionNotReady error = &sentinelError{CodeConnectionNotReady, "connection not ready"}
	ErrConnectionClosed   error = &sentinelError{CodeConnectionClosed, "connection closed"}
	ErrProtocolError      error = &sentinelError{CodeProtocolError, "protocol error"}
	ErrTimeouted          error = &sentinelError{CodeTimeouted, "request timed out"}
	ErrRateLimited        error = &sentinelError{CodeRateLimited, "rate limited"}
	ErrConnectionShutdown error = &sentinelError{CodeConnectionShutdown, "connection shutdown"}
	ErrIoError            error = &sentinelError{CodeIoError, "I/O error"}

	// ErrRetryable marks errors that may succeed on retry. It is
	// joined into the error chain of any retryable ClientError so
	// that errors.Is(err, ErrRetryable) returns true.
	ErrRetryable = errors.New("retryable")
)

// codeToSentinel maps a code to its package-level sentinel.
var codeToSentinel = map[ClientErrorCode]error{
	CodeConnectionNotReady: ErrConnectionNotReady,
	CodeConnectionClosed:   ErrConnectionClosed,
	CodeProtocolError:      ErrProtocolError,
	CodeTimeouted:          ErrTimeouted,
	CodeRateLimited:        ErrRateLimited,
	CodeConnectionShutdown: ErrConnectionShutdown,
	CodeIoError:            ErrIoError,
}

// retryableSentinels lists the codes whose chain implies ErrRetryable.
var retryableSentinels = map[ClientErrorCode]struct{}{
	CodeConnectionNotReady: {},
	CodeTimeouted:          {},
	CodeRateLimited:        {},
	CodeIoError:            {},
}

// ClientError is a failure produced by this client: connection state
// transitions, request timeouts, protocol decoding, or I/O.
//
// Compare with package sentinels via errors.Is. If Cause is set, it
// is reachable via errors.As / errors.Unwrap.
type ClientError struct {
	Code  ClientErrorCode
	Msg   string
	Cause error
}

// Error formats as "<sentinel>: <Msg>: <cause>", omitting any empty
// segment. If the code has no registered sentinel, the prefix falls
// back to "client error 0x<code>".
func (e ClientError) Error() string {
	parts := make([]string, 0, 3)
	if s, ok := codeToSentinel[e.Code]; ok {
		parts = append(parts, s.Error())
	} else {
		parts = append(parts, fmt.Sprintf("client error 0x%x", uint32(e.Code)))
	}
	if e.Msg != "" && parts[0] != e.Msg {
		parts = append(parts, e.Msg)
	}
	if e.Cause != nil {
		parts = append(parts, e.Cause.Error())
	}
	return strings.Join(parts, ": ")
}

// Unwrap exposes the sentinel, ErrRetryable (when applicable), and
// the underlying cause to errors.Is / errors.As.
func (e ClientError) Unwrap() []error {
	out := make([]error, 0, 3)
	if s, ok := codeToSentinel[e.Code]; ok {
		out = append(out, s)
	}
	if _, ok := retryableSentinels[e.Code]; ok {
		out = append(out, ErrRetryable)
	}
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	return out
}

// newClientError is the internal constructor used across the package.
func newClientError(code ClientErrorCode, msg string, cause error) ClientError {
	return ClientError{Code: code, Msg: msg, Cause: cause}
}

// ServerError wraps an error returned by the Tarantool server.
type ServerError struct {
	Code         iproto.Error
	Msg          string
	ExtendedInfo *BoxError
}

// Error converts a ServerError to a string.
func (e ServerError) Error() string {
	if e.ExtendedInfo != nil {
		return e.ExtendedInfo.Error()
	}
	return fmt.Sprintf("%s (0x%x)", e.Msg, int(e.Code))
}

// IsRetryable reports whether err indicates a transient failure that
// may succeed on retry.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRetryable)
}
