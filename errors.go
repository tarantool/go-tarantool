package tarantool

import (
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-iproto"
)

// Client error codes produced by this client itself, as opposed to the
// server-side iproto.Error codes carried by ServerError. The numeric
// values match the constants exposed by previous versions of
// go-tarantool. They share the iproto.Error type so comparisons with
// go-iproto values need no conversion.
//
// Each code is annotated with whether the failed request is safe to
// repeat without side effects. Codes marked retryable are joined with
// ErrRetryable in the error chain, so errors.Is(err, ErrRetryable) and
// IsRetryableError(err) report true for them.
const (
	// CodeConnectionNotReady: the connection was not established, so the
	// request never reached the server. Safe to repeat (retryable).
	CodeConnectionNotReady iproto.Error = 0x4000 + iota
	// CodeConnectionClosed: the connection was closed; the request may or
	// may not have been sent. Not retryable.
	CodeConnectionClosed
	// CodeProtocolError: the server reply could not be decoded. Not
	// retryable.
	CodeProtocolError
	// CodeTimeouted: the request timed out in flight and may already be
	// executing on the server, so repeating it can duplicate side effects.
	// It is nonetheless flagged retryable to preserve the historical
	// transient-failure handling; callers that need exactly-once semantics
	// must guard non-idempotent requests themselves.
	CodeTimeouted
	// CodeRateLimited: the request was rejected locally before being sent
	// because of the rate limit. Safe to repeat (retryable).
	CodeRateLimited
	// CodeConnectionShutdown: the request was rejected because the server
	// signalled graceful shutdown. Not retryable on this connection.
	CodeConnectionShutdown
	// CodeIoError: an I/O error occurred on the socket. Retryable.
	CodeIoError
)

// sentinelError is a package-level error value matched via errors.Is.
type sentinelError struct {
	code iproto.Error
	msg  string
}

func (s *sentinelError) Error() string      { return s.msg }
func (s *sentinelError) Code() iproto.Error { return s.code }

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
var codeToSentinel = map[iproto.Error]error{
	CodeConnectionNotReady: ErrConnectionNotReady,
	CodeConnectionClosed:   ErrConnectionClosed,
	CodeProtocolError:      ErrProtocolError,
	CodeTimeouted:          ErrTimeouted,
	CodeRateLimited:        ErrRateLimited,
	CodeConnectionShutdown: ErrConnectionShutdown,
	CodeIoError:            ErrIoError,
}

// retryableSentinels lists the codes whose chain implies ErrRetryable.
var retryableSentinels = map[iproto.Error]struct{}{
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
	Code  iproto.Error
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
		parts = append(parts, fmt.Sprintf("client error 0x%x", int(e.Code)))
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
func newClientError(code iproto.Error, msg string, cause error) ClientError {
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

// IsRetryableError reports whether err indicates a transient failure
// that may succeed on retry.
func IsRetryableError(err error) bool {
	return errors.Is(err, ErrRetryable)
}
