package tarantool

import (
	"fmt"

	"github.com/tarantool/go-iproto"
)

// Error is wrapper around error returned by Tarantool.
type Error struct {
	Code         iproto.Error
	Msg          string
	ExtendedInfo *BoxError
}

// Error converts an Error to a string.
func (tnterr Error) Error() string {
	if tnterr.ExtendedInfo != nil {
		return tnterr.ExtendedInfo.Error()
	}

	return fmt.Sprintf("%s (0x%x)", tnterr.Msg, tnterr.Code)
}

// ClientError is connection error produced by this client,
// i.e. connection failures or timeouts.
type ClientError struct {
	Code CodeError
	Msg  string
}

// Error converts a ClientError to a string.
func (clierr ClientError) Error() string {
	return fmt.Sprintf("%s (%#x)", clierr.Msg, uint32(clierr.Code))
}

// Temporary returns true if next attempt to perform request may succeeded.
//
// Currently it returns true when:
//
// - Connection is not connected at the moment
//
// - request is timeouted
//
// - request is aborted due to rate limit
func (clierr ClientError) Temporary() bool {
	switch clierr.Code {
	case ErrConnectionNotReady, ErrTimeouted, ErrRateLimited, ErrIoError:
		return true
	default:
		return false
	}
}

// CodeError is an error providing code of failure.
// Allows to differ them and returning error using errors.Is.
type CodeError uint32

// Error converts CodeError to a string.
func (err CodeError) Error() string {
	return fmt.Sprintf("%#x", uint32(err))
}

// Tarantool client error codes.
const (
	ErrConnectionNotReady CodeError = 0x4000 + iota
	ErrConnectionClosed
	ErrProtocolError
	ErrTimeouted
	ErrRateLimited
	ErrConnectionShutdown
	ErrIoError
	ErrCancelledCtx
)
