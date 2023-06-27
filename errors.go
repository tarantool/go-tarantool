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
	Code uint32
	Msg  string
}

// Error converts a ClientError to a string.
func (clierr ClientError) Error() string {
	return fmt.Sprintf("%s (0x%x)", clierr.Msg, clierr.Code)
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

// Tarantool client error codes.
const (
	ErrConnectionNotReady = 0x4000 + iota
	ErrConnectionClosed   = 0x4000 + iota
	ErrProtocolError      = 0x4000 + iota
	ErrTimeouted          = 0x4000 + iota
	ErrRateLimited        = 0x4000 + iota
	ErrConnectionShutdown = 0x4000 + iota
	ErrIoError            = 0x4000 + iota
)
