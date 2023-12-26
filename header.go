package tarantool

import "github.com/tarantool/go-iproto"

// Header is a response header.
type Header struct {
	// RequestId is an id of a corresponding request.
	RequestId uint32
	// Error is a response error. It could be used
	// to check that response has or hasn't an error without decoding.
	// Error == ErrorNo (iproto.ER_UNKNOWN) if there is no error.
	// Otherwise, it contains an error code from iproto.Error enumeration.
	Error iproto.Error
}
