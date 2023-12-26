package tarantool

import (
	"github.com/tarantool/go-iproto"
)

const (
	packetLengthBytes = 5
)

const (
	// ErrorNo indicates that no error has occurred. It could be used to
	// check that a response has an error without the response body decoding.
	ErrorNo = iproto.ER_UNKNOWN
)
