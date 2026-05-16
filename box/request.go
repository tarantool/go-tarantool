package box

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// baseCallRequest wraps a tarantool.CallRequest and implements
// the tarantool.Request interface by delegation.
type baseCallRequest struct {
	call tarantool.CallRequest
}

// Type returns IPROTO type for the request.
func (req baseCallRequest) Type() iproto.Type {
	return req.call.Type()
}

// Body fills an encoder with the request body.
func (req baseCallRequest) Body(res tarantool.SchemaResolver,
	enc *msgpack.Encoder) error {
	return req.call.Body(res, enc)
}

// Ctx returns a context of the request.
func (req baseCallRequest) Ctx() context.Context {
	return req.call.Ctx()
}

// Async returns whether the request expects a response.
func (req baseCallRequest) Async() bool {
	return req.call.Async()
}

// Response creates a response for the request.
func (req baseCallRequest) Response(header tarantool.Header,
	body io.Reader) (tarantool.Response, error) {
	return req.call.Response(header, body)
}
