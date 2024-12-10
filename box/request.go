package box

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
)

type baseRequest struct {
	impl *tarantool.CallRequest
}

func newCall(method string) *tarantool.CallRequest {
	return tarantool.NewCallRequest(method)
}

// Type returns IPROTO type for request.
func (req baseRequest) Type() iproto.Type {
	return req.impl.Type()
}

// Ctx returns a context of request.
func (req baseRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns request expects a response.
func (req baseRequest) Async() bool {
	return req.impl.Async()
}

// Response creates a response for the baseRequest.
func (req baseRequest) Response(header tarantool.Header,
	body io.Reader) (tarantool.Response, error) {
	return req.impl.Response(header, body)
}
