package arrow

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// INSERT Arrow request.
//
// FIXME: replace with iproto.IPROTO_INSERT_ARROW when iproto will released.
// https://github.com/tarantool/go-tarantool/issues/412
const iprotoInsertArrowType = iproto.Type(17)

// The data in Arrow format.
//
// FIXME: replace with iproto.IPROTO_ARROW when iproto will released.
// https://github.com/tarantool/go-tarantool/issues/412
const iprotoArrowKey = iproto.Key(0x36)

// InsertRequest helps you to create an insert request object for execution
// by a Connection.
type InsertRequest struct {
	arrow Arrow
	space interface{}
	ctx   context.Context
}

// NewInsertRequest returns a new InsertRequest.
func NewInsertRequest(space interface{}, arrow Arrow) *InsertRequest {
	return &InsertRequest{
		space: space,
		arrow: arrow,
	}
}

// Type returns a IPROTO_INSERT_ARROW type for the request.
func (r *InsertRequest) Type() iproto.Type {
	return iprotoInsertArrowType
}

// Async returns false to the request return a response.
func (r *InsertRequest) Async() bool {
	return false
}

// Ctx returns a context of the request.
func (r *InsertRequest) Ctx() context.Context {
	return r.ctx
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (r *InsertRequest) Context(ctx context.Context) *InsertRequest {
	r.ctx = ctx
	return r
}

// Arrow sets the arrow for insertion the insert arrow request.
// Note: default value is nil.
func (r *InsertRequest) Arrow(arrow Arrow) *InsertRequest {
	r.arrow = arrow
	return r
}

// Body fills an msgpack.Encoder with the insert arrow request body.
func (r *InsertRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := tarantool.EncodeSpace(res, enc, r.space); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iprotoArrowKey)); err != nil {
		return err
	}
	return enc.Encode(r.arrow)
}

// Response creates a response for the InsertRequest.
func (r *InsertRequest) Response(
	header tarantool.Header,
	body io.Reader,
) (tarantool.Response, error) {
	return tarantool.DecodeBaseResponse(header, body)
}
