package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// LenResult describes result for `crud.len` method.
type LenResult = NumberResult

// LenOpts describes options for `crud.len` method.
type LenOpts = BaseOpts

// LenRequest helps you to create request object to call `crud.len`
// for execution by a Connection.
type LenRequest struct {
	spaceRequest
	opts LenOpts
}

type lenArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Opts     LenOpts
}

// MakeLenRequest returns a new empty LenRequest.
func MakeLenRequest(space string) LenRequest {
	req := LenRequest{}
	req.impl = newCall("crud.len")
	req.space = space
	req.opts = LenOpts{}
	return req
}

// Opts sets the options for the LenRequest request.
// Note: default value is nil.
func (req LenRequest) Opts(opts LenOpts) LenRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req LenRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := lenArgs{Space: req.space, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req LenRequest) Context(ctx context.Context) LenRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
