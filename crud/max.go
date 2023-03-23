package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// MaxOpts describes options for `crud.max` method.
type MaxOpts = BorderOpts

// MaxRequest helps you to create request object to call `crud.max`
// for execution by a Connection.
type MaxRequest struct {
	spaceRequest
	index interface{}
	opts  MaxOpts
}

type maxArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Index    interface{}
	Opts     MaxOpts
}

// MakeMaxRequest returns a new empty MaxRequest.
func MakeMaxRequest(space string) MaxRequest {
	req := MaxRequest{}
	req.impl = newCall("crud.max")
	req.space = space
	req.opts = MaxOpts{}
	return req
}

// Index sets the index name/id for the MaxRequest request.
// Note: default value is nil.
func (req MaxRequest) Index(index interface{}) MaxRequest {
	req.index = index
	return req
}

// Opts sets the options for the MaxRequest request.
// Note: default value is nil.
func (req MaxRequest) Opts(opts MaxOpts) MaxRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req MaxRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := maxArgs{Space: req.space, Index: req.index, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req MaxRequest) Context(ctx context.Context) MaxRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
