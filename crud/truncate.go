package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// TruncateResult describes result for `crud.truncate` method.
type TruncateResult = BoolResult

// TruncateOpts describes options for `crud.truncate` method.
type TruncateOpts = BaseOpts

// TruncateRequest helps you to create request object to call `crud.truncate`
// for execution by a Connection.
type TruncateRequest struct {
	spaceRequest
	opts TruncateOpts
}

type truncateArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Opts     TruncateOpts
}

// MakeTruncateRequest returns a new empty TruncateRequest.
func MakeTruncateRequest(space string) TruncateRequest {
	req := TruncateRequest{}
	req.impl = newCall("crud.truncate")
	req.space = space
	req.opts = TruncateOpts{}
	return req
}

// Opts sets the options for the TruncateRequest request.
// Note: default value is nil.
func (req TruncateRequest) Opts(opts TruncateOpts) TruncateRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req TruncateRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := truncateArgs{Space: req.space, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req TruncateRequest) Context(ctx context.Context) TruncateRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
