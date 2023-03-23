package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// DeleteOpts describes options for `crud.delete` method.
type DeleteOpts = SimpleOperationOpts

// DeleteRequest helps you to create request object to call `crud.delete`
// for execution by a Connection.
type DeleteRequest struct {
	spaceRequest
	key  Tuple
	opts DeleteOpts
}

type deleteArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Key      Tuple
	Opts     DeleteOpts
}

// MakeDeleteRequest returns a new empty DeleteRequest.
func MakeDeleteRequest(space string) DeleteRequest {
	req := DeleteRequest{}
	req.impl = newCall("crud.delete")
	req.space = space
	req.opts = DeleteOpts{}
	return req
}

// Key sets the key for the DeleteRequest request.
// Note: default value is nil.
func (req DeleteRequest) Key(key Tuple) DeleteRequest {
	req.key = key
	return req
}

// Opts sets the options for the DeleteRequest request.
// Note: default value is nil.
func (req DeleteRequest) Opts(opts DeleteOpts) DeleteRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req DeleteRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.key == nil {
		req.key = []interface{}{}
	}
	args := deleteArgs{Space: req.space, Key: req.key, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req DeleteRequest) Context(ctx context.Context) DeleteRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
