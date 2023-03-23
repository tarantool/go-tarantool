package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// ReplaceOpts describes options for `crud.replace` method.
type ReplaceOpts = SimpleOperationOpts

// ReplaceRequest helps you to create request object to call `crud.replace`
// for execution by a Connection.
type ReplaceRequest struct {
	spaceRequest
	tuple Tuple
	opts  ReplaceOpts
}

type replaceArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Tuple    Tuple
	Opts     ReplaceOpts
}

// MakeReplaceRequest returns a new empty ReplaceRequest.
func MakeReplaceRequest(space string) ReplaceRequest {
	req := ReplaceRequest{}
	req.impl = newCall("crud.replace")
	req.space = space
	req.opts = ReplaceOpts{}
	return req
}

// Tuple sets the tuple for the ReplaceRequest request.
// Note: default value is nil.
func (req ReplaceRequest) Tuple(tuple Tuple) ReplaceRequest {
	req.tuple = tuple
	return req
}

// Opts sets the options for the ReplaceRequest request.
// Note: default value is nil.
func (req ReplaceRequest) Opts(opts ReplaceOpts) ReplaceRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req ReplaceRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.tuple == nil {
		req.tuple = []interface{}{}
	}
	args := replaceArgs{Space: req.space, Tuple: req.tuple, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req ReplaceRequest) Context(ctx context.Context) ReplaceRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// ReplaceObjectOpts describes options for `crud.replace_object` method.
type ReplaceObjectOpts = SimpleOperationObjectOpts

// ReplaceObjectRequest helps you to create request object to call
// `crud.replace_object` for execution by a Connection.
type ReplaceObjectRequest struct {
	spaceRequest
	object Object
	opts   ReplaceObjectOpts
}

type replaceObjectArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Object   Object
	Opts     ReplaceObjectOpts
}

// MakeReplaceObjectRequest returns a new empty ReplaceObjectRequest.
func MakeReplaceObjectRequest(space string) ReplaceObjectRequest {
	req := ReplaceObjectRequest{}
	req.impl = newCall("crud.replace_object")
	req.space = space
	req.opts = ReplaceObjectOpts{}
	return req
}

// Object sets the tuple for the ReplaceObjectRequest request.
// Note: default value is nil.
func (req ReplaceObjectRequest) Object(object Object) ReplaceObjectRequest {
	req.object = object
	return req
}

// Opts sets the options for the ReplaceObjectRequest request.
// Note: default value is nil.
func (req ReplaceObjectRequest) Opts(opts ReplaceObjectOpts) ReplaceObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req ReplaceObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.object == nil {
		req.object = MapObject{}
	}
	args := replaceObjectArgs{Space: req.space, Object: req.object, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req ReplaceObjectRequest) Context(ctx context.Context) ReplaceObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
