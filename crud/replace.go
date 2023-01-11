package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// ReplaceResult describes result for `crud.replace` method.
type ReplaceResult = Result

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

// NewReplaceRequest returns a new empty ReplaceRequest.
func NewReplaceRequest(space string) *ReplaceRequest {
	req := new(ReplaceRequest)
	req.initImpl("crud.replace")
	req.setSpace(space)
	req.tuple = Tuple{}
	req.opts = ReplaceOpts{}
	return req
}

// Tuple sets the tuple for the ReplaceRequest request.
// Note: default value is nil.
func (req *ReplaceRequest) Tuple(tuple Tuple) *ReplaceRequest {
	req.tuple = tuple
	return req
}

// Opts sets the options for the ReplaceRequest request.
// Note: default value is nil.
func (req *ReplaceRequest) Opts(opts ReplaceOpts) *ReplaceRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *ReplaceRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := replaceArgs{Space: req.space, Tuple: req.tuple, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *ReplaceRequest) Context(ctx context.Context) *ReplaceRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// ReplaceObjectResult describes result for `crud.replace_object` method.
type ReplaceObjectResult = Result

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

// NewReplaceObjectRequest returns a new empty ReplaceObjectRequest.
func NewReplaceObjectRequest(space string) *ReplaceObjectRequest {
	req := new(ReplaceObjectRequest)
	req.initImpl("crud.replace_object")
	req.setSpace(space)
	req.object = MapObject{}
	req.opts = ReplaceObjectOpts{}
	return req
}

// Object sets the tuple for the ReplaceObjectRequest request.
// Note: default value is nil.
func (req *ReplaceObjectRequest) Object(object Object) *ReplaceObjectRequest {
	req.object = object
	return req
}

// Opts sets the options for the ReplaceObjectRequest request.
// Note: default value is nil.
func (req *ReplaceObjectRequest) Opts(opts ReplaceObjectOpts) *ReplaceObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *ReplaceObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := replaceObjectArgs{Space: req.space, Object: req.object, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *ReplaceObjectRequest) Context(ctx context.Context) *ReplaceObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
