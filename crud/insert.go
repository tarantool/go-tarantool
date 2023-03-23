package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// InsertOpts describes options for `crud.insert` method.
type InsertOpts = SimpleOperationOpts

// InsertRequest helps you to create request object to call `crud.insert`
// for execution by a Connection.
type InsertRequest struct {
	spaceRequest
	tuple Tuple
	opts  InsertOpts
}

type insertArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Tuple    Tuple
	Opts     InsertOpts
}

// MakeInsertRequest returns a new empty InsertRequest.
func MakeInsertRequest(space string) InsertRequest {
	req := InsertRequest{}
	req.impl = newCall("crud.insert")
	req.space = space
	req.opts = InsertOpts{}
	return req
}

// Tuple sets the tuple for the InsertRequest request.
// Note: default value is nil.
func (req InsertRequest) Tuple(tuple Tuple) InsertRequest {
	req.tuple = tuple
	return req
}

// Opts sets the options for the insert request.
// Note: default value is nil.
func (req InsertRequest) Opts(opts InsertOpts) InsertRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req InsertRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.tuple == nil {
		req.tuple = []interface{}{}
	}
	args := insertArgs{Space: req.space, Tuple: req.tuple, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req InsertRequest) Context(ctx context.Context) InsertRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// InsertObjectOpts describes options for `crud.insert_object` method.
type InsertObjectOpts = SimpleOperationObjectOpts

// InsertObjectRequest helps you to create request object to call
// `crud.insert_object` for execution by a Connection.
type InsertObjectRequest struct {
	spaceRequest
	object Object
	opts   InsertObjectOpts
}

type insertObjectArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Object   Object
	Opts     InsertObjectOpts
}

// MakeInsertObjectRequest returns a new empty InsertObjectRequest.
func MakeInsertObjectRequest(space string) InsertObjectRequest {
	req := InsertObjectRequest{}
	req.impl = newCall("crud.insert_object")
	req.space = space
	req.opts = InsertObjectOpts{}
	return req
}

// Object sets the tuple for the InsertObjectRequest request.
// Note: default value is nil.
func (req InsertObjectRequest) Object(object Object) InsertObjectRequest {
	req.object = object
	return req
}

// Opts sets the options for the InsertObjectRequest request.
// Note: default value is nil.
func (req InsertObjectRequest) Opts(opts InsertObjectOpts) InsertObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req InsertObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.object == nil {
		req.object = MapObject{}
	}
	args := insertObjectArgs{Space: req.space, Object: req.object, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req InsertObjectRequest) Context(ctx context.Context) InsertObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
