package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// UpsertOpts describes options for `crud.upsert` method.
type UpsertOpts = SimpleOperationOpts

// UpsertRequest helps you to create request object to call `crud.upsert`
// for execution by a Connection.
type UpsertRequest struct {
	spaceRequest
	tuple      Tuple
	operations []Operation
	opts       UpsertOpts
}

type upsertArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space      string
	Tuple      Tuple
	Operations []Operation
	Opts       UpsertOpts
}

// MakeUpsertRequest returns a new empty UpsertRequest.
func MakeUpsertRequest(space string) UpsertRequest {
	req := UpsertRequest{}
	req.impl = newCall("crud.upsert")
	req.space = space
	req.operations = []Operation{}
	req.opts = UpsertOpts{}
	return req
}

// Tuple sets the tuple for the UpsertRequest request.
// Note: default value is nil.
func (req UpsertRequest) Tuple(tuple Tuple) UpsertRequest {
	req.tuple = tuple
	return req
}

// Operations sets the operations for the UpsertRequest request.
// Note: default value is nil.
func (req UpsertRequest) Operations(operations []Operation) UpsertRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the UpsertRequest request.
// Note: default value is nil.
func (req UpsertRequest) Opts(opts UpsertOpts) UpsertRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req UpsertRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.tuple == nil {
		req.tuple = []interface{}{}
	}
	args := upsertArgs{Space: req.space, Tuple: req.tuple,
		Operations: req.operations, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req UpsertRequest) Context(ctx context.Context) UpsertRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// UpsertObjectOpts describes options for `crud.upsert_object` method.
type UpsertObjectOpts = SimpleOperationOpts

// UpsertObjectRequest helps you to create request object to call
// `crud.upsert_object` for execution by a Connection.
type UpsertObjectRequest struct {
	spaceRequest
	object     Object
	operations []Operation
	opts       UpsertObjectOpts
}

type upsertObjectArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space      string
	Object     Object
	Operations []Operation
	Opts       UpsertObjectOpts
}

// MakeUpsertObjectRequest returns a new empty UpsertObjectRequest.
func MakeUpsertObjectRequest(space string) UpsertObjectRequest {
	req := UpsertObjectRequest{}
	req.impl = newCall("crud.upsert_object")
	req.space = space
	req.operations = []Operation{}
	req.opts = UpsertObjectOpts{}
	return req
}

// Object sets the tuple for the UpsertObjectRequest request.
// Note: default value is nil.
func (req UpsertObjectRequest) Object(object Object) UpsertObjectRequest {
	req.object = object
	return req
}

// Operations sets the operations for the UpsertObjectRequest request.
// Note: default value is nil.
func (req UpsertObjectRequest) Operations(operations []Operation) UpsertObjectRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the UpsertObjectRequest request.
// Note: default value is nil.
func (req UpsertObjectRequest) Opts(opts UpsertObjectOpts) UpsertObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req UpsertObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.object == nil {
		req.object = MapObject{}
	}
	args := upsertObjectArgs{Space: req.space, Object: req.object,
		Operations: req.operations, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req UpsertObjectRequest) Context(ctx context.Context) UpsertObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
