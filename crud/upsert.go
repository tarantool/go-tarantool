package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// UpsertResult describes result for `crud.upsert` method.
type UpsertResult = Result

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

// NewUpsertRequest returns a new empty UpsertRequest.
func NewUpsertRequest(space string) *UpsertRequest {
	req := new(UpsertRequest)
	req.initImpl("crud.upsert")
	req.setSpace(space)
	req.tuple = Tuple{}
	req.operations = []Operation{}
	req.opts = UpsertOpts{}
	return req
}

// Tuple sets the tuple for the UpsertRequest request.
// Note: default value is nil.
func (req *UpsertRequest) Tuple(tuple Tuple) *UpsertRequest {
	req.tuple = tuple
	return req
}

// Operations sets the operations for the UpsertRequest request.
// Note: default value is nil.
func (req *UpsertRequest) Operations(operations []Operation) *UpsertRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the UpsertRequest request.
// Note: default value is nil.
func (req *UpsertRequest) Opts(opts UpsertOpts) *UpsertRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *UpsertRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := upsertArgs{Space: req.space, Tuple: req.tuple,
		Operations: req.operations, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *UpsertRequest) Context(ctx context.Context) *UpsertRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// UpsertObjectResult describes result for `crud.upsert_object` method.
type UpsertObjectResult = Result

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

// NewUpsertObjectRequest returns a new empty UpsertObjectRequest.
func NewUpsertObjectRequest(space string) *UpsertObjectRequest {
	req := new(UpsertObjectRequest)
	req.initImpl("crud.upsert_object")
	req.setSpace(space)
	req.object = MapObject{}
	req.operations = []Operation{}
	req.opts = UpsertObjectOpts{}
	return req
}

// Object sets the tuple for the UpsertObjectRequest request.
// Note: default value is nil.
func (req *UpsertObjectRequest) Object(object Object) *UpsertObjectRequest {
	req.object = object
	return req
}

// Operations sets the operations for the UpsertObjectRequest request.
// Note: default value is nil.
func (req *UpsertObjectRequest) Operations(operations []Operation) *UpsertObjectRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the UpsertObjectRequest request.
// Note: default value is nil.
func (req *UpsertObjectRequest) Opts(opts UpsertObjectOpts) *UpsertObjectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *UpsertObjectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := upsertObjectArgs{Space: req.space, Object: req.object,
		Operations: req.operations, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *UpsertObjectRequest) Context(ctx context.Context) *UpsertObjectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
