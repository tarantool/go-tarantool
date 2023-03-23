package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// UpsertManyOpts describes options for `crud.upsert_many` method.
type UpsertManyOpts = OperationManyOpts

// TupleOperationsData contains tuple with operations to be applied to tuple.
type TupleOperationsData struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Tuple      Tuple
	Operations []Operation
}

// UpsertManyRequest helps you to create request object to call
// `crud.upsert_many` for execution by a Connection.
type UpsertManyRequest struct {
	spaceRequest
	tuplesOperationsData []TupleOperationsData
	opts                 UpsertManyOpts
}

type upsertManyArgs struct {
	_msgpack             struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space                string
	TuplesOperationsData []TupleOperationsData
	Opts                 UpsertManyOpts
}

// MakeUpsertManyRequest returns a new empty UpsertManyRequest.
func MakeUpsertManyRequest(space string) UpsertManyRequest {
	req := UpsertManyRequest{}
	req.impl = newCall("crud.upsert_many")
	req.space = space
	req.tuplesOperationsData = []TupleOperationsData{}
	req.opts = UpsertManyOpts{}
	return req
}

// TuplesOperationsData sets tuples and operations for
// the UpsertManyRequest request.
// Note: default value is nil.
func (req UpsertManyRequest) TuplesOperationsData(tuplesOperationData []TupleOperationsData) UpsertManyRequest {
	req.tuplesOperationsData = tuplesOperationData
	return req
}

// Opts sets the options for the UpsertManyRequest request.
// Note: default value is nil.
func (req UpsertManyRequest) Opts(opts UpsertManyOpts) UpsertManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req UpsertManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := upsertManyArgs{Space: req.space, TuplesOperationsData: req.tuplesOperationsData,
		Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req UpsertManyRequest) Context(ctx context.Context) UpsertManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// UpsertObjectManyOpts describes options for `crud.upsert_object_many` method.
type UpsertObjectManyOpts = OperationManyOpts

// ObjectOperationsData contains object with operations to be applied to object.
type ObjectOperationsData struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Object     Object
	Operations []Operation
}

// UpsertObjectManyRequest helps you to create request object to call
// `crud.upsert_object_many` for execution by a Connection.
type UpsertObjectManyRequest struct {
	spaceRequest
	objectsOperationsData []ObjectOperationsData
	opts                  UpsertObjectManyOpts
}

type upsertObjectManyArgs struct {
	_msgpack              struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space                 string
	ObjectsOperationsData []ObjectOperationsData
	Opts                  UpsertObjectManyOpts
}

// MakeUpsertObjectManyRequest returns a new empty UpsertObjectManyRequest.
func MakeUpsertObjectManyRequest(space string) UpsertObjectManyRequest {
	req := UpsertObjectManyRequest{}
	req.impl = newCall("crud.upsert_object_many")
	req.space = space
	req.objectsOperationsData = []ObjectOperationsData{}
	req.opts = UpsertObjectManyOpts{}
	return req
}

// ObjectOperationsData sets objects and operations
// for the UpsertObjectManyRequest request.
// Note: default value is nil.
func (req UpsertObjectManyRequest) ObjectsOperationsData(
	objectsOperationData []ObjectOperationsData) UpsertObjectManyRequest {
	req.objectsOperationsData = objectsOperationData
	return req
}

// Opts sets the options for the UpsertObjectManyRequest request.
// Note: default value is nil.
func (req UpsertObjectManyRequest) Opts(opts UpsertObjectManyOpts) UpsertObjectManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req UpsertObjectManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := upsertObjectManyArgs{Space: req.space, ObjectsOperationsData: req.objectsOperationsData,
		Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req UpsertObjectManyRequest) Context(ctx context.Context) UpsertObjectManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
