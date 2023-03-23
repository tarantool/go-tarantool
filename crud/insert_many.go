package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// InsertManyOpts describes options for `crud.insert_many` method.
type InsertManyOpts = OperationManyOpts

// InsertManyRequest helps you to create request object to call
// `crud.insert_many` for execution by a Connection.
type InsertManyRequest struct {
	spaceRequest
	tuples []Tuple
	opts   InsertManyOpts
}

type insertManyArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Tuples   []Tuple
	Opts     InsertManyOpts
}

// MakeInsertManyRequest returns a new empty InsertManyRequest.
func MakeInsertManyRequest(space string) InsertManyRequest {
	req := InsertManyRequest{}
	req.impl = newCall("crud.insert_many")
	req.space = space
	req.opts = InsertManyOpts{}
	return req
}

// Tuples sets the tuples for the InsertManyRequest request.
// Note: default value is nil.
func (req InsertManyRequest) Tuples(tuples []Tuple) InsertManyRequest {
	req.tuples = tuples
	return req
}

// Opts sets the options for the InsertManyRequest request.
// Note: default value is nil.
func (req InsertManyRequest) Opts(opts InsertManyOpts) InsertManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req InsertManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.tuples == nil {
		req.tuples = []Tuple{}
	}
	args := insertManyArgs{Space: req.space, Tuples: req.tuples, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req InsertManyRequest) Context(ctx context.Context) InsertManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// InsertObjectManyOpts describes options for `crud.insert_object_many` method.
type InsertObjectManyOpts = OperationObjectManyOpts

// InsertObjectManyRequest helps you to create request object to call
// `crud.insert_object_many` for execution by a Connection.
type InsertObjectManyRequest struct {
	spaceRequest
	objects []Object
	opts    InsertObjectManyOpts
}

type insertObjectManyArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Objects  []Object
	Opts     InsertObjectManyOpts
}

// MakeInsertObjectManyRequest returns a new empty InsertObjectManyRequest.
func MakeInsertObjectManyRequest(space string) InsertObjectManyRequest {
	req := InsertObjectManyRequest{}
	req.impl = newCall("crud.insert_object_many")
	req.space = space
	req.opts = InsertObjectManyOpts{}
	return req
}

// Objects sets the objects for the InsertObjectManyRequest request.
// Note: default value is nil.
func (req InsertObjectManyRequest) Objects(objects []Object) InsertObjectManyRequest {
	req.objects = objects
	return req
}

// Opts sets the options for the InsertObjectManyRequest request.
// Note: default value is nil.
func (req InsertObjectManyRequest) Opts(opts InsertObjectManyOpts) InsertObjectManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req InsertObjectManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.objects == nil {
		req.objects = []Object{}
	}
	args := insertObjectManyArgs{Space: req.space, Objects: req.objects, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req InsertObjectManyRequest) Context(ctx context.Context) InsertObjectManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
