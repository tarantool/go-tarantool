package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// ReplaceManyOpts describes options for `crud.replace_many` method.
type ReplaceManyOpts = OperationManyOpts

// ReplaceManyRequest helps you to create request object to call
// `crud.replace_many` for execution by a Connection.
type ReplaceManyRequest struct {
	spaceRequest
	tuples []Tuple
	opts   ReplaceManyOpts
}

type replaceManyArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Tuples   []Tuple
	Opts     ReplaceManyOpts
}

// MakeReplaceManyRequest returns a new empty ReplaceManyRequest.
func MakeReplaceManyRequest(space string) ReplaceManyRequest {
	req := ReplaceManyRequest{}
	req.impl = newCall("crud.replace_many")
	req.space = space
	req.opts = ReplaceManyOpts{}
	return req
}

// Tuples sets the tuples for the ReplaceManyRequest request.
// Note: default value is nil.
func (req ReplaceManyRequest) Tuples(tuples []Tuple) ReplaceManyRequest {
	req.tuples = tuples
	return req
}

// Opts sets the options for the ReplaceManyRequest request.
// Note: default value is nil.
func (req ReplaceManyRequest) Opts(opts ReplaceManyOpts) ReplaceManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req ReplaceManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.tuples == nil {
		req.tuples = []Tuple{}
	}
	args := replaceManyArgs{Space: req.space, Tuples: req.tuples, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req ReplaceManyRequest) Context(ctx context.Context) ReplaceManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// ReplaceObjectManyOpts describes options for `crud.replace_object_many` method.
type ReplaceObjectManyOpts = OperationObjectManyOpts

// ReplaceObjectManyRequest helps you to create request object to call
// `crud.replace_object_many` for execution by a Connection.
type ReplaceObjectManyRequest struct {
	spaceRequest
	objects []Object
	opts    ReplaceObjectManyOpts
}

type replaceObjectManyArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Objects  []Object
	Opts     ReplaceObjectManyOpts
}

// MakeReplaceObjectManyRequest returns a new empty ReplaceObjectManyRequest.
func MakeReplaceObjectManyRequest(space string) ReplaceObjectManyRequest {
	req := ReplaceObjectManyRequest{}
	req.impl = newCall("crud.replace_object_many")
	req.space = space
	req.opts = ReplaceObjectManyOpts{}
	return req
}

// Objects sets the tuple for the ReplaceObjectManyRequest request.
// Note: default value is nil.
func (req ReplaceObjectManyRequest) Objects(objects []Object) ReplaceObjectManyRequest {
	req.objects = objects
	return req
}

// Opts sets the options for the ReplaceObjectManyRequest request.
// Note: default value is nil.
func (req ReplaceObjectManyRequest) Opts(opts ReplaceObjectManyOpts) ReplaceObjectManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req ReplaceObjectManyRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.objects == nil {
		req.objects = []Object{}
	}
	args := replaceObjectManyArgs{Space: req.space, Objects: req.objects, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req ReplaceObjectManyRequest) Context(ctx context.Context) ReplaceObjectManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
