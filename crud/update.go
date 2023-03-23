package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// UpdateOpts describes options for `crud.update` method.
type UpdateOpts = SimpleOperationOpts

// UpdateRequest helps you to create request object to call `crud.update`
// for execution by a Connection.
type UpdateRequest struct {
	spaceRequest
	key        Tuple
	operations []Operation
	opts       UpdateOpts
}

type updateArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space      string
	Key        Tuple
	Operations []Operation
	Opts       UpdateOpts
}

// MakeUpdateRequest returns a new empty UpdateRequest.
func MakeUpdateRequest(space string) UpdateRequest {
	req := UpdateRequest{}
	req.impl = newCall("crud.update")
	req.space = space
	req.operations = []Operation{}
	req.opts = UpdateOpts{}
	return req
}

// Key sets the key for the UpdateRequest request.
// Note: default value is nil.
func (req UpdateRequest) Key(key Tuple) UpdateRequest {
	req.key = key
	return req
}

// Operations sets the operations for UpdateRequest request.
// Note: default value is nil.
func (req UpdateRequest) Operations(operations []Operation) UpdateRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the UpdateRequest request.
// Note: default value is nil.
func (req UpdateRequest) Opts(opts UpdateOpts) UpdateRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req UpdateRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.key == nil {
		req.key = []interface{}{}
	}
	args := updateArgs{Space: req.space, Key: req.key,
		Operations: req.operations, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req UpdateRequest) Context(ctx context.Context) UpdateRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
