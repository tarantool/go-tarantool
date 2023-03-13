package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// MinOpts describes options for `crud.min` method.
type MinOpts = BorderOpts

// MinRequest helps you to create request object to call `crud.min`
// for execution by a Connection.
type MinRequest struct {
	spaceRequest
	index interface{}
	opts  MinOpts
}

type minArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Index    interface{}
	Opts     MinOpts
}

// NewMinRequest returns a new empty MinRequest.
func NewMinRequest(space string) *MinRequest {
	req := new(MinRequest)
	req.initImpl("crud.min")
	req.setSpace(space)
	req.index = []interface{}{}
	req.opts = MinOpts{}
	return req
}

// Index sets the index name/id for the MinRequest request.
// Note: default value is nil.
func (req *MinRequest) Index(index interface{}) *MinRequest {
	req.index = index
	return req
}

// Opts sets the options for the MinRequest request.
// Note: default value is nil.
func (req *MinRequest) Opts(opts MinOpts) *MinRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *MinRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := minArgs{Space: req.space, Index: req.index, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *MinRequest) Context(ctx context.Context) *MinRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
