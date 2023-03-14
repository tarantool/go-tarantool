package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// StatsRequest helps you to create request object to call `crud.stats`
// for execution by a Connection.
type StatsRequest struct {
	baseRequest
	space OptString
}

// NewStatsRequest returns a new empty StatsRequest.
func NewStatsRequest() *StatsRequest {
	req := new(StatsRequest)
	req.impl = newCall("crud.stats")
	return req
}

// Space sets the space name for the StatsRequest request.
// Note: default value is nil.
func (req *StatsRequest) Space(space string) *StatsRequest {
	req.space = MakeOptString(space)
	return req
}

// Body fills an encoder with the call request body.
func (req *StatsRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := []interface{}{}
	if value, ok := req.space.Get(); ok {
		args = []interface{}{value}
	}
	req.impl.Args(args)

	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *StatsRequest) Context(ctx context.Context) *StatsRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
