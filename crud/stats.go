package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// StatsRequest helps you to create request object to call `crud.stats`
// for execution by a Connection.
type StatsRequest struct {
	baseRequest
	space OptString
}

// MakeStatsRequest returns a new empty StatsRequest.
func MakeStatsRequest() StatsRequest {
	req := StatsRequest{}
	req.impl = newCall("crud.stats")
	return req
}

// Space sets the space name for the StatsRequest request.
// Note: default value is nil.
func (req StatsRequest) Space(space string) StatsRequest {
	req.space = MakeOptString(space)
	return req
}

// Body fills an encoder with the call request body.
func (req StatsRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if value, ok := req.space.Get(); ok {
		req.impl = req.impl.Args([]interface{}{value})
	} else {
		req.impl = req.impl.Args([]interface{}{})
	}

	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req StatsRequest) Context(ctx context.Context) StatsRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
