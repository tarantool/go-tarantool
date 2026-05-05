package crud

import (
	"context"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// StatsRequest helps you to create request object to call `crud.stats`
// for execution by a Connection.
type StatsRequest struct {
	baseRequest

	space option.String
}

// MakeStatsRequest returns a new empty StatsRequest.
func MakeStatsRequest() StatsRequest {
	return StatsRequest{
		baseRequest: newBaseRequest("crud.stats"),
	}
}

// Space sets the space name for the StatsRequest request.
// Note: default value is nil.
func (req StatsRequest) Space(space string) StatsRequest {
	req.space = option.SomeString(space)
	return req
}

// Body fills an encoder with the call request body.
func (req StatsRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if value, ok := req.space.Get(); ok {
		req.impl = req.impl.Args([]any{value})
	} else {
		req.impl = req.impl.Args([]any{})
	}

	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req StatsRequest) Context(ctx context.Context) StatsRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
