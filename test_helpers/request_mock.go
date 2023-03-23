package test_helpers

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

type StrangerRequest struct {
}

func NewStrangerRequest() *StrangerRequest {
	return &StrangerRequest{}
}

func (sr *StrangerRequest) Code() int32 {
	return 0
}

func (sr *StrangerRequest) Async() bool {
	return false
}

func (sr *StrangerRequest) Body(resolver tarantool.SchemaResolver, enc *encoder) error {
	return nil
}

func (sr *StrangerRequest) Conn() *tarantool.Connection {
	return &tarantool.Connection{}
}

func (sr *StrangerRequest) Ctx() context.Context {
	return nil
}
