package test_helpers

import (
	"context"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
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

func (sr *StrangerRequest) Body(resolver tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	return nil
}

func (sr *StrangerRequest) Conn() *tarantool.Connection {
	return &tarantool.Connection{}
}

func (sr *StrangerRequest) Ctx() context.Context {
	return nil
}
