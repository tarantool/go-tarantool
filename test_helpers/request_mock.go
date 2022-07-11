package test_helpers

import (
	"github.com/tarantool/go-tarantool"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type StrangerRequest struct {
}

func NewStrangerRequest() *StrangerRequest {
	return &StrangerRequest{}
}

func (sr *StrangerRequest) Code() int32 {
	return 0
}

func (sr *StrangerRequest) Body(resolver tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	return nil
}

func (sr *StrangerRequest) Conn() *tarantool.Connection {
	return &tarantool.Connection{}
}
