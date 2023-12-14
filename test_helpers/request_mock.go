package test_helpers

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
)

type StrangerRequest struct {
}

func NewStrangerRequest() *StrangerRequest {
	return &StrangerRequest{}
}

func (sr *StrangerRequest) Type() iproto.Type {
	return iproto.Type(0)
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

func (sr *StrangerRequest) Response(header tarantool.Header,
	body io.Reader) (tarantool.Response, error) {
	resp := tarantool.BaseResponse{}
	resp.SetHeader(header)
	return &resp, nil
}
