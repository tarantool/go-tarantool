package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

type baseRequest struct {
	impl *tarantool.CallRequest
}

func (req *baseRequest) initImpl(methodName string) {
	req.impl = tarantool.NewCall17Request(methodName)
}

// Code returns IPROTO code for CRUD request.
func (req *baseRequest) Code() int32 {
	return req.impl.Code()
}

// Ctx returns a context of CRUD request.
func (req *baseRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns is CRUD request expects a response.
func (req *baseRequest) Async() bool {
	return req.impl.Async()
}

type spaceRequest struct {
	baseRequest
	space interface{}
}

func (req *spaceRequest) setSpace(space interface{}) {
	req.space = space
}
