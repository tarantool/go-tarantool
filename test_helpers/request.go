package test_helpers

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
)

// MockRequest is an empty mock request used for testing purposes.
type MockRequest struct {
}

// NewMockRequest creates an empty MockRequest.
func NewMockRequest() *MockRequest {
	return &MockRequest{}
}

// Type returns an iproto type for MockRequest.
func (req *MockRequest) Type() iproto.Type {
	return iproto.Type(0)
}

// Async returns if MockRequest expects a response.
func (req *MockRequest) Async() bool {
	return false
}

// Body fills an msgpack.Encoder with the watch request body.
func (req *MockRequest) Body(resolver tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	return nil
}

// Conn returns the Connection object the request belongs to.
func (req *MockRequest) Conn() *tarantool.Connection {
	return &tarantool.Connection{}
}

// Ctx returns a context of the MockRequest.
func (req *MockRequest) Ctx() context.Context {
	return nil
}

// Response creates a response for the MockRequest.
func (req *MockRequest) Response(header tarantool.Header,
	body io.Reader) (tarantool.Response, error) {
	resp, err := CreateMockResponse(header, body)
	return resp, err
}
