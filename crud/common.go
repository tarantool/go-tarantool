// Package crud with support of API of Tarantool's CRUD module.
//
// Supported CRUD methods:
//
//   - insert
//
//   - insert_object
//
//   - insert_many
//
//   - insert_object_many
//
//   - get
//
//   - update
//
//   - delete
//
//   - replace
//
//   - replace_object
//
//   - replace_many
//
//   - replace_object_many
//
//   - upsert
//
//   - upsert_object
//
//   - upsert_many
//
//   - upsert_object_many
//
//   - select
//
//   - min
//
//   - max
//
//   - truncate
//
//   - len
//
//   - storage_info
//
//   - count
//
//   - stats
//
//   - unflatten_rows
//
// Since: 1.11.0.
package crud

import (
	"context"
	"io"

	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v2"
)

type baseRequest struct {
	impl *tarantool.CallRequest
}

func newCall(method string) *tarantool.CallRequest {
	return tarantool.NewCall17Request(method)
}

// Type returns IPROTO type for CRUD request.
func (req baseRequest) Type() iproto.Type {
	return req.impl.Type()
}

// Ctx returns a context of CRUD request.
func (req baseRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns is CRUD request expects a response.
func (req baseRequest) Async() bool {
	return req.impl.Async()
}

// Response creates a response for the baseRequest.
func (req baseRequest) Response(header tarantool.Header,
	body io.Reader) (tarantool.Response, error) {
	return req.impl.Response(header, body)
}

type spaceRequest struct {
	baseRequest
	space string
}
