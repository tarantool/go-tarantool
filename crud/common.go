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

	"github.com/ice-blockchain/go-tarantool"
)

type baseRequest struct {
	impl *tarantool.CallRequest
}

func newCall(method string) *tarantool.CallRequest {
	return tarantool.NewCall17Request(method)
}

// Code returns IPROTO code for CRUD request.
func (req baseRequest) Code() int32 {
	return req.impl.Code()
}

// Ctx returns a context of CRUD request.
func (req baseRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns is CRUD request expects a response.
func (req baseRequest) Async() bool {
	return req.impl.Async()
}

type spaceRequest struct {
	baseRequest
	space string
}
