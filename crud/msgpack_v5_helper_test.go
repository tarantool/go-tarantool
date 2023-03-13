//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package crud_test

import (
	"github.com/vmihailenco/msgpack/v5"
)

var newEncoder = msgpack.NewEncoder
