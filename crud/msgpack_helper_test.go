//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package crud_test

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

var newEncoder = msgpack.NewEncoder
