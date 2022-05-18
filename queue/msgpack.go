//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package queue

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

type decoder = msgpack.Decoder
