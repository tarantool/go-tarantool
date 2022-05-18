//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package queue_test

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder
