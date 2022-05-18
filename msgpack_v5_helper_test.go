//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package tarantool_test

import (
	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func encodeUint(e *encoder, v uint64) error {
	return e.EncodeUint(v)
}
