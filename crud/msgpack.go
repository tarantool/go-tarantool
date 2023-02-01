//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package crud

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type encoder = msgpack.Encoder

func NewEncoder(w io.Writer) *encoder {
	return msgpack.NewEncoder(w)
}
