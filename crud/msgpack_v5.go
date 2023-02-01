//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package crud

import (
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder

func NewEncoder(w io.Writer) *encoder {
	return msgpack.NewEncoder(w)
}
