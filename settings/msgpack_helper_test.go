//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package settings_test

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"

	"github.com/ice-blockchain/go-tarantool"
)

type encoder = msgpack.Encoder

func NewEncoder(w io.Writer) *encoder {
	return msgpack.NewEncoder(w)
}

func toBoxError(i interface{}) (v tarantool.BoxError, ok bool) {
	v, ok = i.(tarantool.BoxError)
	return
}
