//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package settings_test

import (
	"io"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/ice-blockchain/go-tarantool"
)

type encoder = msgpack.Encoder

func NewEncoder(w io.Writer) *encoder {
	return msgpack.NewEncoder(w)
}

func toBoxError(i interface{}) (v tarantool.BoxError, ok bool) {
	var ptr *tarantool.BoxError
	if ptr, ok = i.(*tarantool.BoxError); ok {
		v = *ptr
	}
	return
}
