//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package tarantool_test

import (
	"github.com/tarantool/go-tarantool/v2"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func encodeUint(e *encoder, v uint64) error {
	return e.EncodeUint(uint(v))
}

func toBoxError(i interface{}) (v tarantool.BoxError, ok bool) {
	v, ok = i.(tarantool.BoxError)
	return
}

func marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
