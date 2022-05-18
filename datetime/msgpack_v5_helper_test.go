//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package datetime_test

import (
	. "github.com/tarantool/go-tarantool/datetime"
	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func toDatetime(i interface{}) (dt Datetime, ok bool) {
	var ptr *Datetime
	if ptr, ok = i.(*Datetime); ok {
		dt = *ptr
	}
	return
}

func marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
