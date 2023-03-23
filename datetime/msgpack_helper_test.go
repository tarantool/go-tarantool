//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package datetime_test

import (
	. "github.com/ice-blockchain/go-tarantool/datetime"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func toDatetime(i interface{}) (dt Datetime, ok bool) {
	dt, ok = i.(Datetime)
	return
}

func marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
