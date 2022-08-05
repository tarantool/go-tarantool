//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package datetime

import (
	"reflect"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func encodeUint(e *encoder, v uint64) error {
	return e.EncodeUint(uint(v))
}

func encodeInt(e *encoder, v int64) error {
	return e.EncodeInt(int(v))
}

func init() {
	msgpack.RegisterExt(datetime_extId, &Datetime{})

	msgpack.Register(reflect.TypeOf((*Interval)(nil)).Elem(), encodeInterval, decodeInterval)
	msgpack.RegisterExt(interval_extId, (*Interval)(nil))
}
