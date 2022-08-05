//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package datetime

import (
	"bytes"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func encodeUint(e *encoder, v uint64) error {
	return e.EncodeUint(v)
}

func encodeInt(e *encoder, v int64) error {
	return e.EncodeInt(v)
}

func init() {
	msgpack.RegisterExt(datetime_extId, (*Datetime)(nil))

	msgpack.RegisterExtEncoder(interval_extId, Interval{},
		func(e *msgpack.Encoder, v reflect.Value) (ret []byte, err error) {
			var b bytes.Buffer

			enc := msgpack.NewEncoder(&b)
			if err = encodeInterval(enc, v); err == nil {
				ret = b.Bytes()
			}

			return
		})
	msgpack.RegisterExtDecoder(interval_extId, Interval{},
		func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
			return decodeInterval(d, v)
		})
}
