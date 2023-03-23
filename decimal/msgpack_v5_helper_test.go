//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package decimal_test

import (
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/ice-blockchain/go-tarantool/decimal"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func toDecimal(i interface{}) (dec Decimal, ok bool) {
	var ptr *Decimal
	if ptr, ok = i.(*Decimal); ok {
		dec = *ptr
	}
	return
}

func marshal(v interface{}) ([]byte, error) {
	return msgpack.Marshal(v)
}

func unmarshal(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}
