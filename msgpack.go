//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package tarantool

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
	msgpcode "gopkg.in/vmihailenco/msgpack.v2/codes"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func newEncoder(w io.Writer) *encoder {
	return msgpack.NewEncoder(w)
}

func newDecoder(r io.Reader) *decoder {
	return msgpack.NewDecoder(r)
}

func encodeUint(e *encoder, v uint64) error {
	return e.EncodeUint(uint(v))
}

func encodeInt(e *encoder, v int64) error {
	return e.EncodeInt(int(v))
}

func msgpackIsUint(code byte) bool {
	return code == msgpcode.Uint8 || code == msgpcode.Uint16 ||
		code == msgpcode.Uint32 || code == msgpcode.Uint64 ||
		msgpcode.IsFixedNum(code)
}

func msgpackIsMap(code byte) bool {
	return code == msgpcode.Map16 || code == msgpcode.Map32 || msgpcode.IsFixedMap(code)
}

func msgpackIsArray(code byte) bool {
	return code == msgpcode.Array16 || code == msgpcode.Array32 ||
		msgpcode.IsFixedArray(code)
}

func msgpackIsString(code byte) bool {
	return msgpcode.IsFixedString(code) || code == msgpcode.Str8 ||
		code == msgpcode.Str16 || code == msgpcode.Str32
}

func init() {
	msgpack.RegisterExt(errorExtID, &BoxError{})
}
