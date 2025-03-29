package tarantool

import (
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

func untypedMapDecoder(dec *msgpack.Decoder) (interface{}, error) {
	return dec.DecodeUntypedMap()
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	d := msgpack.GetDecoder()

	d.Reset(r)
	d.SetMapDecoder(untypedMapDecoder)

	return d
}

func putDecoder(dec *msgpack.Decoder) {
	msgpack.PutDecoder(dec)
}
