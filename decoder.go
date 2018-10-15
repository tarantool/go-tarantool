package tarantool

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v4"
)

type DecoderFactory func(r io.Reader) *msgpack.Decoder

var DefaultDecoder = func(r io.Reader) *msgpack.Decoder { return msgpack.NewDecoder(r) }
