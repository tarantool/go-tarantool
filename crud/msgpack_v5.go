//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

// Object is an interface to describe object for CRUD methods.
type Object interface {
	EncodeMsgpack(enc *encoder)
}

// MapObject is a type to describe object as a map.
type MapObject map[string]interface{}

func (o MapObject) EncodeMsgpack(enc *encoder) {
	enc.Encode(o)
}
