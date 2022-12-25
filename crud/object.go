package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Object is an interface to describe object for CRUD methods.
type Object interface {
	EncodeMsgpack(enc *msgpack.Encoder)
}

// MapObject is a type to describe object as a map.
type MapObject map[string]interface{}

func (o MapObject) EncodeMsgpack(enc *msgpack.Encoder) {
	enc.Encode(o)
}
