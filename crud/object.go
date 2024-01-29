package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Object is an interface to describe object for CRUD methods. It can be any
// type that msgpack can encode as a map.
type Object = interface{}

// Objects is a type to describe an array of object for CRUD methods. It can be
// any type that msgpack can encode, but encoded data must be an array of
// objects.
//
// See the reason why not just []Object:
// https://github.com/tarantool/go-tarantool/issues/365
type Objects = interface{}

// MapObject is a type to describe object as a map.
type MapObject map[string]interface{}

func (o MapObject) EncodeMsgpack(enc *msgpack.Encoder) {
	enc.Encode(o)
}
