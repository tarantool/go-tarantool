package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Object is an interface to describe object for CRUD methods. It can be any
// type that msgpack can encode as a map.
type Object = any

// Objects is a type to describe an array of object for CRUD methods. It can be
// any type that msgpack can encode, but encoded data must be an array of
// objects.
//
// See the reason why not just []Object:
// https://github.com/tarantool/go-tarantool/issues/365
type Objects = any

// MapObject is a type to describe object as a map.
type MapObject map[string]any

func (o MapObject) EncodeMsgpack(enc *msgpack.Encoder) {
	_ = enc.Encode(o)
}
