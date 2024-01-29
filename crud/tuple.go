package crud

// Tuple is a type to describe tuple for CRUD methods. It can be any type that
// msgpask can encode as an array.
type Tuple = interface{}

// Tuples is a type to describe an array of tuples for CRUD methods. It can be
// any type that msgpack can encode, but encoded data must be an array of
// tuples.
//
// See the reason why not just []Tuple:
// https://github.com/tarantool/go-tarantool/issues/365
type Tuples = interface{}
