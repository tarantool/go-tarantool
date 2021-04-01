package tarantool

import (
	"github.com/vmihailenco/msgpack/v5"
)

// IntKey is utility type for passing integer key to Select*, Update* and Delete*
// It serializes to array with single integer element.
type IntKey struct {
	I int
}

func (k IntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeInt(int64(k.I))
	return nil
}

// UintKey is utility type for passing unsigned integer key to Select*, Update* and Delete*
// It serializes to array with single integer element.
type UintKey struct {
	I uint
}

func (k UintKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeUint(uint64(k.I))
	return nil
}

// UintKey is utility type for passing string key to Select*, Update* and Delete*
// It serializes to array with single string element.
type StringKey struct {
	S string
}

func (k StringKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeString(k.S)
	return nil
}

// IntIntKey is utility type for passing two integer keys to Select*, Update* and Delete*
// It serializes to array with two integer elements
type IntIntKey struct {
	I1, I2 int
}

func (k IntIntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(2)
	enc.EncodeInt(int64(k.I1))
	enc.EncodeInt(int64(k.I2))
	return nil
}

// Op - is update operation
type Op struct {
	Op    string
	Field int
	Arg   interface{}
}

func (o Op) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(3)
	enc.EncodeString(o.Op)
	enc.EncodeInt(int64(o.Field))
	return enc.Encode(o.Arg)
}

type OpSplice struct {
	Op      string
	Field   int
	Pos     int
	Len     int
	Replace string
}

func (o OpSplice) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(5)
	enc.EncodeString(o.Op)
	enc.EncodeInt(int64(o.Field))
	enc.EncodeInt(int64(o.Pos))
	enc.EncodeInt(int64(o.Len))
	enc.EncodeString(o.Replace)
	return nil
}
