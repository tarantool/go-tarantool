package tarantool

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalBoxError(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	opt := SomeOptionalBoxError(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalBoxError(t *testing.T) {
	opt := NoneOptionalBoxError()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalBoxError_MustGet(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	optSome := SomeOptionalBoxError(val)
	optNone := NoneOptionalBoxError()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalBoxError_Unwrap(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	optSome := SomeOptionalBoxError(val)
	optNone := NoneOptionalBoxError()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, BoxError{}, optNone.Unwrap())
}

func TestOptionalBoxError_UnwrapOr(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	def := BoxError{Code: 2, Msg: "default"}
	optSome := SomeOptionalBoxError(val)
	optNone := NoneOptionalBoxError()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalBoxError_UnwrapOrElse(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	def := BoxError{Code: 2, Msg: "default"}
	optSome := SomeOptionalBoxError(val)
	optNone := NoneOptionalBoxError()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() BoxError { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() BoxError { return def }))
}

func TestOptionalBoxError_EncodeDecodeMsgpack_Some(t *testing.T) {
	val := BoxError{Code: 1, Msg: "error"}
	some := SomeOptionalBoxError(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalBoxError
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalBoxError_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalBoxError()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalBoxError
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalBoxError_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalBoxError
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
