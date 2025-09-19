package arrow

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalArrow(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	opt := SomeOptionalArrow(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalArrow(t *testing.T) {
	opt := NoneOptionalArrow()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalArrow_MustGet(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	optSome := SomeOptionalArrow(val)
	optNone := NoneOptionalArrow()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalArrow_Unwrap(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	optSome := SomeOptionalArrow(val)
	optNone := NoneOptionalArrow()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, Arrow{}, optNone.Unwrap())
}

func TestOptionalArrow_UnwrapOr(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	def, err := MakeArrow([]byte{4, 5, 6})
	assert.NoError(t, err)
	optSome := SomeOptionalArrow(val)
	optNone := NoneOptionalArrow()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalArrow_UnwrapOrElse(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	def, err := MakeArrow([]byte{4, 5, 6})
	assert.NoError(t, err)
	optSome := SomeOptionalArrow(val)
	optNone := NoneOptionalArrow()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() Arrow { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() Arrow { return def }))
}

func TestOptionalArrow_EncodeDecodeMsgpack_Some(t *testing.T) {
	val, err := MakeArrow([]byte{1, 2, 3})
	assert.NoError(t, err)
	some := SomeOptionalArrow(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err = enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalArrow
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalArrow_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalArrow()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalArrow
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalArrow_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalArrow
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
