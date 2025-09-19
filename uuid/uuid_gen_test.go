package uuid

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalUUID(t *testing.T) {
	val := uuid.New()
	opt := SomeOptionalUUID(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalUUID(t *testing.T) {
	opt := NoneOptionalUUID()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalUUID_MustGet(t *testing.T) {
	val := uuid.New()
	optSome := SomeOptionalUUID(val)
	optNone := NoneOptionalUUID()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalUUID_Unwrap(t *testing.T) {
	val := uuid.New()
	optSome := SomeOptionalUUID(val)
	optNone := NoneOptionalUUID()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, uuid.Nil, optNone.Unwrap())
}

func TestOptionalUUID_UnwrapOr(t *testing.T) {
	val := uuid.New()
	def := uuid.New()
	optSome := SomeOptionalUUID(val)
	optNone := NoneOptionalUUID()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalUUID_UnwrapOrElse(t *testing.T) {
	val := uuid.New()
	def := uuid.New()
	optSome := SomeOptionalUUID(val)
	optNone := NoneOptionalUUID()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() uuid.UUID { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() uuid.UUID { return def }))
}

func TestOptionalUUID_EncodeDecodeMsgpack_Some(t *testing.T) {
	val := uuid.New()
	some := SomeOptionalUUID(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalUUID
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalUUID_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalUUID()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalUUID
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalUUID_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalUUID
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
