package datetime

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalInterval(t *testing.T) {
	val := Interval{Year: 1}
	opt := SomeOptionalInterval(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalInterval(t *testing.T) {
	opt := NoneOptionalInterval()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalInterval_MustGet(t *testing.T) {
	val := Interval{Year: 1}
	optSome := SomeOptionalInterval(val)
	optNone := NoneOptionalInterval()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalInterval_Unwrap(t *testing.T) {
	val := Interval{Year: 1}
	optSome := SomeOptionalInterval(val)
	optNone := NoneOptionalInterval()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, Interval{}, optNone.Unwrap())
}

func TestOptionalInterval_UnwrapOr(t *testing.T) {
	val := Interval{Year: 1}
	def := Interval{Year: 2}
	optSome := SomeOptionalInterval(val)
	optNone := NoneOptionalInterval()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalInterval_UnwrapOrElse(t *testing.T) {
	val := Interval{Year: 1}
	def := Interval{Year: 2}
	optSome := SomeOptionalInterval(val)
	optNone := NoneOptionalInterval()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() Interval { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() Interval { return def }))
}

func TestOptionalInterval_EncodeDecodeMsgpack_Some(t *testing.T) {
	val := Interval{Year: 1}
	some := SomeOptionalInterval(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalInterval
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalInterval_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalInterval()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalInterval
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalInterval_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalInterval
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
