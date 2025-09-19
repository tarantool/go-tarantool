package datetime

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalDatetime(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	opt := SomeOptionalDatetime(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalDatetime(t *testing.T) {
	opt := NoneOptionalDatetime()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalDatetime_MustGet(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	optSome := SomeOptionalDatetime(val)
	optNone := NoneOptionalDatetime()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalDatetime_Unwrap(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	optSome := SomeOptionalDatetime(val)
	optNone := NoneOptionalDatetime()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, Datetime{}, optNone.Unwrap())
}

func TestOptionalDatetime_UnwrapOr(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	def, err := MakeDatetime(time.Now().Add(1 * time.Hour).In(time.UTC))
	assert.NoError(t, err)
	optSome := SomeOptionalDatetime(val)
	optNone := NoneOptionalDatetime()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalDatetime_UnwrapOrElse(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	def, err := MakeDatetime(time.Now().Add(1 * time.Hour).In(time.UTC))
	assert.NoError(t, err)
	optSome := SomeOptionalDatetime(val)
	optNone := NoneOptionalDatetime()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() Datetime { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() Datetime { return def }))
}

func TestOptionalDatetime_EncodeDecodeMsgpack_Some(t *testing.T) {
	val, err := MakeDatetime(time.Now().In(time.UTC))
	assert.NoError(t, err)
	some := SomeOptionalDatetime(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err = enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalDatetime
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalDatetime_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalDatetime()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalDatetime
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalDatetime_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalDatetime
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
