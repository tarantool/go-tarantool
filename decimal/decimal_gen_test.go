package decimal

import (
	"bytes"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestSomeOptionalDecimal(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	opt := SomeOptionalDecimal(val)

	assert.True(t, opt.IsSome())
	assert.False(t, opt.IsZero())

	v, ok := opt.Get()
	assert.True(t, ok)
	assert.Equal(t, val, v)
}

func TestNoneOptionalDecimal(t *testing.T) {
	opt := NoneOptionalDecimal()

	assert.False(t, opt.IsSome())
	assert.True(t, opt.IsZero())

	_, ok := opt.Get()
	assert.False(t, ok)
}

func TestOptionalDecimal_MustGet(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	optSome := SomeOptionalDecimal(val)
	optNone := NoneOptionalDecimal()

	assert.Equal(t, val, optSome.MustGet())
	assert.Panics(t, func() { optNone.MustGet() })
}

func TestOptionalDecimal_Unwrap(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	optSome := SomeOptionalDecimal(val)
	optNone := NoneOptionalDecimal()

	assert.Equal(t, val, optSome.Unwrap())
	assert.Equal(t, Decimal{}, optNone.Unwrap())
}

func TestOptionalDecimal_UnwrapOr(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	def := MakeDecimal(decimal.NewFromFloat(4.56))
	optSome := SomeOptionalDecimal(val)
	optNone := NoneOptionalDecimal()

	assert.Equal(t, val, optSome.UnwrapOr(def))
	assert.Equal(t, def, optNone.UnwrapOr(def))
}

func TestOptionalDecimal_UnwrapOrElse(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	def := MakeDecimal(decimal.NewFromFloat(4.56))
	optSome := SomeOptionalDecimal(val)
	optNone := NoneOptionalDecimal()

	assert.Equal(t, val, optSome.UnwrapOrElse(func() Decimal { return def }))
	assert.Equal(t, def, optNone.UnwrapOrElse(func() Decimal { return def }))
}

func TestOptionalDecimal_EncodeDecodeMsgpack_Some(t *testing.T) {
	val := MakeDecimal(decimal.NewFromFloat(1.23))
	some := SomeOptionalDecimal(val)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(some)
	assert.NoError(t, err)

	var decodedSome OptionalDecimal
	err = dec.Decode(&decodedSome)
	assert.NoError(t, err)
	assert.True(t, decodedSome.IsSome())
	assert.Equal(t, val, decodedSome.Unwrap())
}

func TestOptionalDecimal_EncodeDecodeMsgpack_None(t *testing.T) {
	none := NoneOptionalDecimal()

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(none)
	assert.NoError(t, err)

	var decodedNone OptionalDecimal
	err = dec.Decode(&decodedNone)
	assert.NoError(t, err)
	assert.True(t, decodedNone.IsZero())
}

func TestOptionalDecimal_EncodeDecodeMsgpack_InvalidType(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	dec := msgpack.NewDecoder(&buf)

	err := enc.Encode(123)
	assert.NoError(t, err)

	var decodedInvalid OptionalDecimal
	err = dec.Decode(&decodedInvalid)
	assert.Error(t, err)
}
