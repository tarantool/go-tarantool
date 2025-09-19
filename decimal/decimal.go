// Package decimal with support of Tarantool's decimal data type.
//
// Decimal data type supported in Tarantool since 2.2.
//
// Since: 1.7.0
//
// See also:
//
//   - Tarantool MessagePack extensions:
//     https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/#the-decimal-type
//
//   - Tarantool data model:
//     https://www.tarantool.io/en/doc/latest/book/box/data_model/
//
//   - Tarantool issue for support decimal type:
//     https://github.com/tarantool/tarantool/issues/692
//
//   - Tarantool module decimal:
//     https://www.tarantool.io/en/doc/latest/reference/reference_lua/decimal/
package decimal

import (
	"fmt"
	"reflect"

	"github.com/shopspring/decimal"
	"github.com/vmihailenco/msgpack/v5"
)

// Decimal numbers have 38 digits of precision, that is, the total
// number of digits before and after the decimal point can be 38.
// A decimal operation will fail if overflow happens (when a number is
// greater than 10^38 - 1 or less than -10^38 - 1).
//
// See also:
//
// - Tarantool module decimal:
//   https://www.tarantool.io/en/doc/latest/reference/reference_lua/decimal/

const (
	// Decimal external type.
	decimalExtID     = 1
	decimalPrecision = 38
)

var (
	one = decimal.NewFromInt(1)
	// -10^decimalPrecision - 1
	minSupportedDecimal = maxSupportedDecimal.Neg().Sub(one)
	// 10^decimalPrecision - 1
	maxSupportedDecimal = decimal.New(1, decimalPrecision).Sub(one)
)

//go:generate go tool gentypes -ext-code 1 Decimal
type Decimal struct {
	decimal.Decimal
}

// MakeDecimal creates a new Decimal from a decimal.Decimal.
func MakeDecimal(decimal decimal.Decimal) Decimal {
	return Decimal{Decimal: decimal}
}

// MakeDecimalFromString creates a new Decimal from a string.
func MakeDecimalFromString(src string) (Decimal, error) {
	result := Decimal{}
	dec, err := decimal.NewFromString(src)
	if err != nil {
		return result, err
	}
	result = MakeDecimal(dec)
	return result, nil
}

var (
	ErrDecimalOverflow = fmt.Errorf("msgpack: decimal number is bigger than"+
		" maximum supported number (10^%d - 1)", decimalPrecision)
	ErrDecimalUnderflow = fmt.Errorf("msgpack: decimal number is lesser than"+
		" minimum supported number (-10^%d - 1)", decimalPrecision)
)

// MarshalMsgpack implements a custom msgpack marshaler.
func (d Decimal) MarshalMsgpack() ([]byte, error) {
	switch {
	case d.GreaterThan(maxSupportedDecimal):
		return nil, ErrDecimalOverflow
	case d.LessThan(minSupportedDecimal):
		return nil, ErrDecimalUnderflow
	}

	// Decimal values can be encoded to fixext MessagePack, where buffer
	// has a fixed length encoded by first byte, and ext MessagePack, where
	// buffer length is not fixed and encoded by a number in a separate
	// field:
	//
	//  +--------+-------------------+------------+===============+
	//  | MP_EXT | length (optional) | MP_DECIMAL | PackedDecimal |
	//  +--------+-------------------+------------+===============+
	strBuf := d.String()
	bcdBuf, err := encodeStringToBCD(strBuf)
	if err != nil {
		return nil, fmt.Errorf("msgpack: can't encode string (%s) to a BCD buffer: %w", strBuf, err)
	}
	return bcdBuf, nil
}

// UnmarshalMsgpack implements a custom msgpack unmarshaler.
func (d *Decimal) UnmarshalMsgpack(data []byte) error {
	digits, exp, err := decodeStringFromBCD(data)
	if err != nil {
		return fmt.Errorf("msgpack: can't decode string from BCD buffer (%x): %w", data, err)
	}

	dec, err := decimal.NewFromString(digits)
	if err != nil {
		return fmt.Errorf("msgpack: can't encode string (%s) to a decimal number: %w", digits, err)
	}

	if exp != 0 {
		dec = dec.Shift(int32(exp))
	}

	*d = MakeDecimal(dec)
	return nil
}

func decimalEncoder(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
	dec := v.Interface().(Decimal)

	return dec.MarshalMsgpack()
}

func decimalDecoder(d *msgpack.Decoder, v reflect.Value, extLen int) error {
	b := make([]byte, extLen)

	switch n, err := d.Buffered().Read(b); {
	case err != nil:
		return err
	case n < extLen:
		return fmt.Errorf("msgpack: unexpected end of stream after %d decimal bytes", n)
	}

	ptr := v.Addr().Interface().(*Decimal)
	return ptr.UnmarshalMsgpack(b)
}

func init() {
	msgpack.RegisterExtDecoder(decimalExtID, Decimal{}, decimalDecoder)
	msgpack.RegisterExtEncoder(decimalExtID, Decimal{}, decimalEncoder)
}
