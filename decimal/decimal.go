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
	"math"
	"reflect"
	"strconv"
	"strings"

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

// This method converts the decimal type to a string.
// Use shopspring/decimal by default.
// StringOptimized - optimized version for Tarantool Decimal
// taking into account the limitations of int64 and support for large numbers via fallback
// Tarantool decimal has 38 digits, which can exceed int64.
// Therefore, we cannot use int64 for all cases.
// For the general case, use shopspring/decimal.String().
// For cases where it is known that numbers contain less than 26 characters,
// you can use the optimized version.
func (d Decimal) StringOptimized() string {
	coefficient := d.Decimal.Coefficient() // Note: In shopspring/decimal, the number is stored as coefficient *10^exponent, where exponent can be negative.
	exponent := d.Decimal.Exponent()

	// If exponent is positive, then we use the standard method.
	if exponent > 0 {
		return d.Decimal.String()
	}

	scale := -exponent

	if !coefficient.IsInt64() {
		return d.Decimal.String()
	}

	int64Value := coefficient.Int64()

	return d.stringFromInt64(int64Value, int(scale))
}

// StringFromInt64 is an internal method for converting int64
// and scale to a string (for numbers up to 19 digits)
func (d Decimal) stringFromInt64(value int64, scale int) string {

	var buf [48]byte
	pos := 0

	negative := value < 0
	if negative {
		if value == math.MinInt64 {
			return d.handleMinInt64(scale)
		}
		buf[pos] = '-'
		pos++
		value = -value
	}

	str := strconv.FormatInt(value, 10)
	length := len(str)

	if scale == 0 {
		if pos+length > len(buf) {
			return d.Decimal.String()
		}
		copy(buf[pos:], str)
		pos += length
		return string(buf[:pos])
	}

	if scale >= length {

		required := 2 + (scale - length) + length
		if pos+required > len(buf) {
			return d.Decimal.String()
		}

		buf[pos] = '0'
		buf[pos+1] = '.'
		pos += 2

		zeros := scale - length
		for i := 0; i < zeros; i++ {
			buf[pos] = '0'
			pos++
		}

		copy(buf[pos:], str)
		pos += length
	} else {

		integerLen := length - scale

		required := integerLen + 1 + scale
		if pos+required > len(buf) {
			return d.Decimal.String()
		}

		copy(buf[pos:], str[:integerLen])
		pos += integerLen

		buf[pos] = '.'
		pos++

		copy(buf[pos:], str[integerLen:])
		pos += scale
	}

	return string(buf[:pos])
}
func (d Decimal) handleMinInt64(scale int) string {
	const minInt64Str = "9223372036854775808"

	var buf [48]byte
	pos := 0

	buf[pos] = '-'
	pos++

	length := len(minInt64Str)

	if scale == 0 {
		if pos+length > len(buf) {
			return "-" + minInt64Str // Fallback.
		}
		copy(buf[pos:], minInt64Str)
		pos += length
		return string(buf[:pos])
	}

	if scale >= length {
		required := 2 + (scale - length) + length
		if pos+required > len(buf) {
			// Fallback.
			result := "0." + strings.Repeat("0", scale-length) + minInt64Str
			return "-" + result
		}

		buf[pos] = '0'
		buf[pos+1] = '.'
		pos += 2

		zeros := scale - length
		for i := 0; i < zeros; i++ {
			buf[pos] = '0'
			pos++
		}

		copy(buf[pos:], minInt64Str)
		pos += length
	} else {
		integerLen := length - scale
		required := integerLen + 1 + scale
		if pos+required > len(buf) {
			return d.Decimal.String() // Fallback
		}

		copy(buf[pos:], minInt64Str[:integerLen])
		pos += integerLen

		buf[pos] = '.'
		pos++

		copy(buf[pos:], minInt64Str[integerLen:])
		pos += scale
	}

	return string(buf[:pos])
}

func MustMakeDecimal(src string) Decimal {
	dec, err := MakeDecimalFromString(src)
	if err != nil {
		panic(fmt.Sprintf("MustMakeDecimalFromString: %v", err))
	}
	return dec
}

func init() {
	msgpack.RegisterExtDecoder(decimalExtID, Decimal{}, decimalDecoder)
	msgpack.RegisterExtEncoder(decimalExtID, Decimal{}, decimalEncoder)
}
