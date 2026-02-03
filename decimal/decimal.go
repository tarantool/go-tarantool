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
	strBuf := d.Decimal.String()
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
// String - optimized version for Tarantool Decimal
// taking into account the limitations of int64 and support for large numbers via fallback
// Tarantool decimal has 38 digits, which can exceed int64.
// Therefore, we cannot use int64 for all cases.
// For the general case, use shopspring/decimal.String().
// For cases where it is known that numbers contain less than 26 characters,
// you can use the optimized version.
func (d Decimal) String() string {
	coefficient := d.Decimal.Coefficient() // Note: In shopspring/decimal
	// the number is stored as coefficient *10^exponent,  where exponent can be negative.
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
// and scale to a string (for numbers up to 19 digits).
func (d Decimal) stringFromInt64(value int64, scale int) string {
	var buf [64]byte
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

	// Special case: zero value.
	if value == 0 {
		return "0" // Always return "0" regardless of scale.
	}

	if scale == 0 {
		// No fractional part.
		if pos+length > len(buf) {
			return d.Decimal.String()
		}
		copy(buf[pos:], str)
		pos += length
		return string(buf[:pos])
	}

	if scale >= length {
		// Numbers like 0.00123.
		// Count trailing zeros in the fractional part.
		trailingZeros := 0
		// In this case, the fractional part consists
		//  of (scale-length) zeros followed by the number.
		// We need to count trailing zeros in the actual number part.
		for i := length - 1; i >= 0 && str[i] == '0'; i-- {
			trailingZeros++
		}

		effectiveDigits := length - trailingZeros

		// If all digits are zeros after leading zeros, we need to adjust.
		if effectiveDigits == 0 {
			return "0"
		}

		required := 2 + (scale - length) + effectiveDigits
		if pos+required > len(buf) {
			return d.Decimal.String()
		}

		buf[pos] = '0'
		buf[pos+1] = '.'
		pos += 2

		// Add leading zeros.
		zeros := scale - length
		for i := 0; i < zeros; i++ {
			buf[pos] = '0'
			pos++
		}

		// Copy only significant digits (without trailing zeros).
		copy(buf[pos:], str[:effectiveDigits])
		pos += effectiveDigits
	} else {
		// Numbers like 123.45.
		integerLen := length - scale

		// Count trailing zeros in fractional part.
		trailingZeros := 0
		for i := length - 1; i >= integerLen && str[i] == '0'; i-- {
			trailingZeros++
		}

		effectiveScale := scale - trailingZeros

		// If all fractional digits are zeros, return just integer part.
		if effectiveScale == 0 {
			if pos+integerLen > len(buf) {
				return d.Decimal.String()
			}
			copy(buf[pos:], str[:integerLen])
			pos += integerLen
			return string(buf[:pos])
		}

		required := integerLen + 1 + effectiveScale
		if pos+required > len(buf) {
			return d.Decimal.String()
		}

		// Integer part.
		copy(buf[pos:], str[:integerLen])
		pos += integerLen

		// Decimal point.
		buf[pos] = '.'
		pos++

		// Fractional part without trailing zeros.
		fractionalEnd := integerLen + effectiveScale
		copy(buf[pos:], str[integerLen:fractionalEnd])
		pos += effectiveScale
	}

	return string(buf[:pos])
}
func (d Decimal) handleMinInt64(scale int) string {
	const minInt64Str = "9223372036854775808"

	var buf [64]byte
	pos := 0

	buf[pos] = '-'
	pos++

	length := len(minInt64Str)

	if scale == 0 {
		if pos+length > len(buf) {
			return "-" + minInt64Str
		}
		copy(buf[pos:], minInt64Str)
		pos += length
		return string(buf[:pos])
	}

	if scale >= length {
		// Count trailing zeros in the actual number part.
		trailingZeros := 0
		for i := length - 1; i >= 0 && minInt64Str[i] == '0'; i-- {
			trailingZeros++
		}

		effectiveDigits := length - trailingZeros

		if effectiveDigits == 0 {
			return "0"
		}

		required := 2 + (scale - length) + effectiveDigits
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

		copy(buf[pos:], minInt64Str[:effectiveDigits])
		pos += effectiveDigits
	} else {
		integerLen := length - scale

		// Count trailing zeros for minInt64Str fractional part.
		trailingZeros := 0
		for i := length - 1; i >= integerLen && minInt64Str[i] == '0'; i-- {
			trailingZeros++
		}

		effectiveScale := scale - trailingZeros

		if effectiveScale == 0 {
			if pos+integerLen > len(buf) {
				return d.Decimal.String()
			}
			copy(buf[pos:], minInt64Str[:integerLen])
			pos += integerLen
			return string(buf[:pos])
		}

		required := integerLen + 1 + effectiveScale
		if pos+required > len(buf) {
			return d.Decimal.String()
		}

		copy(buf[pos:], minInt64Str[:integerLen])
		pos += integerLen

		buf[pos] = '.'
		pos++

		fractionalEnd := integerLen + effectiveScale
		copy(buf[pos:], minInt64Str[integerLen:fractionalEnd])
		pos += effectiveScale
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
