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

type Decimal struct {
	decimal.Decimal
}

// NewDecimal creates a new Decimal from a decimal.Decimal.
func NewDecimal(decimal decimal.Decimal) *Decimal {
	return &Decimal{Decimal: decimal}
}

// NewDecimalFromString creates a new Decimal from a string.
func NewDecimalFromString(src string) (result *Decimal, err error) {
	dec, err := decimal.NewFromString(src)
	if err != nil {
		return
	}
	result = NewDecimal(dec)
	return
}

// MarshalMsgpack serializes the Decimal into a MessagePack representation.
func (decNum *Decimal) MarshalMsgpack() ([]byte, error) {
	one := decimal.NewFromInt(1)
	maxSupportedDecimal := decimal.New(1, decimalPrecision).Sub(one) // 10^decimalPrecision - 1
	minSupportedDecimal := maxSupportedDecimal.Neg().Sub(one)        // -10^decimalPrecision - 1
	if decNum.GreaterThan(maxSupportedDecimal) {
		return nil,
			fmt.Errorf(
				"msgpack: decimal number is bigger than maximum supported number (10^%d - 1)",
				decimalPrecision)
	}
	if decNum.LessThan(minSupportedDecimal) {
		return nil,
			fmt.Errorf(
				"msgpack: decimal number is lesser than minimum supported number (-10^%d - 1)",
				decimalPrecision)
	}

	strBuf := decNum.String()
	bcdBuf, err := encodeStringToBCD(strBuf)
	if err != nil {
		return nil, fmt.Errorf("msgpack: can't encode string (%s) to a BCD buffer: %w", strBuf, err)
	}
	return bcdBuf, nil
}

// UnmarshalMsgpack deserializes a Decimal value from a MessagePack
// representation.
func (decNum *Decimal) UnmarshalMsgpack(b []byte) error {
	// Decimal values can be encoded to fixext MessagePack, where buffer
	// has a fixed length encoded by first byte, and ext MessagePack, where
	// buffer length is not fixed and encoded by a number in a separate
	// field:
	//
	//  +--------+-------------------+------------+===============+
	//  | MP_EXT | length (optional) | MP_DECIMAL | PackedDecimal |
	//  +--------+-------------------+------------+===============+
	digits, err := decodeStringFromBCD(b)
	if err != nil {
		return fmt.Errorf("msgpack: can't decode string from BCD buffer (%x): %w", b, err)
	}
	dec, err := decimal.NewFromString(digits)
	*decNum = *NewDecimal(dec)
	if err != nil {
		return fmt.Errorf("msgpack: can't encode string (%s) to a decimal number: %w", digits, err)
	}

	return nil
}

func init() {
	msgpack.RegisterExt(decimalExtID, (*Decimal)(nil))
}
