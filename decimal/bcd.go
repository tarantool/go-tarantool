// Package decimal implements methods to encode and decode BCD.
//
// BCD (Binary-Coded Decimal) is a sequence of bytes representing decimal
// digits of the encoded number (each byte has two decimal digits each encoded
// using 4-bit nibbles), so byte >> 4 is the first digit and byte & 0x0f is the
// second digit. The leftmost digit in the array is the most significant. The
// rightmost digit in the array is the least significant.
//
// The first byte of the BCD array contains the first digit of the number,
// represented as follows:
//
//     |  4 bits           |  4 bits           |
//        = 0x                = the 1st digit
//
// (The first nibble contains 0 if the decimal number has an even number of
// digits). The last byte of the BCD array contains the last digit of the
// number and the final nibble, represented as follows:
//
//     |  4 bits           |  4 bits           |
//        = the last digit    = nibble
//
// The final nibble represents the number's sign: 0x0a, 0x0c, 0x0e, 0x0f stand
// for plus, 0x0b and 0x0d stand for minus.
//
// Examples:
//
// The decimal -12.34 will be encoded as 0xd6, 0x01, 0x02, 0x01, 0x23, 0x4d:
//
// | MP_EXT (fixext 4) | MP_DECIMAL | scale |  1   |  2,3 |  4 (minus) |
// |        0xd6       |    0x01    | 0x02  | 0x01 | 0x23 | 0x4d       |
//
// The decimal 0.000000000000000000000000000000000010 will be encoded as
// 0xc7, 0x03, 0x01, 0x24, 0x01, 0x0c:
//
// | MP_EXT (ext 8) | length | MP_DECIMAL | scale |  1   | 0 (plus) |
// |      0xc7      |  0x03  |    0x01    | 0x24  | 0x01 | 0x0c     |
//
// See also:
//
// * MessagePack extensions https://www.tarantool.io/en/doc/latest/dev_guide/internals/msgpack_extensions/
//
// * An implementation in C language https://github.com/tarantool/decNumber/blob/master/decPacked.c
package decimal

import (
	"fmt"
	"strings"
)

const (
	bytePlus  = byte(0x0c)
	byteMinus = byte(0x0d)
)

var isNegative = [256]bool{
	0x0a: false,
	0x0b: true,
	0x0c: false,
	0x0d: true,
	0x0e: false,
	0x0f: false,
}

// Calculate a number of digits in a buffer with decimal number.
//
// Plus, minus, point and leading zeroes do not count.
// Contains a quirk for a zero - returns 1.
//
// Examples (see more examples in tests):
//
// - 0.0000000000000001 - 1 digit
//
// - 00012.34           - 4 digits
//
// - 0.340              - 3 digits
//
// - 0                  - 1 digit
func getNumberLength(buf string) int {
	if len(buf) == 0 {
		return 0
	}
	n := 0
	for _, ch := range []byte(buf) {
		if ch >= '1' && ch <= '9' {
			n += 1
		} else if ch == '0' && n != 0 {
			n += 1
		}
	}

	// Fix a case with a single 0.
	if n == 0 {
		n = 1
	}

	return n
}

// encodeStringToBCD converts a string buffer to BCD Packed Decimal.
//
// The number is converted to a BCD packed decimal byte array, right aligned in
// the BCD array, whose length is indicated by the second parameter. The final
// 4-bit nibble in the array will be a sign nibble, 0x0c for "+" and 0x0d for
// "-". Unused bytes and nibbles to the left of the number are set to 0. scale
// is set to the scale of the number (this is the exponent, negated).
func encodeStringToBCD(buf string) ([]byte, error) {
	if len(buf) == 0 {
		return nil, fmt.Errorf("Length of number is zero")
	}
	signByte := bytePlus // By default number is positive.
	if buf[0] == '-' {
		signByte = byteMinus
	}

	// The first nibble should contain 0, if the decimal number has an even
	// number of digits. Therefore highNibble is false when decimal number
	// is even.
	highNibble := true
	l := GetNumberLength(buf)
	if l%2 == 0 {
		highNibble = false
	}
	scale := 0 // By default decimal number is integer.
	var byteBuf []byte
	for i, ch := range []byte(buf) {
		// Skip leading zeroes.
		if (len(byteBuf) == 0) && ch == '0' {
			continue
		}
		if (i == 0) && (ch == '-' || ch == '+') {
			continue
		}
		// Calculate a number of digits after the decimal point.
		if ch == '.' {
			if scale != 0 {
				return nil, fmt.Errorf("Number contains more than one point")
			}
			scale = len(buf) - i - 1
			continue
		}

		if ch < '0' || ch > '9' {
			return nil, fmt.Errorf("Failed to convert symbol '%c' to a digit", ch)
		}
		digit := byte(ch - '0')
		if highNibble {
			// Add a digit to a high nibble.
			digit = digit << 4
			byteBuf = append(byteBuf, digit)
			highNibble = false
		} else {
			if len(byteBuf) == 0 {
				byteBuf = make([]byte, 1)
			}
			// Add a digit to a low nibble.
			lowByteIdx := len(byteBuf) - 1
			byteBuf[lowByteIdx] = byteBuf[lowByteIdx] | digit
			highNibble = true
		}
	}
	if len(byteBuf) == 0 {
		// a special case: -0
		signByte = bytePlus
	}
	if highNibble {
		// Put a sign to a high nibble.
		byteBuf = append(byteBuf, signByte)
	} else {
		// Put a sign to a low nibble.
		lowByteIdx := len(byteBuf) - 1
		byteBuf[lowByteIdx] = byteBuf[lowByteIdx] | signByte
	}
	byteBuf = append([]byte{byte(scale)}, byteBuf...)

	return byteBuf, nil
}

// decodeStringFromBCD converts a BCD Packed Decimal to a string buffer.
//
// The BCD packed decimal byte array, together with an associated scale, is
// converted to a string. The BCD array is assumed full of digits, and must be
// ended by a 4-bit sign nibble in the least significant four bits of the final
// byte. The scale is used (negated) as the exponent of the decimal number.
// Note that zeroes may have a sign and/or a scale.
func decodeStringFromBCD(bcdBuf []byte) (string, error) {
	// Index of a byte with scale.
	const scaleIdx = 0
	scale := int(bcdBuf[scaleIdx])

	// Get a BCD buffer without scale.
	bcdBuf = bcdBuf[scaleIdx+1:]
	bufLen := len(bcdBuf)

	// Every nibble contains a digit, and the last low nibble contains a
	// sign.
	ndigits := bufLen*2 - 1

	// The first nibble contains 0 if the decimal number has an even number of
	// digits. Decrease a number of digits if so.
	if bcdBuf[0]&0xf0 == 0 {
		ndigits -= 1
	}

	// Reserve bytes for dot and sign.
	numLen := ndigits + 2
	// Reserve bytes for zeroes.
	if scale >= ndigits {
		numLen += scale - ndigits
	}

	var bld strings.Builder
	bld.Grow(numLen)

	// Add a sign, it is encoded in a low nibble of a last byte.
	lastByte := bcdBuf[bufLen-1]
	sign := lastByte & 0x0f
	if isNegative[sign] {
		bld.WriteByte('-')
	}

	// Add missing zeroes to the left side when scale is bigger than a
	// number of digits and a single missed zero to the right side when
	// equal.
	if scale > ndigits {
		bld.WriteByte('0')
		bld.WriteByte('.')
		for diff := scale - ndigits; diff > 0; diff-- {
			bld.WriteByte('0')
		}
	} else if scale == ndigits {
		bld.WriteByte('0')
	}

	const MaxDigit = 0x09
	// Builds a buffer with symbols of decimal number (digits, dot and sign).
	processNibble := func(nibble byte) {
		if nibble <= MaxDigit {
			if ndigits == scale {
				bld.WriteByte('.')
			}
			bld.WriteByte(nibble + '0')
			ndigits--
		}
	}

	for i, bcdByte := range bcdBuf {
		highNibble := bcdByte >> 4
		lowNibble := bcdByte & 0x0f
		// Skip a first high nibble as no digit there.
		if i != 0 || highNibble != 0 {
			processNibble(highNibble)
		}
		processNibble(lowNibble)
	}

	return bld.String(), nil
}
