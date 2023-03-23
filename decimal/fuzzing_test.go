//go:build go_tarantool_decimal_fuzzing
// +build go_tarantool_decimal_fuzzing

package decimal_test

import (
	"testing"

	"github.com/shopspring/decimal"

	. "github.com/ice-blockchain/go-tarantool/decimal"
)

func strToDecimal(t *testing.T, buf string) decimal.Decimal {
	decNum, err := decimal.NewFromString(buf)
	if err != nil {
		t.Fatal(err)
	}
	return decNum
}

func FuzzEncodeDecodeBCD(f *testing.F) {
	samples := append(correctnessSamples, benchmarkSamples...)
	for _, testcase := range samples {
		if len(testcase.numString) > 0 {
			f.Add(testcase.numString) // Use f.Add to provide a seed corpus.
		}
	}
	f.Fuzz(func(t *testing.T, orig string) {
		if l := GetNumberLength(orig); l > DecimalPrecision {
			t.Skip("max number length is exceeded")
		}
		bcdBuf, err := EncodeStringToBCD(orig)
		if err != nil {
			t.Skip("Only correct requests are interesting: %w", err)
		}
		var dec string
		dec, err = DecodeStringFromBCD(bcdBuf)
		if err != nil {
			t.Fatalf("Failed to decode encoded value ('%s')", orig)
		}

		if !strToDecimal(t, dec).Equal(strToDecimal(t, orig)) {
			t.Fatal("Decimal numbers are not equal")
		}
	})
}
