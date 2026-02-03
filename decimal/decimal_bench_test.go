package decimal

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Minimal benchmark without dependencies.
func BenchmarkMinimal(b *testing.B) {
	dec := MustMakeDecimal("123.45")

	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dec.String()
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = dec.Decimal.String()
		}
	})
}

// Benchmark for small numbers (optimized conversion path).
func BenchmarkDecimalString_SmallNumbers(b *testing.B) {
	smallNumbers := []string{
		"123.45",
		"-123.45",
		"0.00123",
		"100.00",
		"999.99",
		"42",
		"-42",
		"0.000001",
		"1234567.89",
		"-987654.32",
	}

	decimals := make([]Decimal, len(smallNumbers))
	for i, str := range smallNumbers {
		decimals[i] = MustMakeDecimal(str)
	}

	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.String()
			}
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.Decimal.String()
			}
		}
	})
}

// A benchmark for the boundary cases of int64.
func BenchmarkDecimalString_Int64Boundaries(b *testing.B) {
	boundaryNumbers := []string{
		"9223372036854775807",  // max int64
		"-9223372036854775808", // min int64
		"9223372036854775806",
		"-9223372036854775807",
	}

	decimals := make([]Decimal, len(boundaryNumbers))
	for i, str := range boundaryNumbers {
		decimals[i] = MustMakeDecimal(str)
	}

	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.String()
			}
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.Decimal.String()
			}
		}
	})
}

// Benchmark for large numbers (fallback path).
func BenchmarkDecimalString_LargeNumbers(b *testing.B) {
	largeNumbers := []string{
		"123456789012345678901234567890.123456789",
		"-123456789012345678901234567890.123456789",
		"99999999999999999999999999999999999999",
		"-99999999999999999999999999999999999999",
	}

	decimals := make([]Decimal, len(largeNumbers))
	for i, str := range largeNumbers {
		decimals[i] = MustMakeDecimal(str)
	}

	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.String()
			}
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.Decimal.String()
			}
		}
	})
}

// A benchmark for mixed numbers (real-world scenarios).
func BenchmarkDecimalString_Mixed(b *testing.B) {
	mixedNumbers := []string{
		"0",
		"1",
		"-1",
		"0.5",
		"-0.5",
		"123.456",
		"1000000.000001",
		"9223372036854775807",
		"123456789012345678901234567890.123456789",
	}

	decimals := make([]Decimal, len(mixedNumbers))
	for i, str := range mixedNumbers {
		decimals[i] = MustMakeDecimal(str)
	}

	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.String()
			}
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.Decimal.String()
			}
		}
	})
}

// A benchmark for numbers with different precision.
func BenchmarkDecimalString_DifferentPrecision(b *testing.B) {
	testCases := []struct {
		name  string
		value string
	}{
		{"Integer", "1234567890"},
		{"SmallDecimal", "0.000000001"},
		{"MediumDecimal", "123.456789"},
		{"LargeDecimal", "1234567890.123456789"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			dec := MustMakeDecimal(tc.value)

			b.Run("String", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = dec.String()
				}
			})

			b.Run("StandardString", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					_ = dec.Decimal.String()
				}
			})
		})
	}
}

// A benchmark with random numbers for statistical significance.
func BenchmarkDecimalString_Random(b *testing.B) {
	// Create a local random number generator
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Generate random numbers in int64 range
	generateRandomDecimal := func() Decimal {
		// 70% chance for small numbers, 30% for large numbers
		if rng.Float64() < 0.7 {
			// Numbers in int64 range
			value := rng.Int63n(1000000000) - 500000000
			scale := rng.Intn(10)

			if scale == 0 {
				return MustMakeDecimal(strconv.FormatInt(value, 10))
			}

			// For numbers with fractional part
			str := strconv.FormatInt(value, 10)
			if value < 0 {
				str = str[1:] // remove minus sign
			}

			if len(str) > scale {
				integerPart := str[:len(str)-scale]
				fractionalPart := str[len(str)-scale:]
				result := integerPart + "." + fractionalPart
				if value < 0 {
					result = "-" + result
				}
				return MustMakeDecimal(result)
			} else {
				zeros := scale - len(str)
				result := "0." + strings.Repeat("0", zeros) + str
				if value < 0 {
					result = "-" + result
				}
				return MustMakeDecimal(result)
			}
		} else {
			// Large numbers (fallback) - generate correct strings
			// Generate 30-digit number
			bigDigits := make([]byte, 30)
			for i := range bigDigits {
				bigDigits[i] = byte(rng.Intn(10) + '0')
			}
			// Remove leading zeros
			for len(bigDigits) > 1 && bigDigits[0] == '0' {
				bigDigits = bigDigits[1:]
			}

			bigNum := string(bigDigits)
			scale := rng.Intn(10)

			if scale == 0 {
				if rng.Float64() < 0.5 {
					bigNum = "-" + bigNum
				}
				return MustMakeDecimal(bigNum)
			}

			if scale < len(bigNum) {
				integerPart := bigNum[:len(bigNum)-scale]
				fractionalPart := bigNum[len(bigNum)-scale:]
				result := integerPart + "." + fractionalPart
				if rng.Float64() < 0.5 {
					result = "-" + result
				}
				return MustMakeDecimal(result)
			} else {
				zeros := scale - len(bigNum)
				result := "0." + strings.Repeat("0", zeros) + bigNum
				if rng.Float64() < 0.5 {
					result = "-" + result
				}
				return MustMakeDecimal(result)
			}
		}
	}

	b.ResetTimer()
	b.Run("String", func(b *testing.B) {
		total := 0
		for i := 0; i < b.N; i++ {
			dec := generateRandomDecimal()
			result := dec.String()
			total += len(result)
		}
		_ = total
	})

	b.Run("StandardString", func(b *testing.B) {
		total := 0
		for i := 0; i < b.N; i++ {
			dec := generateRandomDecimal()
			result := dec.Decimal.String()
			total += len(result)
		}
		_ = total
	})
}

// A benchmark for checking memory allocations.
func BenchmarkDecimalString_MemoryAllocations(b *testing.B) {
	testNumbers := []string{
		"123.45",
		"0.001",
		"9223372036854775807",
		"123456789012345678901234567890.123456789",
	}

	decimals := make([]Decimal, len(testNumbers))
	for i, str := range testNumbers {
		decimals[i] = MustMakeDecimal(str)
	}

	b.ResetTimer()

	b.Run("String", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.String()
			}
		}
	})

	b.Run("StandardString", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, dec := range decimals {
				_ = dec.Decimal.String()
			}
		}
	})
}
