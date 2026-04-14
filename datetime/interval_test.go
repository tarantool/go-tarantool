package datetime_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v3"
	. "github.com/tarantool/go-tarantool/v3/datetime"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

var _ fmt.Stringer = Interval{}

func TestIntervalAdd(t *testing.T) {
	orig := Interval{
		Year:   1,
		Month:  2,
		Week:   3,
		Day:    4,
		Hour:   -5,
		Min:    6,
		Sec:    -7,
		Nsec:   8,
		Adjust: LastAdjust,
	}
	cpyOrig := orig
	add := Interval{
		Year:   2,
		Month:  3,
		Week:   -4,
		Day:    5,
		Hour:   -6,
		Min:    7,
		Sec:    -8,
		Nsec:   0,
		Adjust: ExcessAdjust,
	}
	expected := Interval{
		Year:   orig.Year + add.Year,
		Month:  orig.Month + add.Month,
		Week:   orig.Week + add.Week,
		Day:    orig.Day + add.Day,
		Hour:   orig.Hour + add.Hour,
		Min:    orig.Min + add.Min,
		Sec:    orig.Sec + add.Sec,
		Nsec:   orig.Nsec + add.Nsec,
		Adjust: orig.Adjust,
	}

	ival := orig.Add(add)

	require.Equal(t, expected, ival, "Unexpected interval result")
	require.Equal(t, cpyOrig, orig, "Original value changed")
}

func TestIntervalSub(t *testing.T) {
	orig := Interval{
		Year:   1,
		Month:  2,
		Week:   3,
		Day:    4,
		Hour:   -5,
		Min:    6,
		Sec:    -7,
		Nsec:   8,
		Adjust: LastAdjust,
	}
	cpyOrig := orig
	sub := Interval{
		Year:   2,
		Month:  3,
		Week:   -4,
		Day:    5,
		Hour:   -6,
		Min:    7,
		Sec:    -8,
		Nsec:   0,
		Adjust: ExcessAdjust,
	}
	expected := Interval{
		Year:   orig.Year - sub.Year,
		Month:  orig.Month - sub.Month,
		Week:   orig.Week - sub.Week,
		Day:    orig.Day - sub.Day,
		Hour:   orig.Hour - sub.Hour,
		Min:    orig.Min - sub.Min,
		Sec:    orig.Sec - sub.Sec,
		Nsec:   orig.Nsec - sub.Nsec,
		Adjust: orig.Adjust,
	}

	ival := orig.Sub(sub)

	require.Equal(t, expected, ival, "Unexpected interval result")
	require.Equal(t, cpyOrig, orig, "Original value changed")
}

func TestIntervalTarantoolEncoding(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	cases := []Interval{
		{},
		{1, 2, 3, 4, -5, 6, -7, 8, LastAdjust},
		{1, 2, 3, 4, -5, 6, -7, 8, ExcessAdjust},
		{1, 2, 3, 4, -5, 6, -7, 8, LastAdjust},
		{0, 2, 3, 4, -5, 0, -7, 8, LastAdjust},
		{0, 0, 3, 0, -5, 6, -7, 8, ExcessAdjust},
		{0, 0, 0, 4, 0, 0, 0, 8, LastAdjust},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%v", tc), func(t *testing.T) {
			req := tarantool.NewCallRequest("call_interval_testdata").
				Args([]interface{}{tc})
			data, err := conn.Do(req).Get()
			require.NoError(t, err, "Unexpected error")

			ret := data[0].(Interval)
			assert.Equal(t, tc, ret, "Unexpected response")
		})
	}
}

func TestIntervalString(t *testing.T) {
	tests := []struct {
		name     string
		interval Interval
		expected string
	}{
		{
			name:     "empty interval",
			interval: Interval{},
			expected: "0 seconds",
		},
		{
			name: "single positive year",
			interval: Interval{
				Year: 1,
			},
			expected: "+1 year",
		},
		{
			name: "single negative year",
			interval: Interval{
				Year: -1,
			},
			expected: "-1 year",
		},
		{
			name: "multiple positive years",
			interval: Interval{
				Year: 5,
			},
			expected: "+5 years",
		},
		{
			name: "multiple positive components",
			interval: Interval{
				Year:  1,
				Month: 2,
				Day:   3,
			},
			expected: "+1 year, 2 months, 3 days",
		},
		{
			name: "positive components without sign after first",
			interval: Interval{
				Hour: 1,
				Min:  30,
				Sec:  45,
			},
			expected: "+1 hour, 30 minutes, 45 seconds",
		},
		{
			name: "mixed signs",
			interval: Interval{
				Year:  -1,
				Month: 2,
				Week:  -3,
				Day:   4,
				Hour:  -5,
				Min:   6,
				Sec:   -7,
				Nsec:  9,
			},
			expected: "-1 year, 2 months, -3 weeks, 4 days, -5 hours, 6 minutes," +
				" -7 seconds, 9 nanoseconds",
		},
		{
			name: "positive seconds with nanoseconds",
			interval: Interval{
				Sec:  5,
				Nsec: 123456789,
			},
			expected: "+5 seconds, 123456789 nanoseconds",
		},
		{
			name: "negative seconds with nanoseconds",
			interval: Interval{
				Sec:  -5,
				Nsec: -123456789,
			},
			expected: "-5 seconds, -123456789 nanoseconds",
		},
		{
			name: "only positive nanoseconds",
			interval: Interval{
				Nsec: 500000000,
			},
			expected: "+500000000 nanoseconds",
		},
		{
			name: "only negative nanoseconds",
			interval: Interval{
				Nsec: -500000000,
			},
			expected: "-500000000 nanoseconds",
		},
		{
			name: "complex interval",
			interval: Interval{
				Year:  1,
				Month: 6,
				Week:  2,
				Day:   3,
				Hour:  12,
				Min:   30,
				Sec:   45,
				Nsec:  123456789,
			},
			expected: "+1 year, 6 months, 2 weeks, 3 days, 12 hours, 30 minutes," +
				" 45 seconds, 123456789 nanoseconds",
		},
		{
			name: "all negative components",
			interval: Interval{
				Year: -1,
				Day:  -2,
				Hour: -3,
			},
			expected: "-1 year, -2 days, -3 hours",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.String()
			assert.Equal(t, tt.expected, result, "Interval.String()")
		})
	}
}
func TestIntervalString_WroksWithFmt(t *testing.T) {
	ival := Interval{Hour: 2, Min: 30}
	result := ival.String()
	expected := "+2 hours, 30 minutes"
	assert.Equal(t, expected, result, "fmt.Sprintf('%s')")

	result = fmt.Sprintf("%v", ival)
	assert.Equal(t, expected, result, "fmt.Sprintf('%v')")
}

func TestIntervalString_FromTarantool(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	testCases := []struct {
		luaExpr  string
		expected string
	}{
		{
			"return require('datetime').interval.new({})",
			"0 seconds",
		},
		{
			"return require('datetime').interval.new({year = 1})",
			"+1 year",
		},
		{
			"return require('datetime').interval.new({year = -1})",
			"-1 year",
		},
		{
			"return require('datetime').interval.new({year = 5})",
			"+5 years",
		},
		{
			"return require('datetime').interval.new({year = 1, month = 2, day = 3})",
			"+1 year, 2 months, 3 days",
		},
		{
			"return require('datetime').interval.new({hour = 1, min = 30, sec = 45})",
			"+1 hour, 30 minutes, 45 seconds",
		},
		{
			"return require('datetime').interval.new({year = -1, month = 2, week = -3, day = 4," +
				" hour = -5, min = 6, sec = -7, nsec = 9})",
			"-1 year, 2 months, -3 weeks, 4 days, -5 hours, 6 minutes, -7 seconds, 9 nanoseconds",
		},
		{
			"return require('datetime').interval.new({sec = 5, nsec = 123456789})",
			"+5 seconds, 123456789 nanoseconds",
		},
		{
			"return require('datetime').interval.new({sec = -5, nsec = -123456789})",
			"-5 seconds, -123456789 nanoseconds",
		},
		{
			"return require('datetime').interval.new({nsec = 500000000})",
			"+500000000 nanoseconds",
		},
		{
			"return require('datetime').interval.new({nsec = -500000000})",
			"-500000000 nanoseconds",
		},
		{
			"return require('datetime').interval.new({year = 1, month = 6, week = 2, day = 3," +
				" hour = 12, min = 30, sec = 45, nsec = 123456789})",
			"+1 year, 6 months, 2 weeks, 3 days, 12 hours, 30 minutes, 45 seconds," +
				" 123456789 nanoseconds",
		},
		{
			"return require('datetime').interval.new({year = -1, day = -2, hour = -3})",
			"-1 year, -2 days, -3 hours",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			data, err := conn.Do(tarantool.NewEvalRequest(tc.luaExpr)).Get()
			require.NoError(t, err, "Eval failed")
			require.Len(t, data, 1, "Expected 1 result")
			ival, ok := data[0].(Interval)
			require.True(t, ok, "Result is not Interval")
			assert.Equal(t, tc.expected, ival.String(), "String()")
		})
	}
}

func TestIntervalString_EdgeCases(t *testing.T) {
	tests := []struct {
		name     string
		interval Interval
	}{
		{
			name:     "max values",
			interval: Interval{Year: 1<<63 - 1, Month: 1<<63 - 1},
		},
		{
			name:     "min values",
			interval: Interval{Year: -1 << 63, Month: -1 << 63},
		},
		{
			name:     "mixed signs complex",
			interval: Interval{Year: 1, Month: -1, Day: 1, Hour: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.String()
			assert.NotEmpty(t, result, "Interval.String() returned empty string")
			assert.LessOrEqual(t, len(result), 1000, "Interval.String() returned unexpectedly long string")
		})
	}
}
