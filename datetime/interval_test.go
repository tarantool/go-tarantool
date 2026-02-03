package datetime_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/tarantool/go-tarantool/v3"
	. "github.com/tarantool/go-tarantool/v3/datetime"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

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

	if !reflect.DeepEqual(ival, expected) {
		t.Fatalf("Unexpected %v, expected %v", ival, expected)
	}
	if !reflect.DeepEqual(cpyOrig, orig) {
		t.Fatalf("Original value changed %v, expected %v", orig, cpyOrig)
	}
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

	if !reflect.DeepEqual(ival, expected) {
		t.Fatalf("Unexpected %v, expected %v", ival, expected)
	}
	if !reflect.DeepEqual(cpyOrig, orig) {
		t.Fatalf("Original value changed %v, expected %v", orig, cpyOrig)
	}
}

func TestIntervalTarantoolEncoding(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

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
			if err != nil {
				t.Fatalf("Unexpected error: %s", err.Error())
			}

			ret := data[0].(Interval)
			if !reflect.DeepEqual(ret, tc) {
				t.Fatalf("Unexpected response: %v, expected %v", ret, tc)
			}
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
			name: "single component - years",
			interval: Interval{
				Year: 1,
			},
			expected: "1 year",
		},
		{
			name: "multiple years",
			interval: Interval{
				Year: 5,
			},
			expected: "5 years",
		},
		{
			name: "multiple components",
			interval: Interval{
				Year:  1,
				Month: 2,
				Day:   3,
			},
			expected: "1 year, 2 months and 3 days",
		},
		{
			name: "time components",
			interval: Interval{
				Hour: 1,
				Min:  30,
				Sec:  45,
			},
			expected: "1 hour, 30 minutes and 45 seconds",
		},
		{
			name: "seconds with nanoseconds same sign",
			interval: Interval{
				Sec:  5,
				Nsec: 123456789,
			},
			expected: "5.123456789 seconds",
		},
		{
			name: "negative seconds with nanoseconds",
			interval: Interval{
				Sec:  -5,
				Nsec: -123456789,
			},
			expected: "-5.123456789 seconds",
		},
		{
			name: "seconds and nanoseconds different signs",
			interval: Interval{
				Sec:  5,
				Nsec: -123456789,
			},
			expected: "5 seconds and -123456789 nanoseconds",
		},
		{
			name: "only nanoseconds",
			interval: Interval{
				Nsec: 500000000,
			},
			expected: "500000000 nanoseconds",
		},
		{
			name: "weeks",
			interval: Interval{
				Week: 2,
			},
			expected: "2 weeks",
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
			expected: "1 year, 6 months, 2 weeks, 3 days, 12 hours, 30 minutes " +
				"and 45.123456789 seconds",
		},
		{
			name: "negative components",
			interval: Interval{
				Year: -1,
				Day:  -2,
				Hour: -3,
			},
			expected: "-1 year, -2 days and -3 hours",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.interval.String()
			if result != tt.expected {
				t.Errorf("Interval.String() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestIntervalStringIntegration(t *testing.T) {
	t.Run("implements Stringer", func(t *testing.T) {
		var _ fmt.Stringer = Interval{}
	})

	t.Run("works with fmt package", func(t *testing.T) {
		ival := Interval{Hour: 2, Min: 30}
		result := ival.String()
		expected := "2 hours and 30 minutes"
		if result != expected {
			t.Errorf("fmt.Sprintf('%%s') = %v, want %v", result, expected)
		}

		result = fmt.Sprintf("%v", ival)
		if result != expected {
			t.Errorf("fmt.Sprintf('%%v') = %v, want %v", result, expected)
		}
	})
}

func TestIntervalStringEdgeCases(t *testing.T) {
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
			if result == "" {
				t.Error("Interval.String() returned empty string")
			}
			if len(result) > 1000 { // Разумный лимит
				t.Error("Interval.String() returned unexpectedly long string")
			}
		})
	}
}
