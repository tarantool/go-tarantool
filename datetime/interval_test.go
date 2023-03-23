package datetime_test

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/ice-blockchain/go-tarantool/datetime"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
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

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
			resp, err := conn.Call17("call_interval_testdata", []interface{}{tc})
			if err != nil {
				t.Fatalf("Unexpected error: %s", err.Error())
			}

			ret := resp.Data[0].(Interval)
			if !reflect.DeepEqual(ret, tc) {
				t.Fatalf("Unexpected response: %v, expected %v", ret, tc)
			}
		})
	}
}
