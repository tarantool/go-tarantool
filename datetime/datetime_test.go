package datetime_test

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/datetime"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

var noTimezoneLoc = time.FixedZone(NoTimezone, 0)

var lesserBoundaryTimes = []time.Time{
	time.Date(-5879610, 06, 22, 0, 0, 1, 0, time.UTC),
	time.Date(-5879610, 06, 22, 0, 0, 0, 1, time.UTC),
	time.Date(5879611, 07, 10, 23, 59, 59, 0, time.UTC),
	time.Date(5879611, 07, 10, 23, 59, 59, 999999999, time.UTC),
}

var boundaryTimes = []time.Time{
	time.Date(-5879610, 06, 22, 0, 0, 0, 0, time.UTC),
	time.Date(5879611, 07, 11, 0, 0, 0, 999999999, time.UTC),
}

var greaterBoundaryTimes = []time.Time{
	time.Date(-5879610, 06, 21, 23, 59, 59, 999999999, time.UTC),
	time.Date(5879611, 07, 11, 0, 0, 1, 0, time.UTC),
}

var isDatetimeSupported = false

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var spaceTuple1 = "testDatetime_1"
var spaceTuple2 = "testDatetime_2"
var index = "primary"

func skipIfDatetimeUnsupported(t *testing.T) {
	t.Helper()

	if isDatetimeSupported == false {
		t.Skip("Skipping test for Tarantool without datetime support in msgpack")
	}
}

func TestDatetimeAdd(t *testing.T) {
	tm := time.Unix(0, 0).UTC()
	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	newdt, err := dt.Add(Interval{
		Year:  1,
		Month: -3,
		Week:  3,
		Day:   4,
		Hour:  -5,
		Min:   5,
		Sec:   6,
		Nsec:  -3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	expected := "1970-10-25 19:05:05.999999997 +0000 UTC"
	if newdt.ToTime().String() != expected {
		t.Fatalf("Unexpected result: %s, expected: %s", newdt.ToTime().String(), expected)
	}
}

func TestDatetimeAddAdjust(t *testing.T) {
	/*
	 How-to test in Tarantool:
	 > date = require("datetime")
	 > date.parse("2012-12-31T00:00:00Z") + {month = -1, adjust = "excess"}
	*/
	cases := []struct {
		year   int64
		month  int64
		adjust Adjust
		date   string
		want   string
	}{
		{
			year:   0,
			month:  1,
			adjust: NoneAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-03-28T00:00:00Z",
		},
		{
			year:   0,
			month:  1,
			adjust: LastAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-03-31T00:00:00Z",
		},
		{
			year:   0,
			month:  1,
			adjust: ExcessAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-03-28T00:00:00Z",
		},
		{
			year:   0,
			month:  1,
			adjust: NoneAdjust,
			date:   "2013-01-31T00:00:00Z",
			want:   "2013-02-28T00:00:00Z",
		},
		{
			year:   0,
			month:  1,
			adjust: LastAdjust,
			date:   "2013-01-31T00:00:00Z",
			want:   "2013-02-28T00:00:00Z",
		},
		{
			year:   0,
			month:  1,
			adjust: ExcessAdjust,
			date:   "2013-01-31T00:00:00Z",
			want:   "2013-03-03T00:00:00Z",
		},
		{
			year:   2,
			month:  2,
			adjust: NoneAdjust,
			date:   "2011-12-31T00:00:00Z",
			want:   "2014-02-28T00:00:00Z",
		},
		{
			year:   2,
			month:  2,
			adjust: LastAdjust,
			date:   "2011-12-31T00:00:00Z",
			want:   "2014-02-28T00:00:00Z",
		},
		{
			year:   2,
			month:  2,
			adjust: ExcessAdjust,
			date:   "2011-12-31T00:00:00Z",
			want:   "2014-03-03T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: NoneAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-01-28T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: LastAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-01-31T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: ExcessAdjust,
			date:   "2013-02-28T00:00:00Z",
			want:   "2013-01-28T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: NoneAdjust,
			date:   "2012-12-31T00:00:00Z",
			want:   "2012-11-30T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: LastAdjust,
			date:   "2012-12-31T00:00:00Z",
			want:   "2012-11-30T00:00:00Z",
		},
		{
			year:   0,
			month:  -1,
			adjust: ExcessAdjust,
			date:   "2012-12-31T00:00:00Z",
			want:   "2012-12-01T00:00:00Z",
		},
		{
			year:   -2,
			month:  -2,
			adjust: NoneAdjust,
			date:   "2011-01-31T00:00:00Z",
			want:   "2008-11-30T00:00:00Z",
		},
		{
			year:   -2,
			month:  -2,
			adjust: LastAdjust,
			date:   "2011-12-31T00:00:00Z",
			want:   "2009-10-31T00:00:00Z",
		},
		{
			year:   -2,
			month:  -2,
			adjust: ExcessAdjust,
			date:   "2011-12-31T00:00:00Z",
			want:   "2009-10-31T00:00:00Z",
		},
	}

	for _, tc := range cases {
		tm, err := time.Parse(time.RFC3339, tc.date)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}
		dt, err := NewDatetime(tm)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err.Error())
		}
		t.Run(fmt.Sprintf("%d_%d_%d_%s", tc.year, tc.month, tc.adjust, tc.date),
			func(t *testing.T) {
				newdt, err := dt.Add(Interval{
					Year:   tc.year,
					Month:  tc.month,
					Adjust: tc.adjust,
				})
				if err != nil {
					t.Fatalf("Unable to add: %s", err.Error())
				}
				if newdt == nil {
					t.Fatalf("Unexpected nil value")
				}
				res := newdt.ToTime().Format(time.RFC3339)
				if res != tc.want {
					t.Fatalf("Unexpected result %s, expected %s", res, tc.want)
				}
			})
	}
}

func TestDatetimeAddSubSymmetric(t *testing.T) {
	tm := time.Unix(0, 0).UTC()
	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	newdtadd, err := dt.Add(Interval{
		Year:  1,
		Month: -3,
		Week:  3,
		Day:   4,
		Hour:  -5,
		Min:   5,
		Sec:   6,
		Nsec:  -3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	newdtsub, err := dt.Sub(Interval{
		Year:  -1,
		Month: 3,
		Week:  -3,
		Day:   -4,
		Hour:  5,
		Min:   -5,
		Sec:   -6,
		Nsec:  3,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	expected := "1970-10-25 19:05:05.999999997 +0000 UTC"
	addstr := newdtadd.ToTime().String()
	substr := newdtsub.ToTime().String()

	if addstr != expected {
		t.Fatalf("Unexpected Add result: %s, expected: %s", addstr, expected)
	}
	if substr != expected {
		t.Fatalf("Unexpected Sub result: %s, expected: %s", substr, expected)
	}
}

// We have a separate test for accurate Datetime boundaries.
func TestDatetimeAddOutOfRange(t *testing.T) {
	tm := time.Unix(0, 0).UTC()
	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err.Error())
	}

	newdt, err := dt.Add(Interval{Year: 1000000000})
	if err == nil {
		t.Fatalf("Unexpected success: %v", newdt)
	}
	expected := "time 1000001970-01-01 00:00:00 +0000 UTC is out of supported range"
	if err.Error() != expected {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
	if newdt != nil {
		t.Fatalf("Unexpected result: %v", newdt)
	}
}

func TestDatetimeInterval(t *testing.T) {
	var first = "2015-03-20T17:50:56.000000009+02:00"
	var second = "2013-01-31T11:51:58.00000009+01:00"

	tmFirst, err := time.Parse(time.RFC3339, first)
	if err != nil {
		t.Fatalf("Error in time.Parse(): %s", err)
	}
	tmSecond, err := time.Parse(time.RFC3339, second)
	if err != nil {
		t.Fatalf("Error in time.Parse(): %s", err)
	}

	dtFirst, err := NewDatetime(tmFirst)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tmFirst, err)
	}
	dtSecond, err := NewDatetime(tmSecond)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tmSecond, err)
	}

	ivalFirst := dtFirst.Interval(dtSecond)
	ivalSecond := dtSecond.Interval(dtFirst)

	expectedFirst := Interval{-2, -2, 0, 11, -6, 61, 2, 81, NoneAdjust}
	expectedSecond := Interval{2, 2, 0, -11, 6, -61, -2, -81, NoneAdjust}

	if !reflect.DeepEqual(ivalFirst, expectedFirst) {
		t.Errorf("Unexpected interval %v, expected %v", ivalFirst, expectedFirst)
	}
	if !reflect.DeepEqual(ivalSecond, expectedSecond) {
		t.Errorf("Unexpected interval %v, expected %v", ivalSecond, expectedSecond)
	}

	dtFirst, err = dtFirst.Add(ivalFirst)
	if err != nil {
		t.Fatalf("Unable to add an interval: %s", err)
	}
	if !dtFirst.ToTime().Equal(dtSecond.ToTime()) {
		t.Errorf("Incorrect add an interval result: %s, expected %s",
			dtFirst.ToTime(), dtSecond.ToTime())
	}
}

func TestDatetimeTarantoolInterval(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	dates := []string{
		// We could return tests with timezones after a release with a fix of
		// the bug:
		// https://github.com/tarantool/tarantool/issues/7698
		//
		// "2010-02-24T23:03:56.0000013-04:00",
		// "2015-03-20T17:50:56.000000009+01:00",
		// "2020-01-01T01:01:01+11:30",
		// "2025-08-01T00:00:00.000000003+11:00",
		"2010-02-24T23:03:56.0000013Z",
		"2015-03-20T17:50:56.000000009Z",
		"2020-01-01T01:01:01Z",
		"2025-08-01T00:00:00.000000003Z",
		"2015-12-21T17:50:53Z",
		"1980-03-28T13:18:39.000099Z",
	}
	datetimes := []*Datetime{}
	for _, date := range dates {
		tm, err := time.Parse(time.RFC3339, date)
		if err != nil {
			t.Fatalf("Error in time.Parse(%s): %s", date, err)
		}
		dt, err := NewDatetime(tm)
		if err != nil {
			t.Fatalf("Error in NewDatetime(%s): %s", tm, err)
		}
		datetimes = append(datetimes, dt)
	}

	for _, dti := range datetimes {
		for _, dtj := range datetimes {
			t.Run(fmt.Sprintf("%s_to_%s", dti.ToTime(), dtj.ToTime()),
				func(t *testing.T) {
					resp, err := conn.Call17("call_datetime_interval",
						[]interface{}{dti, dtj})
					if err != nil {
						t.Fatalf("Unable to call call_datetime_interval: %s", err)
					}
					ival := dti.Interval(dtj)
					ret := resp.Data[0].(Interval)
					if !reflect.DeepEqual(ival, ret) {
						t.Fatalf("%v != %v", ival, ret)
					}
				})
		}
	}
}

// Expect that first element of tuple is time.Time. Compare extracted actual
// and expected datetime values.
func assertDatetimeIsEqual(t *testing.T, tuples []interface{}, tm time.Time) {
	t.Helper()

	dtIndex := 0
	if tpl, ok := tuples[dtIndex].([]interface{}); !ok {
		t.Fatalf("Unexpected return value body")
	} else {
		if len(tpl) != 2 {
			t.Fatalf("Unexpected return value body (tuple len = %d)", len(tpl))
		}
		if val, ok := toDatetime(tpl[dtIndex]); !ok || !val.ToTime().Equal(tm) {
			t.Fatalf("Unexpected tuple %d field %v, expected %v",
				dtIndex,
				val,
				tm)
		}
	}
}

func TestTimezonesIndexMapping(t *testing.T) {
	for _, index := range TimezoneToIndex {
		if _, ok := IndexToTimezone[index]; !ok {
			t.Errorf("Index %d not found", index)
		}
	}
}

func TestTimezonesZonesMapping(t *testing.T) {
	for _, zone := range IndexToTimezone {
		if _, ok := TimezoneToIndex[zone]; !ok {
			t.Errorf("Zone %s not found", zone)
		}
	}
}

func TestInvalidTimezone(t *testing.T) {
	invalidLoc := time.FixedZone("AnyInvalid", 0)
	tm, err := time.Parse(time.RFC3339, "2010-08-12T11:39:14Z")
	if err != nil {
		t.Fatalf("Time parse failed: %s", err)
	}
	tm = tm.In(invalidLoc)
	dt, err := NewDatetime(tm)
	if err == nil {
		t.Fatalf("Unexpected success: %v", dt)
	}
	if err.Error() != "unknown timezone AnyInvalid with offset 0" {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
}

func TestInvalidOffset(t *testing.T) {
	tests := []struct {
		ok     bool
		offset int
	}{
		{ok: true, offset: -12 * 60 * 60},
		{ok: true, offset: -12*60*60 + 1},
		{ok: true, offset: 14*60*60 - 1},
		{ok: true, offset: 14 * 60 * 60},
		{ok: false, offset: -12*60*60 - 1},
		{ok: false, offset: 14*60*60 + 1},
	}

	for _, testcase := range tests {
		name := ""
		if testcase.ok {
			name = fmt.Sprintf("in_boundary_%d", testcase.offset)
		} else {
			name = fmt.Sprintf("out_of_boundary_%d", testcase.offset)
		}
		t.Run(name, func(t *testing.T) {
			loc := time.FixedZone("MSK", testcase.offset)
			tm, err := time.Parse(time.RFC3339, "2010-08-12T11:39:14Z")
			if err != nil {
				t.Fatalf("Time parse failed: %s", err)
			}
			tm = tm.In(loc)
			dt, err := NewDatetime(tm)
			if testcase.ok && err != nil {
				t.Fatalf("Unexpected error: %s", err.Error())
			}
			if !testcase.ok && err == nil {
				t.Fatalf("Unexpected success: %v", dt)
			}
			if testcase.ok && isDatetimeSupported {
				conn := test_helpers.ConnectWithValidation(t, server, opts)
				defer conn.Close()

				tupleInsertSelectDelete(t, conn, tm)
			}
		})
	}
}

func TestCustomTimezone(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	customZone := "Europe/Moscow"
	customOffset := 180 * 60
	// Tarantool does not use a custom offset value if a time zone is provided.
	// So it will change to an actual one.
	zoneOffset := 240 * 60

	customLoc := time.FixedZone(customZone, customOffset)
	tm, err := time.Parse(time.RFC3339, "2010-08-12T11:44:14Z")
	if err != nil {
		t.Fatalf("Time parse failed: %s", err)
	}
	tm = tm.In(customLoc)
	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unable to create datetime: %s", err.Error())
	}

	resp, err := conn.Replace(spaceTuple1, []interface{}{dt, "payload"})
	if err != nil {
		t.Fatalf("Datetime replace failed %s", err.Error())
	}
	assertDatetimeIsEqual(t, resp.Data, tm)

	tpl := resp.Data[0].([]interface{})
	if respDt, ok := toDatetime(tpl[0]); ok {
		zone := respDt.ToTime().Location().String()
		_, offset := respDt.ToTime().Zone()
		if zone != customZone {
			t.Fatalf("Expected zone %s instead of %s", customZone, zone)
		}
		if offset != zoneOffset {
			t.Fatalf("Expected offset %d instead of %d", customOffset, offset)
		}

		_, err = conn.Delete(spaceTuple1, 0, []interface{}{dt})
		if err != nil {
			t.Fatalf("Datetime delete failed: %s", err.Error())
		}
	} else {
		t.Fatalf("Datetime doesn't match")
	}

}

func tupleInsertSelectDelete(t *testing.T, conn *Connection, tm time.Time) {
	t.Helper()

	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tm, err)
	}

	// Insert tuple with datetime.
	_, err = conn.Insert(spaceTuple1, []interface{}{dt, "payload"})
	if err != nil {
		t.Fatalf("Datetime insert failed: %s", err.Error())
	}

	// Select tuple with datetime.
	var offset uint32 = 0
	var limit uint32 = 1
	resp, err := conn.Select(spaceTuple1, index, offset, limit, IterEq, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime select failed: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	assertDatetimeIsEqual(t, resp.Data, tm)

	// Delete tuple with datetime.
	resp, err = conn.Delete(spaceTuple1, index, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime delete failed: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Delete")
	}
	assertDatetimeIsEqual(t, resp.Data, tm)
}

var datetimeSample = []struct {
	fmt   string
	dt    string
	mpBuf string // MessagePack buffer.
	zone  string
}{
	/* Cases for base encoding without a timezone. */
	{time.RFC3339, "2012-01-31T23:59:59.000000010Z", "d8047f80284f000000000a00000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000000010Z", "d80400000000000000000a00000000000000", ""},
	{time.RFC3339, "2010-08-12T11:39:14Z", "d70462dd634c00000000", ""},
	{time.RFC3339, "1984-03-24T18:04:05Z", "d7041530c31a00000000", ""},
	{time.RFC3339, "2010-01-12T00:00:00Z", "d70480bb4b4b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.123456789Z", "d804000000000000000015cd5b0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.12345678Z", "d80400000000000000000ccd5b0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.1234567Z", "d8040000000000000000bccc5b0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.123456Z", "d804000000000000000000ca5b0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.12345Z", "d804000000000000000090b25b0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.1234Z", "d804000000000000000040ef5a0700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.123Z", "d8040000000000000000c0d4540700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.12Z", "d8040000000000000000000e270700000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.1Z", "d804000000000000000000e1f50500000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.01Z", "d80400000000000000008096980000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.001Z", "d804000000000000000040420f0000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0001Z", "d8040000000000000000a086010000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00001Z", "d80400000000000000001027000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000001Z", "d8040000000000000000e803000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0000001Z", "d80400000000000000006400000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00000001Z", "d80400000000000000000a00000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000000001Z", "d80400000000000000000100000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000000009Z", "d80400000000000000000900000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00000009Z", "d80400000000000000005a00000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0000009Z", "d80400000000000000008403000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000009Z", "d80400000000000000002823000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00009Z", "d8040000000000000000905f010000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0009Z", "d8040000000000000000a0bb0d0000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.009Z", "d80400000000000000004054890000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.09Z", "d8040000000000000000804a5d0500000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.9Z", "d804000000000000000000e9a43500000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.99Z", "d80400000000000000008033023b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.999Z", "d8040000000000000000c0878b3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.9999Z", "d80400000000000000006043993b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.99999Z", "d8040000000000000000f0a29a3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.999999Z", "d804000000000000000018c69a3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.9999999Z", "d80400000000000000009cc99a3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.99999999Z", "d8040000000000000000f6c99a3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.999999999Z", "d8040000000000000000ffc99a3b00000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.0000000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.00000000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1970-01-01T00:00:00.000000000Z", "d7040000000000000000", ""},
	{time.RFC3339, "1973-11-29T21:33:09Z", "d70415cd5b0700000000", ""},
	{time.RFC3339, "2013-10-28T17:51:56Z", "d7043ca46e5200000000", ""},
	{time.RFC3339, "9999-12-31T23:59:59Z", "d7047f41f4ff3a000000", ""},
	/* Cases for encoding with a timezone. */
	{time.RFC3339, "2006-01-02T15:04:00Z", "d804e040b9430000000000000000b400b303", "Europe/Moscow"},
}

func TestDatetimeInsertSelectDelete(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(testcase.fmt, testcase.dt)
			if testcase.zone == "" {
				tm = tm.In(noTimezoneLoc)
			} else {
				loc, err := time.LoadLocation(testcase.zone)
				if err != nil {
					t.Fatalf("Unable to load location: %s", err)
				}
				tm = tm.In(loc)
			}
			if err != nil {
				t.Fatalf("Time (%s) parse failed: %s", testcase.dt, err)
			}
			tupleInsertSelectDelete(t, conn, tm)
		})
	}
}

// time.Parse() could not parse formatted string with datetime where year is
// bigger than 9999. That's why testcase with maximum datetime value represented
// as a separate testcase. Testcase with minimal value added for consistency.
func TestDatetimeBoundaryRange(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for _, tm := range append(lesserBoundaryTimes, boundaryTimes...) {
		t.Run(tm.String(), func(t *testing.T) {
			tupleInsertSelectDelete(t, conn, tm)
		})
	}
}

func TestDatetimeOutOfRange(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	for _, tm := range greaterBoundaryTimes {
		t.Run(tm.String(), func(t *testing.T) {
			_, err := NewDatetime(tm)
			if err == nil {
				t.Errorf("Time %s should be unsupported!", tm)
			}
		})
	}
}

func TestDatetimeReplace(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tm, err := time.Parse(time.RFC3339, "2007-01-02T15:04:05Z")
	if err != nil {
		t.Fatalf("Time parse failed: %s", err)
	}

	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tm, err)
	}
	resp, err := conn.Replace(spaceTuple1, []interface{}{dt, "payload"})
	if err != nil {
		t.Fatalf("Datetime replace failed: %s", err)
	}
	if resp == nil {
		t.Fatalf("Response is nil after Replace")
	}
	assertDatetimeIsEqual(t, resp.Data, tm)

	resp, err = conn.Select(spaceTuple1, index, 0, 1, IterEq, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime select failed: %s", err)
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	assertDatetimeIsEqual(t, resp.Data, tm)

	// Delete tuple with datetime.
	_, err = conn.Delete(spaceTuple1, index, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime delete failed: %s", err.Error())
	}
}

type Event struct {
	Datetime Datetime
	Location string
}

type Tuple2 struct {
	Cid    uint
	Orig   string
	Events []Event
}

type Tuple1 struct {
	Datetime Datetime
}

func (t *Tuple1) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := e.Encode(&t.Datetime); err != nil {
		return err
	}
	return nil
}

func (t *Tuple1) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("Array len doesn't match: %d", l)
	}
	err = d.Decode(&t.Datetime)
	if err != nil {
		return err
	}
	return nil
}

func (ev *Event) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := e.EncodeString(ev.Location); err != nil {
		return err
	}
	if err := e.Encode(&ev.Datetime); err != nil {
		return err
	}
	return nil
}

func (ev *Event) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("Array len doesn't match: %d", l)
	}
	if ev.Location, err = d.DecodeString(); err != nil {
		return err
	}
	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	var ok bool
	if ev.Datetime, ok = toDatetime(res); !ok {
		return fmt.Errorf("Datetime doesn't match")
	}
	return nil
}

func (c *Tuple2) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint64(uint64(c.Cid)); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	e.Encode(c.Events)
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("Array len doesn't match: %d", l)
	}
	if c.Cid, err = d.DecodeUint(); err != nil {
		return err
	}
	if c.Orig, err = d.DecodeString(); err != nil {
		return err
	}
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	c.Events = make([]Event, l)
	for i := 0; i < l; i++ {
		d.Decode(&c.Events[i])
	}
	return nil
}

func TestCustomEncodeDecodeTuple1(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tm1, _ := time.Parse(time.RFC3339, "2010-05-24T17:51:56.000000009Z")
	tm2, _ := time.Parse(time.RFC3339, "2022-05-24T17:51:56.000000009Z")
	dt1, err := NewDatetime(tm1)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tm1, err)
	}
	dt2, err := NewDatetime(tm2)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tm2, err)
	}
	const cid = 13
	const orig = "orig"

	tuple := Tuple2{Cid: cid,
		Orig: orig,
		Events: []Event{
			{*dt1, "Minsk"},
			{*dt2, "Moscow"},
		},
	}
	resp, err := conn.Replace(spaceTuple2, &tuple)
	if err != nil || resp.Code != 0 {
		t.Fatalf("Failed to replace: %s", err.Error())
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Body len != 1")
	}

	tpl, ok := resp.Data[0].([]interface{})
	if !ok {
		t.Fatalf("Unexpected body of Replace")
	}

	// Delete the tuple.
	_, err = conn.Delete(spaceTuple2, index, []interface{}{cid})
	if err != nil {
		t.Fatalf("Datetime delete failed: %s", err.Error())
	}

	if len(tpl) != 3 {
		t.Fatalf("Unexpected body of Replace (tuple len)")
	}
	if id, ok := tpl[0].(uint64); !ok || id != cid {
		t.Fatalf("Unexpected body of Replace (%d)", cid)
	}
	if o, ok := tpl[1].(string); !ok || o != orig {
		t.Fatalf("Unexpected body of Replace (%s)", orig)
	}

	events, ok := tpl[2].([]interface{})
	if !ok {
		t.Fatalf("Unable to convert 2 field to []interface{}")
	}

	for i, tv := range []time.Time{tm1, tm2} {
		dt, ok := toDatetime(events[i].([]interface{})[1])
		if !ok || !dt.ToTime().Equal(tv) {
			t.Fatalf("%v != %v", dt.ToTime(), tv)
		}
	}
}

func TestCustomDecodeFunction(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Call function 'call_datetime_testdata' returning a table of custom tuples.
	var tuples []Tuple2
	err := conn.Call16Typed("call_datetime_testdata", []interface{}{1}, &tuples)
	if err != nil {
		t.Fatalf("Failed to CallTyped: %s", err.Error())
	}

	if cid := tuples[0].Cid; cid != 5 {
		t.Fatalf("Wrong Cid (%d), should be 5", cid)
	}
	if orig := tuples[0].Orig; orig != "Go!" {
		t.Fatalf("Wrong Orig (%s), should be 'Hello, there!'", orig)
	}

	events := tuples[0].Events
	if len(events) != 3 {
		t.Fatalf("Wrong a number of Events (%d), should be 3", len(events))
	}

	locations := []string{
		"Klushino",
		"Baikonur",
		"Novoselovo",
	}

	for i, ev := range events {
		loc := ev.Location
		dt := ev.Datetime
		if loc != locations[i] || dt.ToTime().IsZero() {
			t.Fatalf("Expected: %s non-zero time, got %s %v",
				locations[i],
				loc,
				dt.ToTime())
		}
	}
}

func TestCustomEncodeDecodeTuple5(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tm := time.Unix(500, 1000).In(time.FixedZone(NoTimezone, 0))
	dt, err := NewDatetime(tm)
	if err != nil {
		t.Fatalf("Unable to create Datetime from %s: %s", tm, err)
	}
	_, err = conn.Insert(spaceTuple1, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime insert failed: %s", err.Error())
	}

	resp, errSel := conn.Select(spaceTuple1, index, 0, 1, IterEq, []interface{}{dt})
	if errSel != nil {
		t.Errorf("Failed to Select: %s", errSel.Error())
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if val, ok := toDatetime(tpl[0]); !ok || !val.ToTime().Equal(tm) {
			t.Fatalf("Unexpected body of Select")
		}
	}

	// Teardown: delete a value.
	_, err = conn.Delete(spaceTuple1, index, []interface{}{dt})
	if err != nil {
		t.Fatalf("Datetime delete failed: %s", err.Error())
	}
}

func TestMPEncode(t *testing.T) {
	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(testcase.fmt, testcase.dt)
			if testcase.zone == "" {
				tm = tm.In(noTimezoneLoc)
			} else {
				loc, err := time.LoadLocation(testcase.zone)
				if err != nil {
					t.Fatalf("Unable to load location: %s", err)
				}
				tm = tm.In(loc)
			}
			if err != nil {
				t.Fatalf("Time (%s) parse failed: %s", testcase.dt, err)
			}
			dt, err := NewDatetime(tm)
			if err != nil {
				t.Fatalf("Unable to create Datetime from %s: %s", tm, err)
			}
			buf, err := marshal(dt)
			if err != nil {
				t.Fatalf("Marshalling failed: %s", err.Error())
			}
			refBuf, _ := hex.DecodeString(testcase.mpBuf)
			if reflect.DeepEqual(buf, refBuf) != true {
				t.Fatalf("Failed to encode datetime '%s', actual %x, expected %x",
					tm,
					buf,
					refBuf)
			}
		})
	}
}

func TestMPDecode(t *testing.T) {
	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(testcase.fmt, testcase.dt)
			if testcase.zone == "" {
				tm = tm.In(noTimezoneLoc)
			} else {
				loc, err := time.LoadLocation(testcase.zone)
				if err != nil {
					t.Fatalf("Unable to load location: %s", err)
				}
				tm = tm.In(loc)
			}
			if err != nil {
				t.Fatalf("Time (%s) parse failed: %s", testcase.dt, err)
			}
			buf, _ := hex.DecodeString(testcase.mpBuf)
			var v Datetime
			err = unmarshal(buf, &v)
			if err != nil {
				t.Fatalf("Unmarshalling failed: %s", err.Error())
			}
			if !tm.Equal(v.ToTime()) {
				t.Fatalf("Failed to decode datetime buf '%s', actual %v, expected %v",
					testcase.mpBuf,
					testcase.dt,
					v.ToTime())
			}
		})
	}
}

func TestUnmarshalMsgpackInvalidLength(t *testing.T) {
	var v Datetime

	err := v.UnmarshalMsgpack([]byte{0x04})
	if err == nil {
		t.Fatalf("Unexpected success %v", v)
	}
	if err.Error() != "invalid data length: got 1, wanted 8 or 16" {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
}

func TestUnmarshalMsgpackInvalidZone(t *testing.T) {
	var v Datetime

	// The original value from datetimeSample array:
	// {time.RFC3339 + " MST",
	//  "2006-01-02T15:04:00+03:00 MSK",
	//  "d804b016b9430000000000000000b400ee00"}
	buf, _ := hex.DecodeString("b016b9430000000000000000b400ee01")
	err := v.UnmarshalMsgpack(buf)
	if err == nil {
		t.Fatalf("Unexpected success %v", v)
	}
	if err.Error() != "unknown timezone index 494" {
		t.Fatalf("Unexpected error: %s", err.Error())
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Failed to extract Tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping datetime tests...")
		isDatetimeSupported = false
		return m.Run()
	} else {
		isDatetimeSupported = true
	}

	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "config.lua",
		Listen:       server,
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Fatalf("Failed to prepare test Tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
