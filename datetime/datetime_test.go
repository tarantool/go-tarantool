package datetime_test

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/tarantool/go-tarantool/v3"
	. "github.com/tarantool/go-tarantool/v3/datetime"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
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
	Timeout: 5 * time.Second,
}
var dialer = NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
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
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unexpected error")

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
	require.NoError(t, err, "Unexpected error")

	expected := "1970-10-25 19:05:05.999999997 +0000 UTC"
	assert.Equal(t, expected, newdt.ToTime().String(), "Unexpected result")
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
		require.NoError(t, err, "Unexpected error")
		dt, err := MakeDatetime(tm)
		require.NoError(t, err, "Unexpected error")
		t.Run(fmt.Sprintf("%d_%d_%d_%s", tc.year, tc.month, tc.adjust, tc.date),
			func(t *testing.T) {
				newdt, err := dt.Add(Interval{
					Year:   tc.year,
					Month:  tc.month,
					Adjust: tc.adjust,
				})
				require.NoError(t, err, "Unable to add")
				res := newdt.ToTime().Format(time.RFC3339)
				require.Equal(t, tc.want, res, "Unexpected result")
			})
	}
}

func TestDatetimeAddSubSymmetric(t *testing.T) {
	tm := time.Unix(0, 0).UTC()
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unexpected error")

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
	require.NoError(t, err, "Unexpected error")

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
	require.NoError(t, err, "Unexpected error")

	expected := "1970-10-25 19:05:05.999999997 +0000 UTC"
	require.Equal(t, expected, newdtadd.ToTime().String(), "Unexpected Add result")
	require.Equal(t, expected, newdtsub.ToTime().String(), "Unexpected Sub result")
}

func TestDatetimeAddOutOfRange(t *testing.T) {
	tm := time.Unix(0, 0).UTC()
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unexpected error")

	_, err = dt.Add(Interval{Year: 1000000000})
	require.Error(t, err, "Expected error for out of range")
	expected := "time 1000001970-01-01 00:00:00 +0000 UTC is out of supported range"
	require.Equal(t, expected, err.Error(), "Unexpected error")
}

func TestDatetimeInterval(t *testing.T) {
	var first = "2015-03-20T17:50:56.000000009+02:00"
	var second = "2013-01-31T11:51:58.00000009+01:00"

	tmFirst, err := time.Parse(time.RFC3339, first)
	require.NoError(t, err, "Error in time.Parse()")
	tmSecond, err := time.Parse(time.RFC3339, second)
	require.NoError(t, err, "Error in time.Parse()")

	dtFirst, err := MakeDatetime(tmFirst)
	require.NoError(t, err, "Unable to create Datetime")
	dtSecond, err := MakeDatetime(tmSecond)
	require.NoError(t, err, "Unable to create Datetime")

	ivalFirst := dtFirst.Interval(dtSecond)
	ivalSecond := dtSecond.Interval(dtFirst)

	expectedFirst := Interval{-2, -2, 0, 11, -6, 61, 2, 81, NoneAdjust}
	expectedSecond := Interval{2, 2, 0, -11, 6, -61, -2, -81, NoneAdjust}

	assert.Equal(t, expectedFirst, ivalFirst, "Unexpected interval")
	assert.Equal(t, expectedSecond, ivalSecond, "Unexpected interval")

	dtFirst, err = dtFirst.Add(ivalFirst)
	require.NoError(t, err, "Unable to add an interval")
	assert.True(t, dtFirst.ToTime().Equal(dtSecond.ToTime()), "Incorrect add an interval result")
}

func TestDatetimeTarantoolInterval(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
	datetimes := []Datetime{}
	for _, date := range dates {
		tm, err := time.Parse(time.RFC3339, date)
		require.NoError(t, err, "Error in time.Parse()")
		dt, err := MakeDatetime(tm)
		require.NoError(t, err, "Error in MakeDatetime()")
		datetimes = append(datetimes, dt)
	}

	for _, dti := range datetimes {
		for _, dtj := range datetimes {
			t.Run(fmt.Sprintf("%s_to_%s", dti.ToTime(), dtj.ToTime()),
				func(t *testing.T) {
					req := NewCallRequest("call_datetime_interval").
						Args([]interface{}{dti, dtj})
					data, err := conn.Do(req).Get()
					require.NoError(t, err, "Unable to call call_datetime_interval")
					ival := dti.Interval(dtj)
					ret := data[0].(Interval)
					require.Equal(t, ival, ret, "Interval mismatch")
				})
		}
	}
}

func assertDatetimeIsEqual(t *testing.T, tuples []interface{}, tm time.Time) {
	t.Helper()

	dtIndex := 0
	tpl, ok := tuples[dtIndex].([]interface{})
	require.True(t, ok, "Unexpected return value body")
	require.Len(t, tpl, 2, "Unexpected return value body")
	val, ok := tpl[dtIndex].(Datetime)
	require.True(t, ok, "Unexpected tuple field type")
	require.True(t, val.ToTime().Equal(tm), "Unexpected tuple field")
}

func TestTimezonesIndexMapping(t *testing.T) {
	for _, index := range TimezoneToIndex {
		assert.Contains(t, IndexToTimezone, index, "Index not found")
	}
}

func TestTimezonesZonesMapping(t *testing.T) {
	for _, zone := range IndexToTimezone {
		assert.Contains(t, TimezoneToIndex, zone, "Zone not found")
	}
}

func TestInvalidTimezone(t *testing.T) {
	invalidLoc := time.FixedZone("AnyInvalid", 0)
	tm, err := time.Parse(time.RFC3339, "2010-08-12T11:39:14Z")
	require.NoError(t, err, "Time parse failed")
	tm = tm.In(invalidLoc)
	_, err = MakeDatetime(tm)
	require.Error(t, err, "Expected error for invalid timezone")
	require.Equal(t, "unknown timezone AnyInvalid with offset 0", err.Error(), "Unexpected error")
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
		var name string
		if testcase.ok {
			name = fmt.Sprintf("in_boundary_%d", testcase.offset)
		} else {
			name = fmt.Sprintf("out_of_boundary_%d", testcase.offset)
		}
		t.Run(name, func(t *testing.T) {
			loc := time.FixedZone("MSK", testcase.offset)
			tm, err := time.Parse(time.RFC3339, "2010-08-12T11:39:14Z")
			require.NoError(t, err, "Time parse failed")
			tm = tm.In(loc)
			_, err = MakeDatetime(tm)
			if testcase.ok {
				require.NoError(t, err, "Unexpected error")
			} else {
				require.Error(t, err, "Expected error")
			}
			if testcase.ok && isDatetimeSupported {
				conn := test_helpers.ConnectWithValidation(t, dialer, opts)
				defer func() { _ = conn.Close() }()

				tupleInsertSelectDelete(t, conn, tm)
			}
		})
	}
}

func TestCustomTimezone(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	customZone := "Europe/Moscow"
	customOffset := 180 * 60
	// Tarantool does not use a custom offset value if a time zone is provided.
	// So it will change to an actual one.
	zoneOffset := 240 * 60

	customLoc := time.FixedZone(customZone, customOffset)
	tm, err := time.Parse(time.RFC3339, "2010-08-12T11:44:14Z")
	require.NoError(t, err, "Time parse failed")
	tm = tm.In(customLoc)
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unable to create datetime")

	req := NewReplaceRequest(spaceTuple1).Tuple([]interface{}{dt, "payload"})
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Datetime replace failed")
	assertDatetimeIsEqual(t, data, tm)

	tpl := data[0].([]interface{})
	respDt, ok := tpl[0].(Datetime)
	require.True(t, ok, "Datetime doesn't match")
	zone := respDt.ToTime().Location().String()
	_, offset := respDt.ToTime().Zone()
	require.Equal(t, customZone, zone, "Expected zone")
	require.Equal(t, zoneOffset, offset, "Expected offset")

	delReq := NewDeleteRequest(spaceTuple1).Key([]interface{}{dt})
	_, err = conn.Do(delReq).Get()
	require.NoError(t, err, "Datetime delete failed")
}

func tupleInsertSelectDelete(t *testing.T, conn *Connection, tm time.Time) {
	t.Helper()

	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unable to create Datetime from %s", tm)

	// Insert tuple with datetime.
	ins := NewInsertRequest(spaceTuple1).Tuple([]interface{}{dt, "payload"})
	_, err = conn.Do(ins).Get()
	require.NoError(t, err, "Datetime insert failed")

	// Select tuple with datetime.
	var offset uint32 = 0
	var limit uint32 = 1
	sel := NewSelectRequest(spaceTuple1).
		Index(index).
		Offset(offset).
		Limit(limit).
		Iterator(IterEq).
		Key([]interface{}{dt})
	data, err := conn.Do(sel).Get()
	require.NoError(t, err, "Datetime select failed")
	assertDatetimeIsEqual(t, data, tm)

	// Delete tuple with datetime.
	del := NewDeleteRequest(spaceTuple1).Index(index).Key([]interface{}{dt})
	data, err = conn.Do(del).Get()
	require.NoError(t, err, "Datetime delete failed")
	assertDatetimeIsEqual(t, data, tm)
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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(testcase.fmt, testcase.dt)
			if testcase.zone == "" {
				tm = tm.In(noTimezoneLoc)
			} else {
				loc, err := time.LoadLocation(testcase.zone)
				require.NoError(t, err, "Unable to load location")
				tm = tm.In(loc)
			}
			require.NoError(t, err, "Time (%s) parse failed", testcase.dt)
			tupleInsertSelectDelete(t, conn, tm)
		})
	}
}

// time.Parse() could not parse formatted string with datetime where year is
// bigger than 9999. That's why testcase with maximum datetime value represented
// as a separate testcase. Testcase with minimal value added for consistency.
func TestDatetimeBoundaryRange(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
			_, err := MakeDatetime(tm)
			assert.Error(t, err, "Time %s should be unsupported!", tm)
		})
	}
}

func TestDatetimeReplace(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	tm, err := time.Parse(time.RFC3339, "2007-01-02T15:04:05Z")
	require.NoError(t, err, "Time parse failed")

	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unable to create Datetime from %s", tm)

	rep := NewReplaceRequest(spaceTuple1).Tuple([]interface{}{dt, "payload"})
	data, err := conn.Do(rep).Get()
	require.NoError(t, err, "Datetime replace failed")
	assertDatetimeIsEqual(t, data, tm)

	sel := NewSelectRequest(spaceTuple1).
		Index(index).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{dt})
	data, err = conn.Do(sel).Get()
	require.NoError(t, err, "Datetime select failed")
	assertDatetimeIsEqual(t, data, tm)

	// Delete tuple with datetime.
	del := NewDeleteRequest(spaceTuple1).Index(index).Key([]interface{}{dt})
	_, err = conn.Do(del).Get()
	require.NoError(t, err, "Datetime delete failed")
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

func (t *Tuple1) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := e.Encode(&t.Datetime); err != nil {
		return err
	}
	return nil
}

func (t *Tuple1) DecodeMsgpack(d *msgpack.Decoder) error {
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

func (ev *Event) EncodeMsgpack(e *msgpack.Encoder) error {
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

func (ev *Event) DecodeMsgpack(d *msgpack.Decoder) error {
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

	if dt, ok := res.(Datetime); !ok {
		return fmt.Errorf("Datetime doesn't match")
	} else {
		ev.Datetime = dt
	}
	return nil
}

func (c *Tuple2) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint64(uint64(c.Cid)); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	if err := e.Encode(c.Events); err != nil {
		return err
	}
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *msgpack.Decoder) error {
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
		if err = d.Decode(&c.Events[i]); err != nil {
			return err
		}
	}
	return nil
}

func TestCustomEncodeDecodeTuple1(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	tm1, _ := time.Parse(time.RFC3339, "2010-05-24T17:51:56.000000009Z")
	tm2, _ := time.Parse(time.RFC3339, "2022-05-24T17:51:56.000000009Z")
	dt1, err := MakeDatetime(tm1)
	require.NoError(t, err, "Unable to create Datetime from %s", tm1)
	dt2, err := MakeDatetime(tm2)
	require.NoError(t, err, "Unable to create Datetime from %s", tm2)
	const cid = 13
	const orig = "orig"

	tuple := Tuple2{Cid: cid,
		Orig: orig,
		Events: []Event{
			{dt1, "Minsk"},
			{dt2, "Moscow"},
		},
	}
	rep := NewReplaceRequest(spaceTuple2).Tuple(&tuple)
	data, err := conn.Do(rep).Get()
	require.NoError(t, err, "Failed to replace")
	require.Len(t, data, 1, "Response Body len")

	tpl, ok := data[0].([]interface{})
	require.True(t, ok, "Unexpected body of Replace")

	// Delete the tuple.
	del := NewDeleteRequest(spaceTuple2).Index(index).Key([]interface{}{cid})
	_, err = conn.Do(del).Get()
	require.NoError(t, err, "Datetime delete failed")

	require.Len(t, tpl, 3, "Unexpected body of Replace (tuple len)")
	cidVal, ok := tpl[0].(uint64)
	require.True(t, ok, "Unexpected body of Replace (cid)")
	assert.Equal(t, uint64(cid), cidVal, "Unexpected body of Replace (cid)")
	origVal, ok := tpl[1].(string)
	require.True(t, ok, "Unexpected body of Replace (orig)")
	assert.Equal(t, orig, origVal, "Unexpected body of Replace (orig)")

	events, ok := tpl[2].([]interface{})
	require.True(t, ok, "Unable to convert 2 field to []interface{}")

	for i, tv := range []time.Time{tm1, tm2} {
		dt, ok := events[i].([]interface{})[1].(Datetime)
		require.True(t, ok, "Event datetime type")
		assert.True(t, dt.ToTime().Equal(tv), "%v != %v", dt.ToTime(), tv)
	}
}

func TestCustomDecodeFunction(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	// Call function 'call_datetime_testdata' returning a custom tuples.
	var tuple [][]Tuple2
	call := NewCallRequest("call_datetime_testdata").Args([]interface{}{1})
	err := conn.Do(call).GetTyped(&tuple)
	require.NoError(t, err, "Failed to CallTyped")

	require.Equal(t, uint(5), tuple[0][0].Cid, "Wrong Cid, should be 5")
	require.Equal(t, "Go!", tuple[0][0].Orig, "Wrong Orig, should be 'Hello, there!'")

	events := tuple[0][0].Events
	require.Len(t, events, 3, "Wrong a number of Events, should be 3")

	locations := []string{
		"Klushino",
		"Baikonur",
		"Novoselovo",
	}

	for i, ev := range events {
		loc := ev.Location
		dt := ev.Datetime
		assert.Equal(t, locations[i], loc, "Location mismatch")
		assert.False(t, dt.ToTime().IsZero(), "Expected non-zero time for %s, got %v", loc, dt.ToTime())
	}
}

func TestCustomEncodeDecodeTuple5(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	tm := time.Unix(500, 1000).In(time.FixedZone(NoTimezone, 0))
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unable to create Datetime from %s", tm)

	ins := NewInsertRequest(spaceTuple1).Tuple([]interface{}{dt})
	_, err = conn.Do(ins).Get()
	require.NoError(t, err, "Datetime insert failed")

	sel := NewSelectRequest(spaceTuple1).
		Index(index).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{dt})
	data, errSel := conn.Do(sel).Get()
	require.NoError(t, errSel, "Failed to Select")
	tpl, ok := data[0].([]interface{})
	require.True(t, ok, "Unexpected body of Select")
	val, ok := tpl[0].(Datetime)
	require.True(t, ok, "Unexpected body of Select")
	require.True(t, val.ToTime().Equal(tm), "Unexpected body of Select")

	// Teardown: delete a value.
	del := NewDeleteRequest(spaceTuple1).Index(index).Key([]interface{}{dt})
	_, err = conn.Do(del).Get()
	require.NoError(t, err, "Datetime delete failed")
}

func TestMPEncode(t *testing.T) {
	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(testcase.fmt, testcase.dt)
			if testcase.zone == "" {
				tm = tm.In(noTimezoneLoc)
			} else {
				loc, err := time.LoadLocation(testcase.zone)
				require.NoError(t, err, "Unable to load location")
				tm = tm.In(loc)
			}
			require.NoError(t, err, "Time (%s) parse failed", testcase.dt)
			dt, err := MakeDatetime(tm)
			require.NoError(t, err, "Unable to create Datetime")
			buf, err := msgpack.Marshal(dt)
			require.NoError(t, err, "Marshalling failed")
			refBuf, _ := hex.DecodeString(testcase.mpBuf)
			assert.Equal(t, refBuf, buf, "Failed to encode datetime")
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
				require.NoError(t, err, "Unable to load location")
				tm = tm.In(loc)
			}
			require.NoError(t, err, "Time (%s) parse failed", testcase.dt)
			buf, _ := hex.DecodeString(testcase.mpBuf)
			var v Datetime
			err = msgpack.Unmarshal(buf, &v)
			require.NoError(t, err, "Unmarshalling failed")
			assert.True(t, tm.Equal(v.ToTime()),
				"Failed to decode datetime buf '%s', actual %v, expected %v",
				testcase.mpBuf, testcase.dt, v.ToTime())
		})
	}
}

func TestUnmarshalMsgpackInvalidLength(t *testing.T) {
	var v Datetime

	err := msgpack.Unmarshal([]byte{0xd4, 0x04, 0x04}, &v)
	require.Error(t, err, "Unexpected success %v", v)
	assert.Equal(t, "invalid data length: got 1, wanted 8 or 16", err.Error(), "Unexpected error")
}

func TestUnmarshalMsgpackInvalidZone(t *testing.T) {
	var v Datetime

	// The original value from datetimeSample array:
	// {time.RFC3339 + " MST",
	//  "2006-01-02T15:04:00+03:00 MSK",
	//  "d804b016b9430000000000000000b400ee00"}
	buf, _ := hex.DecodeString("d804b016b9430000000000000000b400ee01")
	err := msgpack.Unmarshal(buf, &v)
	require.Error(t, err, "Unexpected success %v", v)
	assert.Equal(t, "unknown timezone index 494", err.Error(), "Unexpected error")
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
		Dialer:       dialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestDatetimeString(t *testing.T) {
	tm, _ := time.Parse(time.RFC3339Nano, "2010-05-24T17:51:56.000000009Z")
	dt, err := MakeDatetime(tm)
	require.NoError(t, err, "Unable to create Datetime from %s", tm)

	expected := "2010-05-24T17:51:56.000000009Z"

	result := dt.String()
	assert.Equal(t, expected, result, "String mismatch")
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
