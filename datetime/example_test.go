// Run a Tarantool instance before example execution:
// Terminal 1:
// $ cd datetime
// $ TEST_TNT_LISTEN=3013 TEST_TNT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
//
// Terminal 2:
// $ cd datetime
// $ go test -v example_test.go
package datetime_test

import (
	"fmt"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/datetime"
)

// Example demonstrates how to use tuples with datetime. To enable support of
// datetime import tarantool/datetime package.
func Example() {
	opts := tarantool.Opts{
		User: "test",
		Pass: "test",
	}
	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		fmt.Printf("Error in connect is %v", err)
		return
	}

	var datetime = "2013-10-28T17:51:56.000000009Z"
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %v", err)
		return
	}
	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tm, err)
		return
	}

	space := "testDatetime_1"
	index := "primary"

	// Replace a tuple with datetime.
	resp, err := conn.Replace(space, []interface{}{dt})
	if err != nil {
		fmt.Printf("Error in replace is %v", err)
		return
	}
	respDt := resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple replace")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())

	// Select a tuple with datetime.
	var offset uint32 = 0
	var limit uint32 = 1
	resp, err = conn.Select(space, index, offset, limit, tarantool.IterEq, []interface{}{dt})
	if err != nil {
		fmt.Printf("Error in select is %v", err)
		return
	}
	respDt = resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple select")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())

	// Delete a tuple with datetime.
	resp, err = conn.Delete(space, index, []interface{}{dt})
	if err != nil {
		fmt.Printf("Error in delete is %v", err)
		return
	}
	respDt = resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple delete")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())
}

// ExampleNewDatetime_localUnsupported demonstrates that "Local" location is
// unsupported.
func ExampleNewDatetime_localUnsupported() {
	tm := time.Now().Local()
	loc := tm.Location()
	fmt.Println("Location:", loc)
	if _, err := NewDatetime(tm); err != nil {
		fmt.Printf("Could not create a Datetime with %s location.\n", loc)
	} else {
		fmt.Printf("A Datetime with %s location created.\n", loc)
	}
	// Output:
	// Location: Local
	// Could not create a Datetime with Local location.
}

// Example demonstrates how to create a datetime for Tarantool without UTC
// timezone in datetime.
func ExampleNewDatetime_noTimezone() {
	var datetime = "2013-10-28T17:51:56.000000009Z"
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %v", err)
		return
	}

	tm = tm.In(time.FixedZone(NoTimezone, 0))

	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tm, err)
		return
	}

	fmt.Printf("Time value: %v\n", dt.ToTime())
}

// ExampleDatetime_Interval demonstrates how to get an Interval value between
// two Datetime values.
func ExampleDatetime_Interval() {
	var first = "2013-01-31T17:51:56.000000009Z"
	var second = "2015-03-20T17:50:56.000000009Z"

	tmFirst, err := time.Parse(time.RFC3339, first)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %v", err)
		return
	}
	tmSecond, err := time.Parse(time.RFC3339, second)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %v", err)
		return
	}

	dtFirst, err := NewDatetime(tmFirst)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tmFirst, err)
		return
	}
	dtSecond, err := NewDatetime(tmSecond)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tmSecond, err)
		return
	}

	ival := dtFirst.Interval(dtSecond)
	fmt.Printf("%v", ival)
	// Output:
	// {2 2 0 -11 0 -1 0 0 0}
}

// ExampleDatetime_Add demonstrates how to add an Interval to a Datetime value.
func ExampleDatetime_Add() {
	var datetime = "2013-01-31T17:51:56.000000009Z"
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %s", err)
		return
	}
	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tm, err)
		return
	}

	newdt, err := dt.Add(Interval{
		Year:   1,
		Month:  1,
		Sec:    333,
		Adjust: LastAdjust,
	})
	if err != nil {
		fmt.Printf("Unable to add to Datetime: %s", err)
		return
	}

	fmt.Printf("New time: %s\n", newdt.ToTime().String())
	// Output:
	// New time: 2014-02-28 17:57:29.000000009 +0000 UTC
}

// ExampleDatetime_Add_dst demonstrates how to add an Interval to a
// Datetime value with a DST location.
func ExampleDatetime_Add_dst() {
	loc, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		fmt.Printf("Unable to load location: %s", err)
		return
	}
	tm := time.Date(2008, 1, 1, 1, 1, 1, 1, loc)
	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime: %s", err)
		return
	}

	fmt.Printf("Datetime time:\n")
	fmt.Printf("%s\n", dt.ToTime())
	fmt.Printf("Datetime time + 6 month:\n")
	fmt.Printf("%s\n", dt.ToTime().AddDate(0, 6, 0))
	dt, err = dt.Add(Interval{Month: 6})
	if err != nil {
		fmt.Printf("Unable to add 6 month: %s", err)
		return
	}
	fmt.Printf("Datetime + 6 month time:\n")
	fmt.Printf("%s\n", dt.ToTime())

	// Output:
	// Datetime time:
	// 2008-01-01 01:01:01.000000001 +0300 MSK
	// Datetime time + 6 month:
	// 2008-07-01 01:01:01.000000001 +0400 MSD
	// Datetime + 6 month time:
	// 2008-07-01 01:01:01.000000001 +0400 MSD
}

// ExampleDatetime_Sub demonstrates how to subtract an Interval from a
// Datetime value.
func ExampleDatetime_Sub() {
	var datetime = "2013-01-31T17:51:56.000000009Z"
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		fmt.Printf("Error in time.Parse() is %s", err)
		return
	}
	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tm, err)
		return
	}

	newdt, err := dt.Sub(Interval{
		Year:   1,
		Month:  1,
		Sec:    333,
		Adjust: LastAdjust,
	})
	if err != nil {
		fmt.Printf("Unable to sub from Datetime: %s", err)
		return
	}

	fmt.Printf("New time: %s\n", newdt.ToTime().String())
	// Output:
	// New time: 2011-12-31 17:46:23.000000009 +0000 UTC
}

// ExampleInterval_Add demonstrates how to add two intervals.
func ExampleInterval_Add() {
	orig := Interval{
		Year:   1,
		Month:  2,
		Week:   3,
		Sec:    10,
		Adjust: ExcessAdjust,
	}
	ival := orig.Add(Interval{
		Year:   10,
		Min:    30,
		Adjust: LastAdjust,
	})

	fmt.Printf("%v", ival)
	// Output:
	// {11 2 3 0 0 30 10 0 1}
}

// ExampleInterval_Sub demonstrates how to subtract two intervals.
func ExampleInterval_Sub() {
	orig := Interval{
		Year:   1,
		Month:  2,
		Week:   3,
		Sec:    10,
		Adjust: ExcessAdjust,
	}
	ival := orig.Sub(Interval{
		Year:   10,
		Min:    30,
		Adjust: LastAdjust,
	})

	fmt.Printf("%v", ival)
	// Output:
	// {-9 2 3 0 0 -30 10 0 1}
}
