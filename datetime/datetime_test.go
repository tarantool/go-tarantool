package datetime_test

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool"
	. "github.com/tarantool/go-tarantool/datetime"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var (
	minTime = time.Unix(0, 0)
	maxTime = time.Unix(1<<63-1, 999999999)
)

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
		if val, ok := tpl[dtIndex].(Datetime); !ok || !val.ToTime().Equal(tm) {
			t.Fatalf("Unexpected tuple %d field %v, expected %v",
				dtIndex,
				val.ToTime(),
				tm)
		}
	}
}

func tupleInsertSelectDelete(t *testing.T, conn *Connection, tm time.Time) {
	dt := NewDatetime(tm)

	// Insert tuple with datetime.
	_, err := conn.Insert(spaceTuple1, []interface{}{dt, "payload"})
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
	dt    string
	mpBuf string // MessagePack buffer.
}{
	{"2012-01-31T23:59:59.000000010Z", "d8047f80284f000000000a00000000000000"},
	{"1970-01-01T00:00:00.000000010Z", "d80400000000000000000a00000000000000"},
	{"2010-08-12T11:39:14Z", "d70462dd634c00000000"},
	{"1984-03-24T18:04:05Z", "d7041530c31a00000000"},
	{"2010-01-12T00:00:00Z", "d70480bb4b4b00000000"},
	{"1970-01-01T00:00:00Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.123456789Z", "d804000000000000000015cd5b0700000000"},
	{"1970-01-01T00:00:00.12345678Z", "d80400000000000000000ccd5b0700000000"},
	{"1970-01-01T00:00:00.1234567Z", "d8040000000000000000bccc5b0700000000"},
	{"1970-01-01T00:00:00.123456Z", "d804000000000000000000ca5b0700000000"},
	{"1970-01-01T00:00:00.12345Z", "d804000000000000000090b25b0700000000"},
	{"1970-01-01T00:00:00.1234Z", "d804000000000000000040ef5a0700000000"},
	{"1970-01-01T00:00:00.123Z", "d8040000000000000000c0d4540700000000"},
	{"1970-01-01T00:00:00.12Z", "d8040000000000000000000e270700000000"},
	{"1970-01-01T00:00:00.1Z", "d804000000000000000000e1f50500000000"},
	{"1970-01-01T00:00:00.01Z", "d80400000000000000008096980000000000"},
	{"1970-01-01T00:00:00.001Z", "d804000000000000000040420f0000000000"},
	{"1970-01-01T00:00:00.0001Z", "d8040000000000000000a086010000000000"},
	{"1970-01-01T00:00:00.00001Z", "d80400000000000000001027000000000000"},
	{"1970-01-01T00:00:00.000001Z", "d8040000000000000000e803000000000000"},
	{"1970-01-01T00:00:00.0000001Z", "d80400000000000000006400000000000000"},
	{"1970-01-01T00:00:00.00000001Z", "d80400000000000000000a00000000000000"},
	{"1970-01-01T00:00:00.000000001Z", "d80400000000000000000100000000000000"},
	{"1970-01-01T00:00:00.000000009Z", "d80400000000000000000900000000000000"},
	{"1970-01-01T00:00:00.00000009Z", "d80400000000000000005a00000000000000"},
	{"1970-01-01T00:00:00.0000009Z", "d80400000000000000008403000000000000"},
	{"1970-01-01T00:00:00.000009Z", "d80400000000000000002823000000000000"},
	{"1970-01-01T00:00:00.00009Z", "d8040000000000000000905f010000000000"},
	{"1970-01-01T00:00:00.0009Z", "d8040000000000000000a0bb0d0000000000"},
	{"1970-01-01T00:00:00.009Z", "d80400000000000000004054890000000000"},
	{"1970-01-01T00:00:00.09Z", "d8040000000000000000804a5d0500000000"},
	{"1970-01-01T00:00:00.9Z", "d804000000000000000000e9a43500000000"},
	{"1970-01-01T00:00:00.99Z", "d80400000000000000008033023b00000000"},
	{"1970-01-01T00:00:00.999Z", "d8040000000000000000c0878b3b00000000"},
	{"1970-01-01T00:00:00.9999Z", "d80400000000000000006043993b00000000"},
	{"1970-01-01T00:00:00.99999Z", "d8040000000000000000f0a29a3b00000000"},
	{"1970-01-01T00:00:00.999999Z", "d804000000000000000018c69a3b00000000"},
	{"1970-01-01T00:00:00.9999999Z", "d80400000000000000009cc99a3b00000000"},
	{"1970-01-01T00:00:00.99999999Z", "d8040000000000000000f6c99a3b00000000"},
	{"1970-01-01T00:00:00.999999999Z", "d8040000000000000000ffc99a3b00000000"},
	{"1970-01-01T00:00:00.0Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.00Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.0000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.00000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.000000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.0000000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.00000000Z", "d7040000000000000000"},
	{"1970-01-01T00:00:00.000000000Z", "d7040000000000000000"},
	{"1973-11-29T21:33:09Z", "d70415cd5b0700000000"},
	{"2013-10-28T17:51:56Z", "d7043ca46e5200000000"},
	{"9999-12-31T23:59:59Z", "d7047f41f4ff3a000000"},
}

func TestDatetimeInsertSelectDelete(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(time.RFC3339, testcase.dt)
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
func TestDatetimeMax(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tupleInsertSelectDelete(t, conn, maxTime)
}

func TestDatetimeMin(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tupleInsertSelectDelete(t, conn, minTime)
}

func TestDatetimeReplace(t *testing.T) {
	skipIfDatetimeUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tm, err := time.Parse(time.RFC3339, "2007-01-02T15:04:05Z")
	if err != nil {
		t.Fatalf("Time parse failed: %s", err)
	}

	dt := NewDatetime(tm)
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

func (t *Tuple1) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(2); err != nil {
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
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	err = d.Decode(&t.Datetime)
	if err != nil {
		return err
	}
	return nil
}

func (ev *Event) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(2); err != nil {
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
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if ev.Location, err = d.DecodeString(); err != nil {
		return err
	}
	res, err := d.DecodeInterface()
	if err != nil {
		return err
	}
	ev.Datetime = res.(Datetime)
	return nil
}

func (c *Tuple2) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint(c.Cid); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	e.Encode(c.Events)
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if c.Cid, err = d.DecodeUint(); err != nil {
		return err
	}
	if c.Orig, err = d.DecodeString(); err != nil {
		return err
	}
	if l, err = d.DecodeSliceLen(); err != nil {
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

	dt1, _ := time.Parse(time.RFC3339, "2010-05-24T17:51:56.000000009Z")
	dt2, _ := time.Parse(time.RFC3339, "2022-05-24T17:51:56.000000009Z")
	const cid = 13
	const orig = "orig"

	tuple := Tuple2{Cid: cid,
		Orig: orig,
		Events: []Event{
			{*NewDatetime(dt1), "Minsk"},
			{*NewDatetime(dt2), "Moscow"},
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

	for i, tv := range []time.Time{dt1, dt2} {
		dt := events[i].([]interface{})[1].(Datetime)
		if !dt.ToTime().Equal(tv) {
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

	tm := time.Unix(500, 1000)
	dt := NewDatetime(tm)
	_, err := conn.Insert(spaceTuple1, []interface{}{dt})
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
		if val, ok := tpl[0].(Datetime); !ok || !val.ToTime().Equal(tm) {
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
			tm, err := time.Parse(time.RFC3339, testcase.dt)
			if err != nil {
				t.Fatalf("Time (%s) parse failed: %s", testcase.dt, err)
			}
			dt := NewDatetime(tm)
			buf, err := msgpack.Marshal(dt)
			if err != nil {
				t.Fatalf("Marshalling failed: %s", err.Error())
			}
			refBuf, _ := hex.DecodeString(testcase.mpBuf)
			if reflect.DeepEqual(buf, refBuf) != true {
				t.Fatalf("Failed to encode datetime '%s', actual %v, expected %v",
					testcase.dt,
					buf,
					refBuf)
			}
		})
	}
}

func TestMPDecode(t *testing.T) {
	for _, testcase := range datetimeSample {
		t.Run(testcase.dt, func(t *testing.T) {
			tm, err := time.Parse(time.RFC3339, testcase.dt)
			if err != nil {
				t.Fatalf("Time (%s) parse failed: %s", testcase.dt, err)
			}
			buf, _ := hex.DecodeString(testcase.mpBuf)
			var v Datetime
			err = msgpack.Unmarshal(buf, &v)
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
		WorkDir:      "work_dir",
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
