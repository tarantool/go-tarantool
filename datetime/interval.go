package datetime

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"

	"github.com/vmihailenco/msgpack/v5"
)

const interval_extId = 6

const (
	fieldYear   = 0
	fieldMonth  = 1
	fieldWeek   = 2
	fieldDay    = 3
	fieldHour   = 4
	fieldMin    = 5
	fieldSec    = 6
	fieldNSec   = 7
	fieldAdjust = 8
)

// Interval type is GoLang implementation of Tarantool intervals.
//
//go:generate go tool gentypes -ext-code 6 Interval
type Interval struct {
	Year   int64
	Month  int64
	Week   int64
	Day    int64
	Hour   int64
	Min    int64
	Sec    int64
	Nsec   int64
	Adjust Adjust
}

func (ival Interval) countNonZeroFields() int {
	count := 0

	for _, field := range []int64{
		ival.Year, ival.Month, ival.Week, ival.Day, ival.Hour,
		ival.Min, ival.Sec, ival.Nsec, adjustToDt[ival.Adjust],
	} {
		if field != 0 {
			count++
		}
	}

	return count
}

// We use int64 for every field to avoid changes in the future, see:
// https://github.com/tarantool/tarantool/blob/943ce3caf8401510ced4f074bca7006c3d73f9b3/src/lib/core/datetime.h#L106

// Add creates a new Interval as addition of intervals.
func (ival Interval) Add(add Interval) Interval {
	ival.Year += add.Year
	ival.Month += add.Month
	ival.Week += add.Week
	ival.Day += add.Day
	ival.Hour += add.Hour
	ival.Min += add.Min
	ival.Sec += add.Sec
	ival.Nsec += add.Nsec

	return ival
}

// Sub creates a new Interval as subtraction of intervals.
func (ival Interval) Sub(sub Interval) Interval {
	ival.Year -= sub.Year
	ival.Month -= sub.Month
	ival.Week -= sub.Week
	ival.Day -= sub.Day
	ival.Hour -= sub.Hour
	ival.Min -= sub.Min
	ival.Sec -= sub.Sec
	ival.Nsec -= sub.Nsec

	return ival
}

// MarshalMsgpack implements a custom msgpack marshaler.
func (ival Interval) MarshalMsgpack() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	if err := ival.MarshalMsgpackTo(enc); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// MarshalMsgpackTo implements a custom msgpack marshaler.
func (ival Interval) MarshalMsgpackTo(e *msgpack.Encoder) error {
	var fieldNum = uint64(ival.countNonZeroFields())
	if err := e.EncodeUint(fieldNum); err != nil {
		return err
	}

	if err := encodeIntervalValue(e, fieldYear, ival.Year); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldMonth, ival.Month); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldWeek, ival.Week); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldDay, ival.Day); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldHour, ival.Hour); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldMin, ival.Min); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldSec, ival.Sec); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldNSec, ival.Nsec); err != nil {
		return err
	}
	if err := encodeIntervalValue(e, fieldAdjust, adjustToDt[ival.Adjust]); err != nil {
		return err
	}

	return nil
}

// UnmarshalMsgpackFrom implements a custom msgpack unmarshaler.
func (ival *Interval) UnmarshalMsgpackFrom(d *msgpack.Decoder) error {
	fieldNum, err := d.DecodeUint()
	if err != nil {
		return err
	}

	ival.Adjust = dtToAdjust[int64(NoneAdjust)]

	for i := 0; i < int(fieldNum); i++ {
		var fieldType uint
		if fieldType, err = d.DecodeUint(); err != nil {
			return err
		}

		var fieldVal int64
		if fieldVal, err = d.DecodeInt64(); err != nil {
			return err
		}

		switch fieldType {
		case fieldYear:
			ival.Year = fieldVal
		case fieldMonth:
			ival.Month = fieldVal
		case fieldWeek:
			ival.Week = fieldVal
		case fieldDay:
			ival.Day = fieldVal
		case fieldHour:
			ival.Hour = fieldVal
		case fieldMin:
			ival.Min = fieldVal
		case fieldSec:
			ival.Sec = fieldVal
		case fieldNSec:
			ival.Nsec = fieldVal
		case fieldAdjust:
			ival.Adjust = dtToAdjust[fieldVal]
		}
	}

	return nil
}

// UnmarshalMsgpack implements a custom msgpack unmarshaler.
func (ival *Interval) UnmarshalMsgpack(data []byte) error {
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	return ival.UnmarshalMsgpackFrom(dec)
}

func encodeIntervalValue(e *msgpack.Encoder, typ uint64, value int64) error {
	if value == 0 {
		return nil
	}

	err := e.EncodeUint(typ)
	if err != nil {
		return err
	}

	switch {
	case value > 0:
		return e.EncodeUint(uint64(value))
	default:
		return e.EncodeInt(value)
	}
}

func encodeInterval(e *msgpack.Encoder, v reflect.Value) (err error) {
	val := v.Interface().(Interval)
	return val.MarshalMsgpackTo(e)
}

func decodeInterval(d *msgpack.Decoder, v reflect.Value) (err error) {
	val := Interval{}
	if err = val.UnmarshalMsgpackFrom(d); err != nil {
		return
	}

	v.Set(reflect.ValueOf(val))
	return nil
}

// Returns a human-readable string representation of the interval.
func (ival Interval) String() string {
	if ival.countNonZeroFields() == 0 {
		return "0 seconds"
	}

	parts := make([]string, 0, 9)

	// Helper function for adding components.
	addPart := func(value int64, singular, plural string) {
		if value == 0 {
			return
		}
		if value == 1 || value == -1 {
			parts = append(parts, fmt.Sprintf("%d %s", value, singular))
		} else {
			parts = append(parts, fmt.Sprintf("%d %s", value, plural))
		}
	}

	addPart(ival.Year, "year", "years")
	addPart(ival.Month, "month", "months")
	addPart(ival.Week, "week", "weeks")
	addPart(ival.Day, "day", "days")
	addPart(ival.Hour, "hour", "hours")
	addPart(ival.Min, "minute", "minutes")

	// Processing seconds and nanoseconds - combine if both are present.
	if ival.Sec != 0 && ival.Nsec != 0 {
		// Define a common symbol for proper formatting.
		secSign := ival.Sec < 0
		nsecSign := ival.Nsec < 0

		if secSign == nsecSign {
			// Same signs - combine them.
			absSec := ival.Sec
			absNsec := ival.Nsec
			if secSign {
				absSec = -absSec
				absNsec = -absNsec
			}
			parts = append(parts, fmt.Sprintf("%s%d.%09d seconds",
				boolToSign(secSign), absSec, absNsec))
		} else {
			// Different characters - output separately.
			addPart(ival.Sec, "second", "seconds")
			addPart(ival.Nsec, "nanosecond", "nanoseconds")
		}
	} else {
		// Only seconds or only nanoseconds.
		addPart(ival.Sec, "second", "seconds")
		addPart(ival.Nsec, "nanosecond", "nanoseconds")
	}

	return joinIntervalParts(parts)
}

// Returns "-" for true and an empty string for false.
func boolToSign(negative bool) string {
	if negative {
		return "-"
	}
	return ""
}

// Combines parts of an interval into a readable string.
func joinIntervalParts(parts []string) string {
	switch len(parts) {
	case 0:
		return "0 seconds"
	case 1:
		return parts[0]
	case 2:
		return parts[0] + " and " + parts[1]
	default:
		return strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
	}
}

func init() {
	msgpack.RegisterExtEncoder(interval_extId, Interval{},
		func(e *msgpack.Encoder, v reflect.Value) (ret []byte, err error) {
			var b bytes.Buffer

			enc := msgpack.NewEncoder(&b)
			if err = encodeInterval(enc, v); err == nil {
				ret = b.Bytes()
			}

			return
		})
	msgpack.RegisterExtDecoder(interval_extId, Interval{},
		func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
			return decodeInterval(d, v)
		})
}
