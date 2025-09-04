package datetime

import (
	"bytes"
	"reflect"

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
//go:generate go run github.com/tarantool/go-option/cmd/gentypes -ext-code 6 Interval
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

func (i Interval) countNonZeroFields() int {
	count := 0

	for _, field := range []int64{
		i.Year, i.Month, i.Week, i.Day, i.Hour, i.Min, i.Sec, i.Nsec, adjustToDt[i.Adjust],
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

func (ival Interval) MarshalMsgpack() ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	if err := ival.MarshalMsgpackTo(enc); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

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
