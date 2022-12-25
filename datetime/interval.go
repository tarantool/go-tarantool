package datetime

import (
	"bytes"
	"fmt"
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

func encodeIntervalValue(e *msgpack.Encoder, typ uint64, value int64) (err error) {
	if value == 0 {
		return
	}
	err = e.EncodeUint(typ)
	if err == nil {
		if value > 0 {
			err = e.EncodeUint(uint64(value))
		} else if value < 0 {
			err = e.EncodeInt(int64(value))
		}
	}
	return
}

func encodeInterval(e *msgpack.Encoder, v reflect.Value) (err error) {
	val := v.Interface().(Interval)

	var fieldNum uint64
	for _, val := range []int64{val.Year, val.Month, val.Week, val.Day,
		val.Hour, val.Min, val.Sec, val.Nsec,
		adjustToDt[val.Adjust]} {
		if val != 0 {
			fieldNum++
		}
	}
	if err = e.EncodeUint(fieldNum); err != nil {
		return
	}

	if err = encodeIntervalValue(e, fieldYear, val.Year); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldMonth, val.Month); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldWeek, val.Week); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldDay, val.Day); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldHour, val.Hour); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldMin, val.Min); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldSec, val.Sec); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldNSec, val.Nsec); err != nil {
		return
	}
	if err = encodeIntervalValue(e, fieldAdjust, adjustToDt[val.Adjust]); err != nil {
		return
	}
	return nil
}

func decodeInterval(d *msgpack.Decoder, v reflect.Value) (err error) {
	var fieldNum uint
	if fieldNum, err = d.DecodeUint(); err != nil {
		return
	}

	var val Interval

	hasAdjust := false
	for i := 0; i < int(fieldNum); i++ {
		var fieldType uint
		if fieldType, err = d.DecodeUint(); err != nil {
			return
		}
		var fieldVal int64
		if fieldVal, err = d.DecodeInt64(); err != nil {
			return
		}
		switch fieldType {
		case fieldYear:
			val.Year = fieldVal
		case fieldMonth:
			val.Month = fieldVal
		case fieldWeek:
			val.Week = fieldVal
		case fieldDay:
			val.Day = fieldVal
		case fieldHour:
			val.Hour = fieldVal
		case fieldMin:
			val.Min = fieldVal
		case fieldSec:
			val.Sec = fieldVal
		case fieldNSec:
			val.Nsec = fieldVal
		case fieldAdjust:
			hasAdjust = true
			if adjust, ok := dtToAdjust[fieldVal]; ok {
				val.Adjust = adjust
			} else {
				return fmt.Errorf("unsupported Adjust: %d", fieldVal)
			}
		default:
			return fmt.Errorf("unsupported interval field type: %d", fieldType)
		}
	}

	if !hasAdjust {
		val.Adjust = dtToAdjust[0]
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
