// Package with support of Tarantool's datetime data type.
//
// Datetime data type supported in Tarantool since 2.10.
//
// Since: 1.7.0
//
// See also:
//
// * Datetime Internals https://github.com/tarantool/tarantool/wiki/Datetime-Internals
package datetime

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

// Datetime MessagePack serialization schema is an MP_EXT extension, which
// creates container of 8 or 16 bytes long payload.
//
//   +---------+--------+===============+-------------------------------+
//   |0xd7/0xd8|type (4)| seconds (8b)  | nsec; tzoffset; tzindex; (8b) |
//   +---------+--------+===============+-------------------------------+
//
// MessagePack data encoded using fixext8 (0xd7) or fixext16 (0xd8), and may
// contain:
//
// * [required] seconds parts as full, unencoded, signed 64-bit integer,
// stored in little-endian order;
//
// * [optional] all the other fields (nsec, tzoffset, tzindex) if any of them
// were having not 0 value. They are packed naturally in little-endian order;

// Datetime external type. Supported since Tarantool 2.10. See more details in
// issue https://github.com/tarantool/tarantool/issues/5946.
const datetimeExtID = 4

// datetime structure keeps a number of seconds and nanoseconds since Unix Epoch.
// Time is normalized by UTC, so time-zone offset is informative only.
type datetime struct {
	// Seconds since Epoch, where the epoch is the point where the time
	// starts, and is platform dependent. For Unix, the epoch is January 1,
	// 1970, 00:00:00 (UTC). Tarantool uses a double type, see a structure
	// definition in src/lib/core/datetime.h and reasons in
	// https://github.com/tarantool/tarantool/wiki/Datetime-internals#intervals-in-c
	seconds int64
	// Nanoseconds, fractional part of seconds. Tarantool uses int32_t, see
	// a definition in src/lib/core/datetime.h.
	nsec int32
	// Timezone offset in minutes from UTC. Tarantool uses a int16_t type,
	// see a structure definition in src/lib/core/datetime.h.
	tzOffset int16
	// Olson timezone id. Tarantool uses a int16_t type, see a structure
	// definition in src/lib/core/datetime.h.
	tzIndex int16
}

// Size of datetime fields in a MessagePack value.
const (
	secondsSize  = 8
	nsecSize     = 4
	tzIndexSize  = 2
	tzOffsetSize = 2
)

// Limits are from c-dt library:
// https://github.com/tarantool/c-dt/blob/e6214325fe8d4336464ebae859ac2b456fd22b77/API.pod#introduction
// https://github.com/tarantool/tarantool/blob/a99ccce5f517d2a04670289d3d09a8cc2f5916f9/src/lib/core/datetime.h#L44-L61
const (
	minSeconds = -185604722870400
	maxSeconds = 185480451417600
)

const maxSize = secondsSize + nsecSize + tzIndexSize + tzOffsetSize

//go:generate go run github.com/tarantool/go-option/cmd/gentypes -ext-code 4 Datetime
type Datetime struct {
	time time.Time
}

const (
	// NoTimezone allows to create a datetime without UTC timezone for
	// Tarantool. The problem is that Golang by default creates a time value
	// with UTC timezone. So it is a way to create a datetime without timezone.
	NoTimezone = ""
)

var noTimezoneLoc = time.FixedZone(NoTimezone, 0)

const (
	offsetMin = -12 * 60 * 60
	offsetMax = 14 * 60 * 60
)

// MakeDatetime returns a datetime.Datetime object that contains a
// specified time.Time. It may return an error if the Time value is out of
// supported range: [-5879610-06-22T00:00Z .. 5879611-07-11T00:00Z] or
// an invalid timezone or offset value is out of supported range:
// [-12 * 60 * 60, 14 * 60 * 60].
//
// NOTE: Tarantool's datetime.tz value is picked from t.Location().String().
// "Local" location is unsupported, see ExampleMakeDatetime_localUnsupported.
func MakeDatetime(t time.Time) (Datetime, error) {
	dt := Datetime{}
	seconds := t.Unix()

	if seconds < minSeconds || seconds > maxSeconds {
		return dt, fmt.Errorf("time %s is out of supported range", t)
	}

	zone := t.Location().String()
	_, offset := t.Zone()
	if zone != NoTimezone {
		if _, ok := timezoneToIndex[zone]; !ok {
			return dt, fmt.Errorf("unknown timezone %s with offset %d",
				zone, offset)
		}
	}

	if offset < offsetMin || offset > offsetMax {
		return dt, fmt.Errorf("offset must be between %d and %d hours",
			offsetMin, offsetMax)
	}

	dt.time = t
	return dt, nil
}

func intervalFromDatetime(dtime Datetime) (ival Interval) {
	ival.Year = int64(dtime.time.Year())
	ival.Month = int64(dtime.time.Month())
	ival.Day = int64(dtime.time.Day())
	ival.Hour = int64(dtime.time.Hour())
	ival.Min = int64(dtime.time.Minute())
	ival.Sec = int64(dtime.time.Second())
	ival.Nsec = int64(dtime.time.Nanosecond())
	ival.Adjust = NoneAdjust

	return ival
}

func daysInMonth(year int64, month int64) int64 {
	if month == 12 {
		year++
		month = 1
	} else {
		month += 1
	}

	// We use the fact that time.Date accepts values outside their usual
	// ranges - the values are normalized during the conversion.
	//
	// So we got a day (year, month - 1, last day of the month) before
	// (year, month, 1) because we pass (year, month, 0).
	return int64(time.Date(int(year), time.Month(month), 0, 0, 0, 0, 0, time.UTC).Day())
}

// C implementation:
// https://github.com/tarantool/c-dt/blob/cec6acebb54d9e73ea0b99c63898732abd7683a6/dt_arithmetic.c#L74-L98
func addMonth(ival Interval, delta int64, adjust Adjust) Interval {
	oldYear := ival.Year
	oldMonth := ival.Month

	ival.Month += delta
	if ival.Month < 1 || ival.Month > 12 {
		ival.Year += ival.Month / 12
		ival.Month %= 12
		if ival.Month < 1 {
			ival.Year--
			ival.Month += 12
		}
	}
	if adjust == ExcessAdjust || ival.Day < 28 {
		return ival
	}

	dim := daysInMonth(ival.Year, ival.Month)
	if ival.Day > dim || (adjust == LastAdjust && ival.Day == daysInMonth(oldYear, oldMonth)) {
		ival.Day = dim
	}
	return ival
}

func (d Datetime) MarshalMsgpack() ([]byte, error) {
	tm := d.ToTime()

	var dt datetime
	dt.seconds = tm.Unix()
	dt.nsec = int32(tm.Nanosecond())

	zone := tm.Location().String()
	_, offset := tm.Zone()
	if zone != NoTimezone {
		// The zone value already checked in MakeDatetime() or
		// UnmarshalMsgpack() calls.
		dt.tzIndex = int16(timezoneToIndex[zone])
	}
	dt.tzOffset = int16(offset / 60)

	var bytesSize = secondsSize
	if dt.nsec != 0 || dt.tzOffset != 0 || dt.tzIndex != 0 {
		bytesSize += nsecSize + tzIndexSize + tzOffsetSize
	}

	buf := make([]byte, bytesSize)
	binary.LittleEndian.PutUint64(buf, uint64(dt.seconds))
	if bytesSize == maxSize {
		binary.LittleEndian.PutUint32(buf[secondsSize:], uint32(dt.nsec))
		binary.LittleEndian.PutUint16(buf[secondsSize+nsecSize:], uint16(dt.tzOffset))
		binary.LittleEndian.PutUint16(buf[secondsSize+nsecSize+tzOffsetSize:], uint16(dt.tzIndex))
	}

	return buf, nil
}

func (d *Datetime) UnmarshalMsgpack(data []byte) error {
	var dt datetime

	sec := binary.LittleEndian.Uint64(data)
	dt.seconds = int64(sec)
	dt.nsec = 0
	if len(data) == maxSize {
		dt.nsec = int32(binary.LittleEndian.Uint32(data[secondsSize:]))
		dt.tzOffset = int16(binary.LittleEndian.Uint16(data[secondsSize+nsecSize:]))
		dt.tzIndex = int16(binary.LittleEndian.Uint16(data[secondsSize+nsecSize+tzOffsetSize:]))
	}

	tt := time.Unix(dt.seconds, int64(dt.nsec))

	loc := noTimezoneLoc
	if dt.tzIndex != 0 || dt.tzOffset != 0 {
		zone := NoTimezone
		offset := int(dt.tzOffset) * 60

		if dt.tzIndex != 0 {
			if _, ok := indexToTimezone[int(dt.tzIndex)]; !ok {
				return fmt.Errorf("unknown timezone index %d", dt.tzIndex)
			}
			zone = indexToTimezone[int(dt.tzIndex)]
		}
		if zone != NoTimezone {
			if loadLoc, err := time.LoadLocation(zone); err == nil {
				loc = loadLoc
			} else {
				// Unable to load location.
				loc = time.FixedZone(zone, offset)
			}
		} else {
			// Only offset.
			loc = time.FixedZone(zone, offset)
		}
	}
	tt = tt.In(loc)

	newDatetime, err := MakeDatetime(tt)
	if err != nil {
		return err
	}

	*d = newDatetime

	return nil
}

func (d Datetime) add(ival Interval, positive bool) (Datetime, error) {
	newVal := intervalFromDatetime(d)

	var direction int64
	if positive {
		direction = 1
	} else {
		direction = -1
	}

	newVal = addMonth(newVal, direction*ival.Year*12+direction*ival.Month, ival.Adjust)
	newVal.Day += direction * 7 * ival.Week
	newVal.Day += direction * ival.Day
	newVal.Hour += direction * ival.Hour
	newVal.Min += direction * ival.Min
	newVal.Sec += direction * ival.Sec
	newVal.Nsec += direction * ival.Nsec

	tm := time.Date(int(newVal.Year), time.Month(newVal.Month),
		int(newVal.Day), int(newVal.Hour), int(newVal.Min),
		int(newVal.Sec), int(newVal.Nsec), d.time.Location())

	return MakeDatetime(tm)
}

// Add creates a new Datetime as addition of the Datetime and Interval. It may
// return an error if a new Datetime is out of supported range.
func (d Datetime) Add(ival Interval) (Datetime, error) {
	return d.add(ival, true)
}

// Sub creates a new Datetime as subtraction of the Datetime and Interval. It
// may return an error if a new Datetime is out of supported range.
func (d Datetime) Sub(ival Interval) (Datetime, error) {
	return d.add(ival, false)
}

// Interval returns an Interval value to a next Datetime value.
func (d Datetime) Interval(next Datetime) Interval {
	curIval := intervalFromDatetime(d)
	nextIval := intervalFromDatetime(next)
	_, curOffset := d.time.Zone()
	_, nextOffset := next.time.Zone()
	curIval.Min -= int64(curOffset-nextOffset) / 60
	return nextIval.Sub(curIval)
}

// ToTime returns a time.Time that Datetime contains.
//
// If a Datetime created from time.Time value then an original location is used
// for the time value.
//
// If a Datetime created via unmarshaling Tarantool's datetime then we try to
// create a location with time.LoadLocation() first. In case of failure, we use
// a location created with time.FixedZone().
func (d *Datetime) ToTime() time.Time {
	return d.time
}

func datetimeEncoder(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
	dtime := v.Interface().(Datetime)

	return dtime.MarshalMsgpack()
}

func datetimeDecoder(d *msgpack.Decoder, v reflect.Value, extLen int) error {
	if extLen != maxSize && extLen != secondsSize {
		return fmt.Errorf("invalid data length: got %d, wanted %d or %d",
			extLen, secondsSize, maxSize)
	}

	b := make([]byte, extLen)
	switch n, err := d.Buffered().Read(b); {
	case err != nil:
		return err
	case n < extLen:
		return fmt.Errorf("msgpack: unexpected end of stream after %d datetime bytes", n)
	}

	ptr := v.Addr().Interface().(*Datetime)
	return ptr.UnmarshalMsgpack(b)
}

func init() {
	msgpack.RegisterExtDecoder(datetimeExtID, Datetime{}, datetimeDecoder)
	msgpack.RegisterExtEncoder(datetimeExtID, Datetime{}, datetimeEncoder)
}
