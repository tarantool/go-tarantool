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
	"time"
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
const datetime_extId = 4

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

// NewDatetime returns a pointer to a new datetime.Datetime that contains a
// specified time.Time. It may return an error if the Time value is out of
// supported range: [-5879610-06-22T00:00Z .. 5879611-07-11T00:00Z] or
// an invalid timezone or offset value is out of supported range:
// [-12 * 60 * 60, 14 * 60 * 60].
func NewDatetime(t time.Time) (*Datetime, error) {
	seconds := t.Unix()

	if seconds < minSeconds || seconds > maxSeconds {
		return nil, fmt.Errorf("time %s is out of supported range", t)
	}

	zone, offset := t.Zone()
	if zone != NoTimezone {
		if _, ok := timezoneToIndex[zone]; !ok {
			return nil, fmt.Errorf("unknown timezone %s with offset %d",
				zone, offset)
		}
	}
	if offset < offsetMin || offset > offsetMax {
		return nil, fmt.Errorf("offset must be between %d and %d hours",
			offsetMin, offsetMax)
	}

	dt := new(Datetime)
	dt.time = t
	return dt, nil
}

// ToTime returns a time.Time that Datetime contains.
func (dtime *Datetime) ToTime() time.Time {
	return dtime.time
}

func (dtime *Datetime) MarshalMsgpack() ([]byte, error) {
	tm := dtime.ToTime()

	var dt datetime
	dt.seconds = tm.Unix()
	dt.nsec = int32(tm.Nanosecond())

	zone, offset := tm.Zone()
	if zone != NoTimezone {
		// The zone value already checked in NewDatetime() or
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

func (tm *Datetime) UnmarshalMsgpack(b []byte) error {
	l := len(b)
	if l != maxSize && l != secondsSize {
		return fmt.Errorf("invalid data length: got %d, wanted %d or %d", len(b), secondsSize, maxSize)
	}

	var dt datetime
	sec := binary.LittleEndian.Uint64(b)
	dt.seconds = int64(sec)
	dt.nsec = 0
	if l == maxSize {
		dt.nsec = int32(binary.LittleEndian.Uint32(b[secondsSize:]))
		dt.tzOffset = int16(binary.LittleEndian.Uint16(b[secondsSize+nsecSize:]))
		dt.tzIndex = int16(binary.LittleEndian.Uint16(b[secondsSize+nsecSize+tzOffsetSize:]))
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
		loc = time.FixedZone(zone, offset)
	}
	tt = tt.In(loc)

	dtp, err := NewDatetime(tt)
	if dtp != nil {
		*tm = *dtp
	}
	return err
}
