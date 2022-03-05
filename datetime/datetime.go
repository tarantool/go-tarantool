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

	"gopkg.in/vmihailenco/msgpack.v2"
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
	// Timezone offset in minutes from UTC (not implemented in Tarantool,
	// see gh-163). Tarantool uses a int16_t type, see a structure
	// definition in src/lib/core/datetime.h.
	tzOffset int16
	// Olson timezone id (not implemented in Tarantool, see gh-163).
	// Tarantool uses a int16_t type, see a structure definition in
	// src/lib/core/datetime.h.
	tzIndex int16
}

// Size of datetime fields in a MessagePack value.
const (
	secondsSize  = 8
	nsecSize     = 4
	tzIndexSize  = 2
	tzOffsetSize = 2
)

const maxSize = secondsSize + nsecSize + tzIndexSize + tzOffsetSize

type Datetime struct {
	time time.Time
}

// NewDatetime returns a pointer to a new datetime.Datetime that contains a
// specified time.Time.
func NewDatetime(t time.Time) *Datetime {
	dt := new(Datetime)
	dt.time = t
	return dt
}

// ToTime returns a time.Time that Datetime contains.
func (dtime *Datetime) ToTime() time.Time {
	return dtime.time
}

var _ msgpack.Marshaler = (*Datetime)(nil)
var _ msgpack.Unmarshaler = (*Datetime)(nil)

func (dtime *Datetime) MarshalMsgpack() ([]byte, error) {
	tm := dtime.ToTime()

	var dt datetime
	dt.seconds = tm.Unix()
	dt.nsec = int32(tm.Nanosecond())
	dt.tzIndex = 0  // It is not implemented, see gh-163.
	dt.tzOffset = 0 // It is not implemented, see gh-163.

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
	tt := time.Unix(dt.seconds, int64(dt.nsec)).UTC()
	*tm = *NewDatetime(tt)

	return nil
}

func init() {
	msgpack.RegisterExt(datetime_extId, &Datetime{})
}
