package datetime

import "fmt"

// An Adjust is used as a parameter for date adjustions, see:
// https://github.com/tarantool/tarantool/wiki/Datetime-Internals#date-adjustions-and-leap-years
type Adjust int

const (
	NoneAdjust   Adjust = 0 // adjust = "none" in Tarantool
	ExcessAdjust Adjust = 1 // adjust = "excess" in Tarantool
	LastAdjust   Adjust = 2 // adjust = "last" in Tarantool
)

// We need the mappings to make NoneAdjust as a default value instead of
// dtExcess.
const (
	dtExcess = 0 // DT_EXCESS from dt-c/dt_arithmetic.h
	dtLimit  = 1 // DT_LIMIT
	dtSnap   = 2 // DT_SNAP
)

var adjustToDt = map[Adjust]int64{
	NoneAdjust:   dtLimit,
	ExcessAdjust: dtExcess,
	LastAdjust:   dtSnap,
}

var dtToAdjust = map[int64]Adjust{
	dtExcess: ExcessAdjust,
	dtLimit:  NoneAdjust,
	dtSnap:   LastAdjust,
}

// toDt returns the Tarantool representation of the adjust or an error if the
// adjust is unknown.
func (a Adjust) toDt() (int64, error) {
	dt, ok := adjustToDt[a]
	if !ok {
		return 0, fmt.Errorf("unknown interval adjust %d", a)
	}
	return dt, nil
}

// adjustFromDt returns the adjust for its Tarantool representation or an
// error if the representation is unknown.
func adjustFromDt(dt int64) (Adjust, error) {
	adjust, ok := dtToAdjust[dt]
	if !ok {
		return 0, fmt.Errorf("unknown interval adjust %d in msgpack", dt)
	}
	return adjust, nil
}
