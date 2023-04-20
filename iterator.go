package tarantool

import (
	"github.com/tarantool/go-iproto"
)

// Iter is an enumeration type of a select iterator.
type Iter uint32

const (
	// Key == x ASC order.
	IterEq Iter = Iter(iproto.ITER_EQ)
	// Key == x DESC order.
	IterReq Iter = Iter(iproto.ITER_REQ)
	// All tuples.
	IterAll Iter = Iter(iproto.ITER_ALL)
	// Key < x.
	IterLt Iter = Iter(iproto.ITER_LT)
	// Key <= x.
	IterLe Iter = Iter(iproto.ITER_LE)
	// Key >= x.
	IterGe Iter = Iter(iproto.ITER_GE)
	// Key > x.
	IterGt Iter = Iter(iproto.ITER_GT)
	// All bits from x are set in key.
	IterBitsAllSet Iter = Iter(iproto.ITER_BITS_ALL_SET)
	// All bits are not set.
	IterBitsAnySet Iter = Iter(iproto.ITER_BITS_ANY_SET)
	// All bits are not set.
	IterBitsAllNotSet Iter = Iter(iproto.ITER_BITS_ALL_NOT_SET)
	// Key overlaps x.
	IterOverlaps Iter = Iter(iproto.ITER_OVERLAPS)
	// Tuples in distance ascending order from specified point.
	IterNeighbor Iter = Iter(iproto.ITER_NEIGHBOR)
)
