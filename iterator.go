package tarantool

import (
	"github.com/tarantool/go-iproto"
)

// Iter is an enumeration type of a select iterator.
type Iter uint32

const (
	// IterEq is key == x ASC order.
	IterEq Iter = Iter(iproto.ITER_EQ)
	// IterReq is key == x DESC order.
	IterReq Iter = Iter(iproto.ITER_REQ)
	// IterAll selects all tuples.
	IterAll Iter = Iter(iproto.ITER_ALL)
	// IterLt is key < x.
	IterLt Iter = Iter(iproto.ITER_LT)
	// IterLe is key <= x.
	IterLe Iter = Iter(iproto.ITER_LE)
	// IterGe is key >= x.
	IterGe Iter = Iter(iproto.ITER_GE)
	// IterGt is key > x.
	IterGt Iter = Iter(iproto.ITER_GT)
	// IterBitsAllSet is all bits from x are set in key.
	IterBitsAllSet Iter = Iter(iproto.ITER_BITS_ALL_SET)
	// IterBitsAnySet is any bit from x is set in key.
	IterBitsAnySet Iter = Iter(iproto.ITER_BITS_ANY_SET)
	// IterBitsAllNotSet is all bits from x are not set in key.
	IterBitsAllNotSet Iter = Iter(iproto.ITER_BITS_ALL_NOT_SET)
	// IterOverlaps is key overlaps x.
	IterOverlaps Iter = Iter(iproto.ITER_OVERLAPS)
	// IterNeighbor returns tuples in distance ascending order from a specified point.
	IterNeighbor Iter = Iter(iproto.ITER_NEIGHBOR)
)
