package tarantool

import (
	"github.com/tarantool/go-iproto"
)

const (
	packetLengthBytes = 5
)

const (
	IterEq            = uint32(0) // key == x ASC order
	IterReq           = uint32(1) // key == x DESC order
	IterAll           = uint32(2) // all tuples
	IterLt            = uint32(3) // key < x
	IterLe            = uint32(4) // key <= x
	IterGe            = uint32(5) // key >= x
	IterGt            = uint32(6) // key > x
	IterBitsAllSet    = uint32(7) // all bits from x are set in key
	IterBitsAnySet    = uint32(8) // at least one x's bit is set
	IterBitsAllNotSet = uint32(9) // all bits are not set
	IterOverlaps      = uint32(10) // key overlaps x
	IterNeighbor      = uint32(11) // tuples in distance ascending order from specified point

	RLimitDrop = 1
	RLimitWait = 2

	OkCode   = uint32(iproto.IPROTO_OK)
	PushCode = uint32(iproto.IPROTO_CHUNK)
)
