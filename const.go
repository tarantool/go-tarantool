package tarantool

const (
	SelectRequest    = 1
	InsertRequest    = 2
	ReplaceRequest   = 3
	UpdateRequest    = 4
	DeleteRequest    = 5
	CallRequest      = 6 /* call in 1.6 format */
	AuthRequest      = 7
	EvalRequest      = 8
	UpsertRequest    = 9
	Call17Request    = 10
	PingRequest      = 64
	SubscribeRequest = 66

	KeyCode         = 0x00
	KeySync         = 0x01
	KeySpaceNo      = 0x10
	KeyIndexNo      = 0x11
	KeyLimit        = 0x12
	KeyOffset       = 0x13
	KeyIterator     = 0x14
	KeyKey          = 0x20
	KeyTuple        = 0x21
	KeyFunctionName = 0x22
	KeyUserName     = 0x23
	KeyExpression   = 0x27
	KeyDefTuple     = 0x28
	KeyData         = 0x30
	KeyError        = 0x31

	// https://github.com/fl00r/go-tarantool-1.6/issues/2

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

	RLimitDrop = 1
	RLimitWait = 2

	OkCode            = uint32(0)
	ErrorCodeBit      = 0x8000
	PacketLengthBytes = 5
)
