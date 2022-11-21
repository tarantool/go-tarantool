package tarantool

const (
	SelectRequestCode    = 1
	InsertRequestCode    = 2
	ReplaceRequestCode   = 3
	UpdateRequestCode    = 4
	DeleteRequestCode    = 5
	Call16RequestCode    = 6 /* call in 1.6 format */
	AuthRequestCode      = 7
	EvalRequestCode      = 8
	UpsertRequestCode    = 9
	Call17RequestCode    = 10 /* call in >= 1.7 format */
	ExecuteRequestCode   = 11
	PrepareRequestCode   = 13
	BeginRequestCode     = 14
	CommitRequestCode    = 15
	RollbackRequestCode  = 16
	PingRequestCode      = 64
	SubscribeRequestCode = 66
	IdRequestCode        = 73
	WatchRequestCode     = 74
	UnwatchRequestCode   = 75

	KeyCode         = 0x00
	KeySync         = 0x01
	KeyStreamId     = 0x0a
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
	KeyError24      = 0x31 /* Error in pre-2.4 format. */
	KeyMetaData     = 0x32
	KeyBindCount    = 0x34
	KeySQLText      = 0x40
	KeySQLBind      = 0x41
	KeySQLInfo      = 0x42
	KeyStmtID       = 0x43
	KeyError        = 0x52 /* Extended error in >= 2.4 format. */
	KeyVersion      = 0x54
	KeyFeatures     = 0x55
	KeyTimeout      = 0x56
	KeyEvent        = 0x57
	KeyEventData    = 0x58
	KeyTxnIsolation = 0x59

	KeyFieldName               = 0x00
	KeyFieldType               = 0x01
	KeyFieldColl               = 0x02
	KeyFieldIsNullable         = 0x03
	KeyIsAutoincrement         = 0x04
	KeyFieldSpan               = 0x05
	KeySQLInfoRowCount         = 0x00
	KeySQLInfoAutoincrementIds = 0x01

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
	EventCode         = uint32(0x4c)
	PushCode          = uint32(0x80)
	ErrorCodeBit      = 0x8000
	PacketLengthBytes = 5
	ErSpaceExistsCode = 0xa
	IteratorCode      = 0x14
)
