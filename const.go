package tarantool

const (
	SelectRequest    = 1
	InsertRequest    = 2
	ReplaceRequest   = 3
	UpdateRequest    = 4
	DeleteRequest    = 5
	CallRequest      = 6
	AuthRequest      = 7
	EvalRequest      = 8
	UpsertRequest    = 9
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
	IterGe            = uint32(5) // key > x
	IterGt            = uint32(6) // key >= x
	IterBitsAllSet    = uint32(7) // all bits from x are set in key
	IterBitsAnySet    = uint32(8) // at least one x's bit is set
	IterBitsAllNotSet = uint32(9) // all bits are not set

	OkCode            = uint32(0)
	PacketLengthBytes = 5
)

// Tarantool client error codes
const (
	ErrConnectionNotReady = 0x4000 + iota
	ErrConnectionClosed   = 0x4000 + iota
	ErrProtocolError      = 0x4000 + iota
)

// Tarantool server error codes
const (
	ErrUnknown                       = 0x8000 + iota // Unknown error
	ErrIllegalParams                 = 0x8000 + iota // Illegal parameters, %s
	ErrMemoryIssue                   = 0x8000 + iota // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = 0x8000 + iota // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = 0x8000 + iota // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = 0x8000 + iota // %s does not support %s
	ErrNonmaster                     = 0x8000 + iota // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = 0x8000 + iota // Can't modify data because this server is in read-only mode.
	ErrInjection                     = 0x8000 + iota // Error injection '%s'
	ErrCreateSpace                   = 0x8000 + iota // Failed to create space '%s': %s
	ErrSpaceExists                   = 0x8000 + iota // Space '%s' already exists
	ErrDropSpace                     = 0x8000 + iota // Can't drop space '%s': %s
	ErrAlterSpace                    = 0x8000 + iota // Can't modify space '%s': %s
	ErrIndexType                     = 0x8000 + iota // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = 0x8000 + iota // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = 0x8000 + iota // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = 0x8000 + iota // Tuple format limit reached: %u
	ErrDropPrimaryKey                = 0x8000 + iota // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = 0x8000 + iota // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = 0x8000 + iota // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = 0x8000 + iota // Invalid MsgPack - %s
	ErrProcRet                       = 0x8000 + iota // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = 0x8000 + iota // Tuple/Key must be MsgPack array
	ErrFieldType                     = 0x8000 + iota // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = 0x8000 + iota // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = 0x8000 + iota // SPLICE error on field %u: %s
	ErrArgType                       = 0x8000 + iota // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = 0x8000 + iota // Tuple is too long %u
	ErrUnknownUpdateOp               = 0x8000 + iota // Unknown UPDATE operation
	ErrUpdateField                   = 0x8000 + iota // Field %u UPDATE error: %s
	ErrFiberStack                    = 0x8000 + iota // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = 0x8000 + iota // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = 0x8000 + iota // %s
	ErrNoSuchProc                    = 0x8000 + iota // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = 0x8000 + iota // Trigger is not found
	ErrNoSuchIndex                   = 0x8000 + iota // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = 0x8000 + iota // Space '%s' does not exist
	ErrNoSuchField                   = 0x8000 + iota // Field %d was not found in the tuple
	ErrSpaceFieldCount               = 0x8000 + iota // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = 0x8000 + iota // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = 0x8000 + iota // Failed to write to disk
	ErrMoreThanOneTuple              = 0x8000 + iota // More than one tuple found by get()
	ErrAccessDenied                  = 0x8000 + iota // %s access denied for user '%s'
	ErrCreateUser                    = 0x8000 + iota // Failed to create user '%s': %s
	ErrDropUser                      = 0x8000 + iota // Failed to drop user '%s': %s
	ErrNoSuchUser                    = 0x8000 + iota // User '%s' is not found
	ErrUserExists                    = 0x8000 + iota // User '%s' already exists
	ErrPasswordMismatch              = 0x8000 + iota // Incorrect password supplied for user '%s'
	ErrUnknownRequestType            = 0x8000 + iota // Unknown request type %u
	ErrUnknownSchemaObject           = 0x8000 + iota // Unknown object type '%s'
	ErrCreateFunction                = 0x8000 + iota // Failed to create function '%s': %s
	ErrNoSuchFunction                = 0x8000 + iota // Function '%s' does not exist
	ErrFunctionExists                = 0x8000 + iota // Function '%s' already exists
	ErrFunctionAccessDenied          = 0x8000 + iota // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = 0x8000 + iota // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = 0x8000 + iota // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = 0x8000 + iota // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = 0x8000 + iota // Space engine '%s' does not exist
	ErrReloadCfg                     = 0x8000 + iota // Can't set option '%s' dynamically
	ErrCfg                           = 0x8000 + iota // Incorrect value for option '%s': %s
	ErrSophia                        = 0x8000 + iota // %s
	ErrLocalServerIsNotActive        = 0x8000 + iota // Local server is not active
	ErrUnknownServer                 = 0x8000 + iota // Server %s is not registered with the cluster
	ErrClusterIdMismatch             = 0x8000 + iota // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUuid                   = 0x8000 + iota // Invalid UUID: %s
	ErrClusterIdIsRo                 = 0x8000 + iota // Can't reset cluster id: it is already assigned
	ErrReserved66                    = 0x8000 + iota // Reserved66
	ErrServerIdIsReserved            = 0x8000 + iota // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = 0x8000 + iota // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = 0x8000 + iota // Missing mandatory field '%s' in request
	ErrIdentifier                    = 0x8000 + iota // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = 0x8000 + iota // Can't drop function %u: %s
	ErrIteratorType                  = 0x8000 + iota // Unknown iterator type '%s'
	ErrReplicaMax                    = 0x8000 + iota // Replica count limit reached: %u
	ErrInvalidXlog                   = 0x8000 + iota // Failed to read xlog: %lld
	ErrInvalidXlogName               = 0x8000 + iota // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = 0x8000 + iota // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = 0x8000 + iota // Connection is not established
	ErrTimeout                       = 0x8000 + iota // Timeout exceeded
	ErrActiveTransaction             = 0x8000 + iota // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = 0x8000 + iota // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = 0x8000 + iota // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = 0x8000 + iota // Role '%s' is not found
	ErrRoleExists                    = 0x8000 + iota // Role '%s' already exists
	ErrCreateRole                    = 0x8000 + iota // Failed to create role '%s': %s
	ErrIndexExists                   = 0x8000 + iota // Index '%s' already exists
	ErrTupleRefOverflow              = 0x8000 + iota // Tuple reference counter overflow
	ErrRoleLoop                      = 0x8000 + iota // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = 0x8000 + iota // Incorrect grant arguments: %s
	ErrPrivGranted                   = 0x8000 + iota // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = 0x8000 + iota // User '%s' already has role '%s'
	ErrPrivNotGranted                = 0x8000 + iota // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = 0x8000 + iota // User '%s' does not have role '%s'
	ErrMissingSnapshot               = 0x8000 + iota // Can't find snapshot
	ErrCantUpdatePrimaryKey          = 0x8000 + iota // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = 0x8000 + iota // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = 0x8000 + iota // Setting password for guest user has no effect
	ErrTransactionConflict           = 0x8000 + iota // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = 0x8000 + iota // Unsupported role privilege '%s'
	ErrLoadFunction                  = 0x8000 + iota // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = 0x8000 + iota // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = 0x8000 + iota // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = 0x8000 + iota // ???
	ErrUnknownRtreeIndexDistanceType = 0x8000 + iota //Unknown RTREE index distance type %s
	ErrProtocol                      = 0x8000 + iota // %s
	ErrUpsertUniqueSecondaryKey      = 0x8000 + iota // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = 0x8000 + iota // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = 0x8000 + iota // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = 0x8000 + iota // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = 0x8000 + iota // Wrong schema version, current: %d, in request: %u
	ErrSlabAllocMax                  = 0x8000 + iota // Failed to allocate %u bytes for tuple in the slab allocator: tuple is too large. Check 'slab_alloc_maximal' configuration option.
)
