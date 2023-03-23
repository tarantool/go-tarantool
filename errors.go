package tarantool

import "fmt"

// Error is wrapper around error returned by Tarantool.
type Error struct {
	Code         uint32
	Msg          string
	ExtendedInfo *BoxError
}

// Error converts an Error to a string.
func (tnterr Error) Error() string {
	if tnterr.ExtendedInfo != nil {
		return tnterr.ExtendedInfo.Error()
	}

	return fmt.Sprintf("%s (0x%x)", tnterr.Msg, tnterr.Code)
}

// ClientError is connection error produced by this client,
// i.e. connection failures or timeouts.
type ClientError struct {
	Code uint32
	Msg  string
}

// Error converts a ClientError to a string.
func (clierr ClientError) Error() string {
	return fmt.Sprintf("%s (0x%x)", clierr.Msg, clierr.Code)
}

// Temporary returns true if next attempt to perform request may succeeded.
//
// Currently it returns true when:
//
// - Connection is not connected at the moment
//
// - request is timeouted
//
// - request is aborted due to rate limit
func (clierr ClientError) Temporary() bool {
	switch clierr.Code {
	case ErrConnectionNotReady, ErrTimeouted, ErrRateLimited:
		return true
	default:
		return false
	}
}

// Tarantool client error codes.
const (
	ErrConnectionNotReady = 0x4000 + iota
	ErrConnectionClosed   = 0x4000 + iota
	ErrProtocolError      = 0x4000 + iota
	ErrTimeouted          = 0x4000 + iota
	ErrRateLimited        = 0x4000 + iota
	ErrConnectionShutdown = 0x4000 + iota
)

// Tarantool server error codes.
const (
	ErrUnknown                       = 0   // Unknown error
	ErrIllegalParams                 = 1   // Illegal parameters, %s
	ErrMemoryIssue                   = 2   // Failed to allocate %u bytes in %s for %s
	ErrTupleFound                    = 3   // Duplicate key exists in unique index '%s' in space '%s'
	ErrTupleNotFound                 = 4   // Tuple doesn't exist in index '%s' in space '%s'
	ErrUnsupported                   = 5   // %s does not support %s
	ErrNonmaster                     = 6   // Can't modify data on a replication slave. My master is: %s
	ErrReadonly                      = 7   // Can't modify data because this server is in read-only mode.
	ErrInjection                     = 8   // Error injection '%s'
	ErrCreateSpace                   = 9   // Failed to create space '%s': %s
	ErrSpaceExists                   = 10  // Space '%s' already exists
	ErrDropSpace                     = 11  // Can't drop space '%s': %s
	ErrAlterSpace                    = 12  // Can't modify space '%s': %s
	ErrIndexType                     = 13  // Unsupported index type supplied for index '%s' in space '%s'
	ErrModifyIndex                   = 14  // Can't create or modify index '%s' in space '%s': %s
	ErrLastDrop                      = 15  // Can't drop the primary key in a system space, space '%s'
	ErrTupleFormatLimit              = 16  // Tuple format limit reached: %u
	ErrDropPrimaryKey                = 17  // Can't drop primary key in space '%s' while secondary keys exist
	ErrKeyPartType                   = 18  // Supplied key type of part %u does not match index part type: expected %s
	ErrExactMatch                    = 19  // Invalid key part count in an exact match (expected %u, got %u)
	ErrInvalidMsgpack                = 20  // Invalid MsgPack - %s
	ErrProcRet                       = 21  // msgpack.encode: can not encode Lua type '%s'
	ErrTupleNotArray                 = 22  // Tuple/Key must be MsgPack array
	ErrFieldType                     = 23  // Tuple field %u type does not match one required by operation: expected %s
	ErrFieldTypeMismatch             = 24  // Ambiguous field type in index '%s', key part %u. Requested type is %s but the field has previously been defined as %s
	ErrSplice                        = 25  // SPLICE error on field %u: %s
	ErrArgType                       = 26  // Argument type in operation '%c' on field %u does not match field type: expected a %s
	ErrTupleIsTooLong                = 27  // Tuple is too long %u
	ErrUnknownUpdateOp               = 28  // Unknown UPDATE operation
	ErrUpdateField                   = 29  // Field %u UPDATE error: %s
	ErrFiberStack                    = 30  // Can not create a new fiber: recursion limit reached
	ErrKeyPartCount                  = 31  // Invalid key part count (expected [0..%u], got %u)
	ErrProcLua                       = 32  // %s
	ErrNoSuchProc                    = 33  // Procedure '%.*s' is not defined
	ErrNoSuchTrigger                 = 34  // Trigger is not found
	ErrNoSuchIndex                   = 35  // No index #%u is defined in space '%s'
	ErrNoSuchSpace                   = 36  // Space '%s' does not exist
	ErrNoSuchField                   = 37  // Field %d was not found in the tuple
	ErrSpaceFieldCount               = 38  // Tuple field count %u does not match space '%s' field count %u
	ErrIndexFieldCount               = 39  // Tuple field count %u is less than required by a defined index (expected %u)
	ErrWalIo                         = 40  // Failed to write to disk
	ErrMoreThanOneTuple              = 41  // More than one tuple found by get()
	ErrAccessDenied                  = 42  // %s access denied for user '%s'
	ErrCreateUser                    = 43  // Failed to create user '%s': %s
	ErrDropUser                      = 44  // Failed to drop user '%s': %s
	ErrNoSuchUser                    = 45  // User '%s' is not found
	ErrUserExists                    = 46  // User '%s' already exists
	ErrPasswordMismatch              = 47  // Incorrect password supplied for user '%s'
	ErrUnknownRequestType            = 48  // Unknown request type %u
	ErrUnknownSchemaObject           = 49  // Unknown object type '%s'
	ErrCreateFunction                = 50  // Failed to create function '%s': %s
	ErrNoSuchFunction                = 51  // Function '%s' does not exist
	ErrFunctionExists                = 52  // Function '%s' already exists
	ErrFunctionAccessDenied          = 53  // %s access denied for user '%s' to function '%s'
	ErrFunctionMax                   = 54  // A limit on the total number of functions has been reached: %u
	ErrSpaceAccessDenied             = 55  // %s access denied for user '%s' to space '%s'
	ErrUserMax                       = 56  // A limit on the total number of users has been reached: %u
	ErrNoSuchEngine                  = 57  // Space engine '%s' does not exist
	ErrReloadCfg                     = 58  // Can't set option '%s' dynamically
	ErrCfg                           = 59  // Incorrect value for option '%s': %s
	ErrSophia                        = 60  // %s
	ErrLocalServerIsNotActive        = 61  // Local server is not active
	ErrUnknownServer                 = 62  // Server %s is not registered with the cluster
	ErrClusterIdMismatch             = 63  // Cluster id of the replica %s doesn't match cluster id of the master %s
	ErrInvalidUUID                   = 64  // Invalid UUID: %s
	ErrClusterIdIsRo                 = 65  // Can't reset cluster id: it is already assigned
	ErrReserved66                    = 66  // Reserved66
	ErrServerIdIsReserved            = 67  // Can't initialize server id with a reserved value %u
	ErrInvalidOrder                  = 68  // Invalid LSN order for server %u: previous LSN = %llu, new lsn = %llu
	ErrMissingRequestField           = 69  // Missing mandatory field '%s' in request
	ErrIdentifier                    = 70  // Invalid identifier '%s' (expected letters, digits or an underscore)
	ErrDropFunction                  = 71  // Can't drop function %u: %s
	ErrIteratorType                  = 72  // Unknown iterator type '%s'
	ErrReplicaMax                    = 73  // Replica count limit reached: %u
	ErrInvalidXlog                   = 74  // Failed to read xlog: %lld
	ErrInvalidXlogName               = 75  // Invalid xlog name: expected %lld got %lld
	ErrInvalidXlogOrder              = 76  // Invalid xlog order: %lld and %lld
	ErrNoConnection                  = 77  // Connection is not established
	ErrTimeout                       = 78  // Timeout exceeded
	ErrActiveTransaction             = 79  // Operation is not permitted when there is an active transaction
	ErrNoActiveTransaction           = 80  // Operation is not permitted when there is no active transaction
	ErrCrossEngineTransaction        = 81  // A multi-statement transaction can not use multiple storage engines
	ErrNoSuchRole                    = 82  // Role '%s' is not found
	ErrRoleExists                    = 83  // Role '%s' already exists
	ErrCreateRole                    = 84  // Failed to create role '%s': %s
	ErrIndexExists                   = 85  // Index '%s' already exists
	ErrTupleRefOverflow              = 86  // Tuple reference counter overflow
	ErrRoleLoop                      = 87  // Granting role '%s' to role '%s' would create a loop
	ErrGrant                         = 88  // Incorrect grant arguments: %s
	ErrPrivGranted                   = 89  // User '%s' already has %s access on %s '%s'
	ErrRoleGranted                   = 90  // User '%s' already has role '%s'
	ErrPrivNotGranted                = 91  // User '%s' does not have %s access on %s '%s'
	ErrRoleNotGranted                = 92  // User '%s' does not have role '%s'
	ErrMissingSnapshot               = 93  // Can't find snapshot
	ErrCantUpdatePrimaryKey          = 94  // Attempt to modify a tuple field which is part of index '%s' in space '%s'
	ErrUpdateIntegerOverflow         = 95  // Integer overflow when performing '%c' operation on field %u
	ErrGuestUserPassword             = 96  // Setting password for guest user has no effect
	ErrTransactionConflict           = 97  // Transaction has been aborted by conflict
	ErrUnsupportedRolePriv           = 98  // Unsupported role privilege '%s'
	ErrLoadFunction                  = 99  // Failed to dynamically load function '%s': %s
	ErrFunctionLanguage              = 100 // Unsupported language '%s' specified for function '%s'
	ErrRtreeRect                     = 101 // RTree: %s must be an array with %u (point) or %u (rectangle/box) numeric coordinates
	ErrProcC                         = 102 // ???
	ErrUnknownRtreeIndexDistanceType = 103 //Unknown RTREE index distance type %s
	ErrProtocol                      = 104 // %s
	ErrUpsertUniqueSecondaryKey      = 105 // Space %s has a unique secondary index and does not support UPSERT
	ErrWrongIndexRecord              = 106 // Wrong record in _index space: got {%s}, expected {%s}
	ErrWrongIndexParts               = 107 // Wrong index parts (field %u): %s; expected field1 id (number), field1 type (string), ...
	ErrWrongIndexOptions             = 108 // Wrong index options (field %u): %s
	ErrWrongSchemaVaersion           = 109 // Wrong schema version, current: %d, in request: %u
	ErrSlabAllocMax                  = 110 // Failed to allocate %u bytes for tuple in the slab allocator: tuple is too large. Check 'slab_alloc_maximal' configuration option.
)
const (
	ER_UNKNOWN uint32 = iota
	ER_ILLEGAL_PARAMS
	ER_MEMORY_ISSUE
	ER_TUPLE_FOUND
	ER_TUPLE_NOT_FOUND
	ER_UNSUPPORTED
	ER_NONMASTER
	ER_READONLY
	ER_INJECTION
	ER_CREATE_SPACE
	ER_SPACE_EXISTS
	ER_DROP_SPACE
	ER_ALTER_SPACE
	ER_INDEX_TYPE
	ER_MODIFY_INDEX
	ER_LAST_DROP
	ER_TUPLE_FORMAT_LIMIT
	ER_DROP_PRIMARY_KEY
	ER_KEY_PART_TYPE
	ER_EXACT_MATCH
	ER_INVALID_MSGPACK
	ER_PROC_RET
	ER_TUPLE_NOT_ARRAY
	ER_FIELD_TYPE
	ER_INDEX_PART_TYPE_MISMATCH
	ER_UPDATE_SPLICE
	ER_UPDATE_ARG_TYPE
	ER_FORMAT_MISMATCH_INDEX_PART
	ER_UNKNOWN_UPDATE_OP
	ER_UPDATE_FIELD
	ER_FUNCTION_TX_ACTIVE
	ER_KEY_PART_COUNT
	ER_PROC_LUA
	ER_NO_SUCH_PROC
	ER_NO_SUCH_TRIGGER
	ER_NO_SUCH_INDEX_ID
	ER_NO_SUCH_SPACE
	ER_NO_SUCH_FIELD_NO
	ER_EXACT_FIELD_COUNT
	ER_FIELD_MISSING
	ER_WAL_IO
	ER_MORE_THAN_ONE_TUPLE
	ER_ACCESS_DENIED
	ER_CREATE_USER
	ER_DROP_USER
	ER_NO_SUCH_USER
	ER_USER_EXISTS
	ER_PASSWORD_MISMATCH
	ER_UNKNOWN_REQUEST_TYPE
	ER_UNKNOWN_SCHEMA_OBJECT
	ER_CREATE_FUNCTION
	ER_NO_SUCH_FUNCTION
	ER_FUNCTION_EXISTS
	ER_BEFORE_REPLACE_RET
	ER_MULTISTATEMENT_TRANSACTION
	ER_TRIGGER_EXISTS
	ER_USER_MAX
	ER_NO_SUCH_ENGINE
	ER_RELOAD_CFG
	ER_CFG
	ER_SAVEPOINT_EMPTY_TX
	ER_NO_SUCH_SAVEPOINT
	ER_UNKNOWN_REPLICA
	ER_REPLICASET_UUID_MISMATCH
	ER_INVALID_UUID
	ER_REPLICASET_UUID_IS_RO
	ER_INSTANCE_UUID_MISMATCH
	ER_REPLICA_ID_IS_RESERVED
	ER_INVALID_ORDER
	ER_MISSING_REQUEST_FIELD
	ER_IDENTIFIER
	ER_DROP_FUNCTION
	ER_ITERATOR_TYPE
	ER_REPLICA_MAX
	ER_INVALID_XLOG
	ER_INVALID_XLOG_NAME
	ER_INVALID_XLOG_ORDER
	ER_NO_CONNECTION
	ER_TIMEOUT
	ER_ACTIVE_TRANSACTION
	ER_CURSOR_NO_TRANSACTION
	ER_CROSS_ENGINE_TRANSACTION
	ER_NO_SUCH_ROLE
	ER_ROLE_EXISTS
	ER_CREATE_ROLE
	ER_INDEX_EXISTS
	ER_SESSION_CLOSED
	ER_ROLE_LOOP
	ER_GRANT
	ER_PRIV_GRANTED
	ER_ROLE_GRANTED
	ER_PRIV_NOT_GRANTED
	ER_ROLE_NOT_GRANTED
	ER_MISSING_SNAPSHOT
	ER_CANT_UPDATE_PRIMARY_KEY
	ER_UPDATE_INTEGER_OVERFLOW
	ER_GUEST_USER_PASSWORD
	ER_TRANSACTION_CONFLICT
	ER_UNSUPPORTED_PRIV
	ER_LOAD_FUNCTION
	ER_FUNCTION_LANGUAGE
	ER_RTREE_RECT
	ER_PROC_C
	ER_UNKNOWN_RTREE_INDEX_DISTANCE_TYPE
	ER_PROTOCOL
	ER_UPSERT_UNIQUE_SECONDARY_KEY
	ER_WRONG_INDEX_RECORD
	ER_WRONG_INDEX_PARTS
	ER_WRONG_INDEX_OPTIONS
	ER_WRONG_SCHEMA_VERSION
	ER_MEMTX_MAX_TUPLE_SIZE
	ER_WRONG_SPACE_OPTIONS
	ER_UNSUPPORTED_INDEX_FEATURE
	ER_VIEW_IS_RO
	ER_NO_TRANSACTION
	ER_SYSTEM
	ER_LOADING
	ER_CONNECTION_TO_SELF
	ER_KEY_PART_IS_TOO_LONG
	ER_COMPRESSION
	ER_CHECKPOINT_IN_PROGRESS
	ER_SUB_STMT_MAX
	ER_COMMIT_IN_SUB_STMT
	ER_ROLLBACK_IN_SUB_STMT
	ER_DECOMPRESSION
	ER_INVALID_XLOG_TYPE
	ER_ALREADY_RUNNING
	ER_INDEX_FIELD_COUNT_LIMIT
	ER_LOCAL_INSTANCE_ID_IS_READ_ONLY
	ER_BACKUP_IN_PROGRESS
	ER_READ_VIEW_ABORTED
	ER_INVALID_INDEX_FILE
	ER_INVALID_RUN_FILE
	ER_INVALID_VYLOG_FILE
	ER_CASCADE_ROLLBACK
	ER_VY_QUOTA_TIMEOUT
	ER_PARTIAL_KEY
	ER_TRUNCATE_SYSTEM_SPACE
	ER_LOAD_MODULE
	ER_VINYL_MAX_TUPLE_SIZE
	ER_WRONG_DD_VERSION
	ER_WRONG_SPACE_FORMAT
	ER_CREATE_SEQUENCE
	ER_ALTER_SEQUENCE
	ER_DROP_SEQUENCE
	ER_NO_SUCH_SEQUENCE
	ER_SEQUENCE_EXISTS
	ER_SEQUENCE_OVERFLOW
	ER_NO_SUCH_INDEX_NAME
	ER_SPACE_FIELD_IS_DUPLICATE
	ER_CANT_CREATE_COLLATION
	ER_WRONG_COLLATION_OPTIONS
	ER_NULLABLE_PRIMARY
	ER_NO_SUCH_FIELD_NAME_IN_SPACE
	ER_TRANSACTION_YIELD
	ER_NO_SUCH_GROUP
	ER_SQL_BIND_VALUE
	ER_SQL_BIND_TYPE
	ER_SQL_BIND_PARAMETER_MAX
	ER_SQL_EXECUTE
	ER_UPDATE_DECIMAL_OVERFLOW
	ER_SQL_BIND_NOT_FOUND
	ER_ACTION_MISMATCH
	ER_VIEW_MISSING_SQL
	ER_FOREIGN_KEY_CONSTRAINT
	ER_NO_SUCH_MODULE
	ER_NO_SUCH_COLLATION
	ER_CREATE_FK_CONSTRAINT
	ER_DROP_FK_CONSTRAINT
	ER_NO_SUCH_CONSTRAINT
	ER_CONSTRAINT_EXISTS
	ER_SQL_TYPE_MISMATCH
	ER_ROWID_OVERFLOW
	ER_DROP_COLLATION
	ER_ILLEGAL_COLLATION_MIX
	ER_SQL_NO_SUCH_PRAGMA
	ER_SQL_CANT_RESOLVE_FIELD
	ER_INDEX_EXISTS_IN_SPACE
	ER_INCONSISTENT_TYPES
	ER_SQL_SYNTAX_WITH_POS
	ER_SQL_STACK_OVERFLOW
	ER_SQL_SELECT_WILDCARD
	ER_SQL_STATEMENT_EMPTY
	ER_SQL_KEYWORD_IS_RESERVED
	ER_SQL_SYNTAX_NEAR_TOKEN
	ER_SQL_UNKNOWN_TOKEN
	ER_SQL_PARSER_GENERIC
	ER_SQL_ANALYZE_ARGUMENT
	ER_SQL_COLUMN_COUNT_MAX
	ER_HEX_LITERAL_MAX
	ER_INT_LITERAL_MAX
	ER_SQL_PARSER_LIMIT
	ER_INDEX_DEF_UNSUPPORTED
	ER_CK_DEF_UNSUPPORTED
	ER_MULTIKEY_INDEX_MISMATCH
	ER_CREATE_CK_CONSTRAINT
	ER_CK_CONSTRAINT_FAILED
	ER_SQL_COLUMN_COUNT
	ER_FUNC_INDEX_FUNC
	ER_FUNC_INDEX_FORMAT
	ER_FUNC_INDEX_PARTS
	ER_NO_SUCH_FIELD_NAME
	ER_FUNC_WRONG_ARG_COUNT
	ER_BOOTSTRAP_READONLY
	ER_SQL_FUNC_WRONG_RET_COUNT
	ER_FUNC_INVALID_RETURN_TYPE
	ER_SQL_PARSER_GENERIC_WITH_POS
	ER_REPLICA_NOT_ANON
	ER_CANNOT_REGISTER
	ER_SESSION_SETTING_INVALID_VALUE
	ER_SQL_PREPARE
	ER_WRONG_QUERY_ID
	ER_SEQUENCE_NOT_STARTED
	ER_NO_SUCH_SESSION_SETTING
	ER_UNCOMMITTED_FOREIGN_SYNC_TXNS
	ER_SYNC_MASTER_MISMATCH
	ER_SYNC_QUORUM_TIMEOUT
	ER_SYNC_ROLLBACK
	ER_TUPLE_METADATA_IS_TOO_BIG
	ER_XLOG_GAP
	ER_TOO_EARLY_SUBSCRIBE
	ER_SQL_CANT_ADD_AUTOINC
	ER_QUORUM_WAIT
	ER_INTERFERING_PROMOTE
	ER_ELECTION_DISABLED
	ER_TXN_ROLLBACK
	ER_NOT_LEADER
	ER_SYNC_QUEUE_UNCLAIMED
	ER_SYNC_QUEUE_FOREIGN
	ER_UNABLE_TO_PROCESS_IN_STREAM
	ER_UNABLE_TO_PROCESS_OUT_OF_STREAM
	ER_TRANSACTION_TIMEOUT
	ER_ACTIVE_TIMER
)
