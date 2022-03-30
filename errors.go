package tarantool

import (
	"fmt"
)

// Error is wrapper around error returned by Tarantool.
type Error struct {
	Code uint32
	Msg  string
}

// Error converts an Error to a string.
func (tnterr Error) Error() string {
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

// Temporary returns true if next attempt to perform request may succeeed.
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
