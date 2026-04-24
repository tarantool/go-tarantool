//go:generate go tool stringer -type Role -linecomment
package pool

// Mode is a request mode for a connection pool.
//
// Default mode for each request table:
//
//	  Request   Default mode
//	---------- --------------
//	| call    | no default  |
//	| eval    | no default  |
//	| execute | no default  |
//	| ping    | no default  |
//	| insert  | ModeRW      |
//	| delete  | ModeRW      |
//	| replace | ModeRW      |
//	| update  | ModeRW      |
//	| upsert  | ModeRW      |
//	| select  | ModeAny     |
//	| get     | ModeAny     |
type Mode uint32

const (
	ModeAny      Mode = iota // The request can be executed on any instance (master or replica).
	ModeRW                   // The request can only be executed on master.
	ModeRO                   // The request can only be executed on replica.
	ModePreferRW             // If there is one, otherwise fallback to a writeable one (master).
	ModePreferRO             // If there is one, otherwise fallback to a read only one (replica).
)

// Role describes a role of an instance by its mode.
type Role uint32

const (
	// RoleUnknown - the connection pool was unable to detect the instance mode.
	RoleUnknown Role = iota // unknown
	// RoleMaster - the instance is in read-write mode.
	RoleMaster // master
	// RoleReplica - the instance is in read-only mode.
	RoleReplica // replica
)
