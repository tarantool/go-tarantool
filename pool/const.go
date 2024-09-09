//go:generate stringer -type Role -linecomment
package pool

/*
Default mode for each request table:

	  Request   Default mode
	---------- --------------
	| call    | no default  |
	| eval    | no default  |
	| execute | no default  |
	| ping    | no default  |
	| insert  | RW          |
	| delete  | RW          |
	| replace | RW          |
	| update  | RW          |
	| upsert  | RW          |
	| select  | ANY         |
	| get     | ANY         |
*/
type Mode uint32

const (
	ANY      Mode = iota // The request can be executed on any instance (master or replica).
	RW                   // The request can only be executed on master.
	RO                   // The request can only be executed on replica.
	PreferRW             // If there is one, otherwise fallback to a writeable one (master).
	PreferRO             // If there is one, otherwise fallback to a read only one (replica).
)

// Role describes a role of an instance by its mode.
type Role uint32

const (
	// UnknownRole - the connection pool was unable to detect the instance mode.
	UnknownRole Role = iota // unknown
	// MasterRole - the instance is in read-write mode.
	MasterRole // master
	// ReplicaRole - the instance is in read-only mode.
	ReplicaRole // replica
)
