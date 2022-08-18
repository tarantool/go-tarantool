package connection_pool

/*
Default mode for each request table:

	  Request   Default mode
	---------- --------------
	| call    | no default  |
	| eval    | no default  |
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

type Role uint32

// master/replica role
const (
	unknown = iota
	master
	replica
)

type State uint32

// pool state
const (
	connConnected = iota
	connClosed
)
