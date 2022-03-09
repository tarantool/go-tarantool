package connection_pool

type Mode uint32
type Role uint32
type State uint32

/*
Mode parameter:

- ANY (use any instance) - the request can be executed on any instance (master or replica).

- RW (writeable instance (master)) - the request can only be executed on master.

- RO (read only instance (replica)) - the request can only be executed on replica.

- PREFER_RO (prefer read only instance (replica)) - if there is one, otherwise fallback to a writeable one (master).

- PREFER_RW (prefer write only instance (master)) - if there is one, otherwise fallback to a read only one (replica).

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
const (
	ANY = iota
	RW
	RO
	PreferRW
	PreferRO
)

// master/replica role
const (
	unknown = iota
	master
	replica
)

// pool state
const (
	connConnected = iota
	connClosed
)
