package box

import (
	"github.com/tarantool/go-tarantool/v2"
)

// Box defines an interface for interacting with a Tarantool instance.
// It includes the Info method, which retrieves instance information.
type Box interface {
	Info() (Info, error) // Retrieves detailed information about the Tarantool instance.
}

// box is a concrete implementation of the Box interface.
// It holds a connection to the Tarantool instance via the Doer interface.
type box struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// By returns a new instance of the box structure, which implements the Box interface.
func By(conn tarantool.Doer) Box {
	return &box{
		conn: conn, // Assigns the provided Tarantool connection.
	}
}
