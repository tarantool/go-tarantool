package box

import (
	"errors"

	"github.com/tarantool/go-tarantool/v3"
)

// Box is a helper that wraps box.* requests.
// It holds a connection to the Tarantool instance via the Doer interface.
type Box struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// New returns a new instance of the box structure, which implements the Box interface.
func New(conn tarantool.Doer) (*Box, error) {
	if conn == nil {
		return nil, errors.New("tarantool connection cannot be nil")
	}

	return &Box{
		conn: conn, // Assigns the provided Tarantool connection.
	}, nil
}

// MustNew returns a new instance of the box structure, which implements the Box interface.
func MustNew(conn tarantool.Doer) *Box {
	b, err := New(conn)
	if err != nil {
		// Check if the provided Tarantool connection is nil, and if it is, panic with an error
		// message. panic early helps to catch and fix nil pointer issues in the code
		panic(err)
	}
	return b
}

// Schema returns a new Schema instance, providing access to schema-related operations.
// It uses the connection from the Box instance to communicate with Tarantool.
func (b *Box) Schema() *Schema {
	return newSchema(b.conn)
}

// Info retrieves the current information of the Tarantool instance.
// It calls the "box.info" function and parses the result into the Info structure.
func (b *Box) Info() (Info, error) {
	var infoResp InfoResponse

	// Call "box.info" to get instance information from Tarantool.
	fut := b.conn.Do(NewInfoRequest())

	// Parse the result into the Info structure.
	err := fut.GetTyped(&infoResp)
	if err != nil {
		return Info{}, err
	}

	// Return the parsed info and any potential error.
	return infoResp.Info, err
}
