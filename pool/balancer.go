package pool

import "github.com/tarantool/go-tarantool/v2"

// BalancerFactory is an interface for creating a balancing pool of connections.
type BalancerFactory interface {
	// Create initializes a new BalancingPool with the specified size.
	// The size parameter indicates the intended number of connections to manage within the pool.
	Create(size int) BalancingPool
}

// BalancingPool represents a connection pool with load balancing.
type BalancingPool interface {
	// GetConnection returns the connection associated with the specified identifier.
	// If no connection with the given identifier is found, it returns nil.
	GetConnection(string) *tarantool.Connection

	// DeleteConnection removes the connection with the specified identifier from the pool
	// and returns the removed connection. If no connection is found, it returns nil.
	DeleteConnection(string) *tarantool.Connection

	// AddConnection adds a new connection to the pool under the specified identifier.
	// If a connection with that identifier already exists, the behavior may depend
	// on the implementation (e.g., it may overwrite the existing connection).
	AddConnection(id string, conn *tarantool.Connection)

	// GetNextConnection returns the next available connection from the pool.
	// The implementation may use load balancing algorithms to select the connection.
	GetNextConnection() *tarantool.Connection

	// GetConnections returns the current map of all connections in the pool,
	// where the key is the connection identifier and the value is a pointer to the connection object.
	GetConnections() map[string]*tarantool.Connection
}

func IsEmpty(pool BalancingPool) bool {
	return len(pool.GetConnections()) == 0
}
