package pool

import "github.com/tarantool/go-tarantool/v3"

// Strategy defines the interface for connection selection strategies.
// Strategies own connections directly and provide round-robin or other
// selection algorithms.
type Strategy interface {
	// Add adds a connection with the given ID to the strategy.
	Add(id string, conn *tarantool.Connection)

	// Remove removes a connection by ID.
	Remove(id string) *tarantool.Connection

	// Get returns a connection by ID.
	Get(id string) *tarantool.Connection

	// Next returns the next connection according to the strategy's algorithm.
	Next() *tarantool.Connection

	// Connections returns all connections managed by this strategy.
	Connections() map[string]*tarantool.Connection

	// Len returns the number of connections managed by this strategy.
	Len() int
}
