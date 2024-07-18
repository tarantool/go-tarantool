package pool

import "github.com/tarantool/go-tarantool/v2"

type BalancerFactory interface {
	Create(size int) BalancingPool
}

type BalancingPool interface {
	GetConnection(string) *tarantool.Connection
	DeleteConnection(string) *tarantool.Connection
	AddConnection(id string, conn *tarantool.Connection)
	GetNextConnection() *tarantool.Connection
	GetConnections() map[string]*tarantool.Connection
}

func IsEmpty(pool BalancingPool) bool {
	return len(pool.GetConnections()) == 0
}
