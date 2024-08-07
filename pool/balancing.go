package pool

import "github.com/tarantool/go-tarantool/v2"

type BalancingMethod = func(int) BalancingPool

type BalancingPool interface {
	IsEmpty() bool
	GetConnection(string) *tarantool.Connection
	DeleteConnection(string) *tarantool.Connection
	AddConnection(id string, conn *tarantool.Connection)
	GetNextConnection() *tarantool.Connection
	GetConnections() map[string]*tarantool.Connection
}
