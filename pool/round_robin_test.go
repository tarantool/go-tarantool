package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/tarantool/go-tarantool/v3"
)

const (
	validAddr1 = "x"
	validAddr2 = "y"
)

func TestRoundRobinAddDelete(t *testing.T) {
	rr := newRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	for i, addr := range addrs {
		if conn := rr.DeleteConnection(addr); conn != conns[i] {
			assert.Equal(t, conns[i], conn, "Unexpected connection on address %s", addr)
		}
	}
	assert.True(t, rr.IsEmpty(), "RoundRobin does not empty")
}

func TestRoundRobinAddDuplicateDelete(t *testing.T) {
	rr := newRoundRobinStrategy(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	rr.AddConnection(validAddr1, conn1)
	rr.AddConnection(validAddr1, conn2)

	assert.Equal(t, conn2, rr.DeleteConnection(validAddr1), "Unexpected deleted connection")
	assert.True(t, rr.IsEmpty(), "RoundRobin does not empty")
	assert.Nil(t, rr.DeleteConnection(validAddr1), "Unexpected value after second deletion")
}

func TestRoundRobinGetNextConnection(t *testing.T) {
	rr := newRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	expectedConns := []*tarantool.Connection{conns[0], conns[1], conns[0], conns[1]}
	for i, expected := range expectedConns {
		assert.Equal(t, expected, rr.GetNextConnection(), "Unexpected connection on %d call", i)
	}
}

func TestRoundRobinStrategy_GetConnections(t *testing.T) {
	rr := newRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	rr.GetConnections()[validAddr2] = conns[0] // GetConnections() returns a copy.
	rrConns := rr.GetConnections()

	for i, addr := range addrs {
		assert.Equal(t, conns[i], rrConns[addr], "Unexpected connection on %s addr", addr)
	}
}
