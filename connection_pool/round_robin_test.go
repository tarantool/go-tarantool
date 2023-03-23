package connection_pool_test

import (
	"testing"

	"github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/connection_pool"
)

const (
	validAddr1 = "x"
	validAddr2 = "y"
)

func TestRoundRobinAddDelete(t *testing.T) {
	rr := NewEmptyRoundRobin(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConn(addr, conns[i])
	}

	for i, addr := range addrs {
		if conn := rr.DeleteConnByAddr(addr); conn != conns[i] {
			t.Errorf("Unexpected connection on address %s", addr)
		}
	}
	if !rr.IsEmpty() {
		t.Errorf("RoundRobin does not empty")
	}
}

func TestRoundRobinAddDuplicateDelete(t *testing.T) {
	rr := NewEmptyRoundRobin(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	rr.AddConn(validAddr1, conn1)
	rr.AddConn(validAddr1, conn2)

	if rr.DeleteConnByAddr(validAddr1) != conn2 {
		t.Errorf("Unexpected deleted connection")
	}
	if !rr.IsEmpty() {
		t.Errorf("RoundRobin does not empty")
	}
	if rr.DeleteConnByAddr(validAddr1) != nil {
		t.Errorf("Unexpected value after second deletion")
	}
}

func TestRoundRobinGetNextConnection(t *testing.T) {
	rr := NewEmptyRoundRobin(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConn(addr, conns[i])
	}

	expectedConns := []*tarantool.Connection{conns[0], conns[1], conns[0], conns[1]}
	for i, expected := range expectedConns {
		if rr.GetNextConnection() != expected {
			t.Errorf("Unexpected connection on %d call", i)
		}
	}
}

func TestRoundRobinStrategy_GetConnections(t *testing.T) {
	rr := NewEmptyRoundRobin(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConn(addr, conns[i])
	}

	rr.GetConnections()[1] = conns[0] // GetConnections() returns a copy.
	rrConns := rr.GetConnections()
	for i, expected := range conns {
		if expected != rrConns[i] {
			t.Errorf("Unexpected connection on %d call", i)
		}
	}
}
