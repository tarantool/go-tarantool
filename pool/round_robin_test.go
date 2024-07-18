package pool

import (
	"testing"

	"github.com/tarantool/go-tarantool/v2"
)

const (
	validAddr1 = "x"
	validAddr2 = "y"
)

func TestRoundRobinAddDelete(t *testing.T) {
	rr := NewRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	for i, addr := range addrs {
		if conn := rr.DeleteConnection(addr); conn != conns[i] {
			t.Errorf("Unexpected connection on address %s", addr)
		}
	}
	if !IsEmpty(rr) {
		t.Errorf("RoundRobin does not empty")
	}
}

func TestRoundRobinAddDuplicateDelete(t *testing.T) {
	rr := NewRoundRobinStrategy(10)

	conn1 := &tarantool.Connection{}
	conn2 := &tarantool.Connection{}

	rr.AddConnection(validAddr1, conn1)
	rr.AddConnection(validAddr1, conn2)

	if rr.DeleteConnection(validAddr1) != conn2 {
		t.Errorf("Unexpected deleted connection")
	}
	if !IsEmpty(rr) {
		t.Errorf("RoundRobin does not empty")
	}
	if rr.DeleteConnection(validAddr1) != nil {
		t.Errorf("Unexpected value after second deletion")
	}
}

func TestRoundRobinGetNextConnection(t *testing.T) {
	rr := NewRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	expectedConns := []*tarantool.Connection{conns[0], conns[1], conns[0], conns[1]}
	for i, expected := range expectedConns {
		if rr.GetNextConnection() != expected {
			t.Errorf("Unexpected connection on %d call", i)
		}
	}
}

func TestRoundRobinStrategy_GetConnections(t *testing.T) {
	rr := NewRoundRobinStrategy(10)

	addrs := []string{validAddr1, validAddr2}
	conns := []*tarantool.Connection{&tarantool.Connection{}, &tarantool.Connection{}}

	for i, addr := range addrs {
		rr.AddConnection(addr, conns[i])
	}

	rr.GetConnections()[validAddr2] = conns[0] // GetConnections() returns a copy.
	rrConns := rr.GetConnections()

	for i, addr := range addrs {
		if conns[i] != rrConns[addr] {
			t.Errorf("Unexpected connection on %s addr", addr)
		}
	}
}
