package test_helpers

import (
	"testing"

	"github.com/tarantool/go-tarantool"
)

// ConnectWithValidation tries to connect to a Tarantool instance.
// It returns a valid connection if it is successful, otherwise finishes a test
// with an error.
func ConnectWithValidation(t testing.TB,
	server string,
	opts tarantool.Opts) *tarantool.Connection {
	t.Helper()

	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if conn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	return conn
}

func SkipIfSQLUnsupported(t testing.TB) {
	t.Helper()

	// Tarantool supports SQL since version 2.0.0
	isLess, err := IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}
	if isLess {
		t.Skip()
	}
}
