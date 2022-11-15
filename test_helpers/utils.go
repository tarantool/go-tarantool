package test_helpers

import (
	"testing"
	"time"

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

func DeleteRecordByKey(t *testing.T, conn tarantool.Connector,
	space interface{}, index interface{}, key []interface{}) {
	t.Helper()

	req := tarantool.NewDeleteRequest(space).
		Index(index).
		Key(key)
	resp, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
}

// WaitUntilReconnected waits until connection is reestablished.
// Returns false in case of connection is not in the connected state
// after specified retries count, true otherwise.
func WaitUntilReconnected(conn *tarantool.Connection, retries uint, timeout time.Duration) bool {
	for i := uint(0); ; i++ {
		connected := conn.ConnectedNow()
		if connected {
			return true
		}

		if i == retries {
			break
		}

		time.Sleep(timeout)
	}

	return false
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

func SkipIfStreamsUnsupported(t *testing.T) {
	t.Helper()

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, err := IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if isLess {
		t.Skip("Skipping test for Tarantool without streams support")
	}
}

// SkipIfIdUnsupported skips test run if Tarantool without
// IPROTO_ID support is used.
func SkipIfIdUnsupported(t *testing.T) {
	t.Helper()

	// Tarantool supports Id requests since version 2.10.0
	isLess, err := IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if isLess {
		t.Skip("Skipping test for Tarantool without id requests support")
	}
}

// SkipIfIdSupported skips test run if Tarantool with
// IPROTO_ID support is used. Skip is useful for tests validating
// that protocol info is processed as expected even for pre-IPROTO_ID instances.
func SkipIfIdSupported(t *testing.T) {
	t.Helper()

	// Tarantool supports Id requests since version 2.10.0
	isLess, err := IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if !isLess {
		t.Skip("Skipping test for Tarantool with non-zero protocol version and features")
	}
}
