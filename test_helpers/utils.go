package test_helpers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func skipIfLess(t *testing.T, feature string, major, minor, patch uint64) {
	t.Helper()

	isLess, err := IsTarantoolVersionLess(major, minor, patch)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if isLess {
		t.Skipf("Skipping test for Tarantool without %s support", feature)
	}
}

func skipIfGreaterOrEqual(t *testing.T, feature string, major, minor, patch uint64) {
	t.Helper()

	isLess, err := IsTarantoolVersionLess(major, minor, patch)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if !isLess {
		t.Skipf("Skipping test for Tarantool with %s support", feature)
	}
}

// SkipOfStreamsUnsupported skips test run if Tarantool without streams
// support is used.
func SkipIfStreamsUnsupported(t *testing.T) {
	t.Helper()

	skipIfLess(t, "streams", 2, 10, 0)
}

// SkipOfStreamsUnsupported skips test run if Tarantool without watchers
// support is used.
func SkipIfWatchersUnsupported(t *testing.T) {
	t.Helper()

	skipIfLess(t, "watchers", 2, 10, 0)
}

// SkipIfWatchersSupported skips test run if Tarantool with watchers
// support is used.
func SkipIfWatchersSupported(t *testing.T) {
	t.Helper()

	skipIfGreaterOrEqual(t, "watchers", 2, 10, 0)
}

// SkipIfIdUnsupported skips test run if Tarantool without
// IPROTO_ID support is used.
func SkipIfIdUnsupported(t *testing.T) {
	t.Helper()

	skipIfLess(t, "id requests", 2, 10, 0)
}

// SkipIfIdSupported skips test run if Tarantool with
// IPROTO_ID support is used. Skip is useful for tests validating
// that protocol info is processed as expected even for pre-IPROTO_ID instances.
func SkipIfIdSupported(t *testing.T) {
	t.Helper()

	skipIfGreaterOrEqual(t, "id requests", 2, 10, 0)
}

// CheckEqualBoxErrors checks equivalence of tarantool.BoxError objects.
//
// Tarantool errors are not comparable by nature:
//
// tarantool> msgpack.decode(mp_error_repr) == msgpack.decode(mp_error_repr)
// ---
// - false
// ...
//
// Tarantool error file and line could differ even between
// different patches.
//
// So we check equivalence of all attributes except for Line and File.
// For Line and File, we check that they are filled with some non-default values
// (lines are counted starting with 1 and empty file path is not expected too).
func CheckEqualBoxErrors(t *testing.T, expected tarantool.BoxError, actual tarantool.BoxError) {
	t.Helper()

	require.Equalf(t, expected.Depth(), actual.Depth(), "Error stack depth is the same")

	for {
		require.Equal(t, expected.Type, actual.Type)
		require.Greater(t, len(expected.File), 0)
		require.Greater(t, expected.Line, uint64(0))
		require.Equal(t, expected.Msg, actual.Msg)
		require.Equal(t, expected.Errno, actual.Errno)
		require.Equal(t, expected.Code, actual.Code)
		require.Equal(t, expected.Fields, actual.Fields)

		if expected.Prev != nil {
			// Stack depth is the same
			expected = *expected.Prev
			actual = *actual.Prev
		} else {
			break
		}
	}
}

// SkipIfErrorExtendedInfoUnsupported skips test run if Tarantool without
// IPROTO_ERROR (0x52) support is used.
func SkipIfErrorExtendedInfoUnsupported(t *testing.T) {
	t.Helper()

	// Tarantool provides extended error info only since 2.4.1 version.
	isLess, err := IsTarantoolVersionLess(2, 4, 1)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if isLess {
		t.Skip("Skipping test for Tarantool without error extended info support")
	}
}

// SkipIfErrorExtendedInfoUnsupported skips test run if Tarantool without
// MP_ERROR type over iproto support is used.
func SkipIfErrorMessagePackTypeUnsupported(t *testing.T) {
	t.Helper()

	// Tarantool error type over MessagePack supported only since 2.10.0 version.
	isLess, err := IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}

	if isLess {
		t.Skip("Skipping test for Tarantool without support of error type over MessagePack")
		t.Skip("Skipping test for Tarantool without error extended info support")
	}
}
