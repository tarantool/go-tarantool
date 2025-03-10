package test_helpers

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
)

// ConnectWithValidation tries to connect to a Tarantool instance.
// It returns a valid connection if it is successful, otherwise finishes a test
// with an error.
func ConnectWithValidation(t testing.TB,
	dialer tarantool.Dialer,
	opts tarantool.Opts) *tarantool.Connection {
	t.Helper()

	ctx, cancel := GetConnectContext()
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, opts)
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
	err := Retry(func(arg interface{}) error {
		conn := arg.(*tarantool.Connection)
		connected := conn.ConnectedNow()
		if !connected {
			return fmt.Errorf("not connected")
		}
		return nil
	}, conn, int(retries), timeout)

	return err == nil
}

func SkipIfSQLUnsupported(t testing.TB) {
	t.Helper()

	// Tarantool supports SQL since version 2.0.0
	isLess, err := IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	}
	if isLess {
		t.Skip()
	}
}

// SkipIfLess skips test run if Tarantool version is less than expected.
func SkipIfLess(t *testing.T, reason string, major, minor, patch uint64) {
	t.Helper()

	isLess, err := IsTarantoolVersionLess(major, minor, patch)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	}

	if isLess {
		t.Skipf("Skipping test for Tarantool %s", reason)
	}
}

// SkipIfGreaterOrEqual skips test run if Tarantool version is greater or equal
// than expected.
func SkipIfGreaterOrEqual(t *testing.T, reason string, major, minor, patch uint64) {
	t.Helper()

	isLess, err := IsTarantoolVersionLess(major, minor, patch)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	}

	if !isLess {
		t.Skipf("Skipping test for Tarantool %s", reason)
	}
}

// SkipIfFeatureUnsupported skips test run if Tarantool does not yet support a feature.
func SkipIfFeatureUnsupported(t *testing.T, feature string, major, minor, patch uint64) {
	t.Helper()

	SkipIfLess(t, fmt.Sprintf("without %s support", feature), major, minor, patch)
}

// SkipIfFeatureSupported skips test run if Tarantool supports a feature.
// Helper if useful when we want to test if everything is alright
// on older versions.
func SkipIfFeatureSupported(t *testing.T, feature string, major, minor, patch uint64) {
	t.Helper()

	SkipIfGreaterOrEqual(t, fmt.Sprintf("with %s support", feature), major, minor, patch)
}

// SkipIfFeatureDropped skips test run if Tarantool had dropped
// support of a feature.
func SkipIfFeatureDropped(t *testing.T, feature string, major, minor, patch uint64) {
	t.Helper()

	SkipIfGreaterOrEqual(t, fmt.Sprintf("with %s support dropped", feature), major, minor, patch)
}

// SkipOfStreamsUnsupported skips test run if Tarantool without streams
// support is used.
func SkipIfStreamsUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "streams", 2, 10, 0)
}

// SkipOfStreamsUnsupported skips test run if Tarantool without watchers
// support is used.
func SkipIfWatchersUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "watchers", 2, 10, 0)
}

// SkipIfWatchersSupported skips test run if Tarantool with watchers
// support is used.
func SkipIfWatchersSupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureSupported(t, "watchers", 2, 10, 0)
}

// SkipIfIdUnsupported skips test run if Tarantool without
// IPROTO_ID support is used.
func SkipIfIdUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "id requests", 2, 10, 0)
}

// SkipIfIdSupported skips test run if Tarantool with
// IPROTO_ID support is used. Skip is useful for tests validating
// that protocol info is processed as expected even for pre-IPROTO_ID instances.
func SkipIfIdSupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureSupported(t, "id requests", 2, 10, 0)
}

// SkipIfErrorExtendedInfoUnsupported skips test run if Tarantool without
// IPROTO_ERROR (0x52) support is used.
func SkipIfErrorExtendedInfoUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "error extended info", 2, 4, 1)
}

// SkipIfErrorMessagePackTypeUnsupported skips test run if Tarantool without
// MP_ERROR type over iproto support is used.
func SkipIfErrorMessagePackTypeUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "error type in MessagePack", 2, 10, 0)
}

// SkipIfPaginationUnsupported skips test run if Tarantool without
// pagination is used.
func SkipIfPaginationUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "pagination", 2, 11, 0)
}

// SkipIfWatchOnceUnsupported skips test run if Tarantool without WatchOnce
// request type is used.
func SkipIfWatchOnceUnsupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "watch once", 3, 0, 0)
}

// SkipIfWatchOnceSupported skips test run if Tarantool with WatchOnce
// request type is used.
func SkipIfWatchOnceSupported(t *testing.T) {
	t.Helper()

	SkipIfFeatureSupported(t, "watch once", 3, 0, 0)
}

// SkipIfCrudSpliceBroken skips test run if splice operation is broken
// on the crud side.
// https://github.com/tarantool/crud/issues/397
func SkipIfCrudSpliceBroken(t *testing.T) {
	t.Helper()

	SkipIfFeatureUnsupported(t, "crud update splice", 2, 0, 0)
}

// IsTcsSupported checks if Tarantool supports centralized storage.
// Tarantool supports centralized storage with Enterprise since 3.3.0 version.
func IsTcsSupported() (bool, error) {

	if isEe, err := IsTarantoolEE(); !isEe || err != nil {
		return false, err
	}
	if isLess, err := IsTarantoolVersionLess(3, 3, 0); isLess || err != nil {
		return false, err
	}
	return true, nil
}

// SkipIfTCSUnsupported skips test if no centralized storage support.
func SkipIfTcsUnsupported(t testing.TB) {
	t.Helper()

	ok, err := IsTcsSupported()
	if err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	}
	if !ok {
		t.Skip("not found Tarantool EE 3.3+")
	}
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
		require.Subset(t, actual.Fields, expected.Fields)

		if expected.Prev != nil {
			// Stack depth is the same
			expected = *expected.Prev
			actual = *actual.Prev
		} else {
			break
		}
	}
}
