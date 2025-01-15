package box_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var server = "127.0.0.1:3013"
var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

func validateInfo(t testing.TB, info box.Info) {
	var err error

	// Check all fields run correctly.
	_, err = uuid.Parse(info.UUID)
	require.NoErrorf(t, err, "validate instance uuid is valid")

	require.NotEmpty(t, info.Version)
	// Check that pid parsed correctly.
	require.NotEqual(t, info.PID, 0)

	// Check replication is parsed correctly.
	require.NotEmpty(t, info.Replication)

	// Check one replica uuid is equal system uuid.
	require.Equal(t, info.UUID, info.Replication[1].UUID)
}

func TestBox_Sugar_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	info, err := box.New(conn).Info()
	require.NoError(t, err)

	validateInfo(t, info)
}

func TestBox_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	fut := conn.Do(box.NewInfoRequest())
	require.NotNil(t, fut)

	resp := &box.InfoResponse{}
	err = fut.GetTyped(resp)
	require.NoError(t, err)

	validateInfo(t, resp.Info)
}

func runTestMain(m *testing.M) int {
	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
