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

func TestBox_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	info, err := box.By(conn).Info()
	require.NoError(t, err)

	// check all fields run correctly
	_, err = uuid.Parse(info.UUID)
	require.NoErrorf(t, err, "validate instance uuid is valid")

	require.NotEmpty(t, info.Version)
	// check that pid parsed correctly
	require.NotEqual(t, info.PID, 0)

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
