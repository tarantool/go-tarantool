package arrow_test

import (
	"encoding/hex"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/arrow"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var isArrowSupported = false

var server = "127.0.0.1:3013"
var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}
var space = "testArrow"

var opts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

// TestInsert uses Arrow sequence from Tarantool's test.
// See: https://github.com/tarantool/tarantool/blob/d628b71bc537a75b69c253f45ec790462cf1a5cd/test/box-luatest/gh_10508_iproto_insert_arrow_test.lua#L56
func TestInsert_invalid(t *testing.T) {
	arrows := []struct {
		arrow    string
		expected iproto.Error
	}{
		{
			"",
			iproto.ER_INVALID_MSGPACK,
		},
		{
			"00",
			iproto.ER_INVALID_MSGPACK,
		},
		{
			"ffffffff70000000040000009effffff0400010004000000" +
				"b6ffffff0c00000004000000000000000100000004000000daffffff140000000202" +
				"000004000000f0ffffff4000000001000000610000000600080004000c0010000400" +
				"080009000c000c000c0000000400000008000a000c00040006000800ffffffff8800" +
				"0000040000008affffff0400030010000000080000000000000000000000acffffff" +
				"01000000000000003400000008000000000000000200000000000000000000000000" +
				"00000000000000000000000000000800000000000000000000000100000001000000" +
				"0000000000000000000000000a00140004000c0010000c0014000400060008000c00" +
				"00000000000000000000",
			iproto.ER_UNSUPPORTED,
		},
	}

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	for i, a := range arrows {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			data, err := hex.DecodeString(a.arrow)
			require.NoError(t, err)

			arr, err := arrow.MakeArrow(data)
			require.NoError(t, err)

			req := arrow.NewInsertRequest(space, arr)
			_, err = conn.Do(req).Get()
			ttErr := err.(tarantool.Error)

			require.Equal(t, a.expected, ttErr.Code)
		})
	}

}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(3, 3, 0)
	if err != nil {
		log.Fatalf("Failed to extract Tarantool version: %s", err)
	}
	isArrowSupported = !isLess

	if !isArrowSupported {
		log.Println("Skipping insert Arrow tests...")
		return 0
	}

	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config-memtx.lua",
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
