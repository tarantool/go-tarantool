package tarantool_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	DEBUG = false
)

func setDebug() {
	DEBUG = true
}

func logMsgpackAsJsonConvert(t *testing.T, data []byte) {
	var decodedMsgpack map[int]interface{}

	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	require.NoError(t, decoder.Decode(&decodedMsgpack))

	var encodedJson []byte
	if DEBUG {
		decodedConvertedMsgpack := map[string]interface{}{}
		for k, v := range decodedMsgpack {
			decodedConvertedMsgpack[fmt.Sprintf("%s[%d]", iproto.Key(k).String(), k)] = v
		}

		var err error
		encodedJson, err = json.MarshalIndent(decodedConvertedMsgpack, "", "  ")
		require.NoError(t, err, "failed to convert msgpack to json")
	} else {
		var err error
		encodedJson, err = json.MarshalIndent(decodedMsgpack, "", "  ")
		require.NoError(t, err)
	}

	for _, line := range bytes.Split(encodedJson, []byte("\n")) {
		t.Log(string(line))
	}
}

func compareGoldenMsgpack(t *testing.T, name string, data []byte) bool {
	t.Helper()

	testContent, err := os.ReadFile(name)
	require.NoError(t, err, "failed to read golden file", name)

	if assert.Equal(t, testContent, data, "golden file content is not equal to actual") {
		if DEBUG {
			logMsgpackAsJsonConvert(t, data)
		}
		return true
	}

	t.Logf("expected:\n")
	logMsgpackAsJsonConvert(t, testContent)
	t.Logf("actual:\n")
	logMsgpackAsJsonConvert(t, data)

	return false
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

func executeGoldenTest(t *testing.T, name string, f func(t *testing.T, enc *msgpack.Encoder)) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		var out bytes.Buffer
		encoder := msgpack.NewEncoder(&out)

		switch f(t, encoder); {
		case !fileExists(name):
			t.Logf("writing golden file %s", name)
			err := os.WriteFile(name, out.Bytes(), 0644)
			assert.NoError(t, err, "failed to write golden file", name)
		case !compareGoldenMsgpack(t, name, out.Bytes()):
			// ???
		}
	})
}

func TestGolden(t *testing.T) {
	setDebug()

	var pathPrefix = "testdata/requests"

	var testCases = []struct {
		name   string
		getter func(t *testing.T, enc *msgpack.Encoder)
	}{
		{
			name: "commit-raw.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				commit := tarantool.NewCommitRequest()
				require.NoError(t, commit.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "commit-with-sync.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				commit := tarantool.NewCommitRequest().IsSync(true)
				require.NoError(t, commit.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "commit-with-sync-false.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				commit := tarantool.NewCommitRequest().IsSync(false)
				require.NoError(t, commit.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "begin.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				begin := tarantool.NewBeginRequest()
				require.NoError(t, begin.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "begin-with-txn-isolation.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				begin := tarantool.NewBeginRequest().
					TxnIsolation(tarantool.ReadCommittedLevel)
				require.NoError(t, begin.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "begin-with-txn-isolation-is-sync.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				begin := tarantool.NewBeginRequest().
					TxnIsolation(tarantool.ReadCommittedLevel).
					IsSync(true)
				require.NoError(t, begin.Body(nil, enc), "failed to encode request")
			},
		},
		{
			name: "begin-with-txn-isolation-is-sync-timeout.msgpack",
			getter: func(t *testing.T, enc *msgpack.Encoder) {
				begin := tarantool.NewBeginRequest().
					TxnIsolation(tarantool.ReadCommittedLevel).
					IsSync(true).
					Timeout(2 * time.Second)
				require.NoError(t, begin.Body(nil, enc), "failed to encode request")
			},
		},
	}

	for _, tc := range testCases {
		executeGoldenTest(t, path.Join(pathPrefix, tc.name), tc.getter)
	}
}
