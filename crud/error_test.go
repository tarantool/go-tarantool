package crud_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3/crud"
)

func TestErrorMany(t *testing.T) {
	errs := crud.ErrorMany{Errors: []crud.Error{
		{
			ClassName: "a",
			Str:       "msg 1",
		},
		{
			ClassName: "b",
			Str:       "msg 2",
		},
		{
			ClassName: "c",
			Str:       "msg 3",
		},
	}}

	require.Equal(t, "msg 1\nmsg 2\nmsg 3", errs.Error())
}

func TestErrorDecodeAtomicBatchFields(t *testing.T) {
	errMsg := "Operation #2 (insert on \"users\") failed: " +
		"Duplicate key exists in unique index 'primary' in space 'users'"

	sampleError := map[string]any{
		"class_name":      "AtomicBatchError",
		"err":             errMsg,
		"str":             "AtomicBatchError: " + errMsg,
		"operation_index": uint64(2),
		"unref_error": map[string]any{
			"class_name": "VShardError",
			"err":        "Failed to unref bucket",
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleError)
	require.NoError(t, err)

	var e crud.Error
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&e)
	require.NoError(t, err)
	require.Equal(t, "AtomicBatchError", e.ClassName)
	require.Equal(t, uint64(2), e.OperationIndex)
	require.Equal(t, map[string]any{
		"class_name": "VShardError",
		"err":        "Failed to unref bucket",
	}, e.UnrefError)
}
