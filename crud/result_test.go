package crud_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3/crud"
)

func TestResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"rows": []any{"1", "2", "3"},
		},
		nil,
	}
	responses := []any{sampleCrudResponse, sampleCrudResponse}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(responses)
	require.NoError(t, err)

	var results []crud.Result
	decoder := msgpack.NewDecoder(b)
	err = decoder.DecodeValue(reflect.ValueOf(&results))
	require.NoError(t, err)
	require.Equal(t, []any{"1", "2", "3"}, results[0].Rows)
	require.Equal(t, []any{"1", "2", "3"}, results[1].Rows)
}

func TestAtomicBatchResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"metadata": map[string]any{
				"test": []any{
					map[string]any{"name": "id", "type": "unsigned"},
					map[string]any{"name": "name", "type": "string"},
				},
			},
			"ops": []any{
				map[string]any{"type": "get", "space": "test"},
			},
			"data": []any{
				[]any{"1", "foo"},
			},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.AtomicBatchResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)

	require.Equal(t, []crud.FieldFormat{
		{Name: "id", Type: "unsigned"},
		{Name: "name", Type: "string"},
	}, result.Metadata["test"])
	require.Equal(t, []crud.AtomicBatchOpDescriptor{
		{Type: "get", Space: "test"},
	}, result.Ops)
	require.Equal(t, []any{[]any{"1", "foo"}}, result.Data)
}

func TestAtomicBatchResult_DecodeMsgpackNoreturn(t *testing.T) {
	sampleCrudResponse := []any{nil, nil}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.AtomicBatchResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)
	require.Nil(t, result.Metadata)
	require.Nil(t, result.Ops)
	require.Nil(t, result.Data)
}

func TestAtomicBatchResult_DecodeMsgpackError(t *testing.T) {
	errMsg := "Operation #2 (insert on \"users\") failed: " +
		"Duplicate key exists in unique index 'primary' in space 'users'"

	sampleCrudResponse := []any{
		nil,
		map[string]any{
			"class_name": "AtomicBatchError",
			"err":        errMsg,
			"str":        "AtomicBatchError: " + errMsg,
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.AtomicBatchResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)
	require.Equal(t, "AtomicBatchError: "+errMsg, err.Error())
}

func TestAtomicBatchResult_DecodeMsgpackEmpty(t *testing.T) {
	sampleCrudResponse := []any{}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.AtomicBatchResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected empty response array")
}

func TestLocateResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []any{"memtx", nil}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.LocateResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)
	require.Equal(t, "memtx", result.Value)
	require.True(t, result.Found)
}

func TestLocateResult_DecodeMsgpackNotFound(t *testing.T) {
	sampleCrudResponse := []any{nil, nil}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.LocateResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)
	require.Empty(t, result.Value)
	require.False(t, result.Found)
}

func TestLocateResult_DecodeMsgpackError(t *testing.T) {
	errMsg := "Module 'cooler' with function 'locate' is not available on this storage"

	sampleCrudResponse := []any{
		nil,
		map[string]any{
			"class_name": "LocateError",
			"err":        errMsg,
			"str":        "LocateError: " + errMsg,
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.LocateResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)
	require.Equal(t, "LocateError: "+errMsg, err.Error())
}
