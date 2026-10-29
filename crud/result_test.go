package crud_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-option"
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

func TestStorageCallResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"returns": []any{"foo", nil, "bar"},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)
	require.Equal(t, []any{"foo", nil, "bar"}, result.Returns)
}

func TestStorageCallResult_DecodeMsgpackTyped(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"returns": []any{"foo", "bar"},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	result := crud.NewStorageCallResult(reflect.TypeFor[string]())
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)
	require.Equal(t, []string{"foo", "bar"}, result.Returns)
}

func TestStorageCallResult_DecodeMsgpackError(t *testing.T) {
	sampleCrudResponse := []any{
		nil,
		map[string]any{
			"class_name": "StorageCallError",
			"err":        "Function \"missing\" is not registered",
			"str":        "StorageCallError: Function \"missing\" is not registered",
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)
	require.Equal(t, "StorageCallError: Function \"missing\" is not registered", err.Error())
}

func TestStorageCallResult_DecodeMsgpackErrorFull(t *testing.T) {
	sampleCrudResponse := []any{
		nil,
		map[string]any{
			"class_name":            "StorageCallError",
			"err":                   "Function \"missing\" is not registered",
			"str":                   "StorageCallError: Function \"missing\" is not registered",
			"may_have_side_effects": false,
			"func_name":             "missing",
			"bucket_id":             uint(42),
			"replicaset_id":         "replicaset",
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)

	crudErr, ok := err.(*crud.StorageCallError)
	require.True(t, ok)
	require.Equal(t, "StorageCallError", crudErr.Base.ClassName)
	require.Equal(t, "Function \"missing\" is not registered", crudErr.Base.Err)
	require.Equal(t, "missing", crudErr.FuncName.MustGet())
	require.Equal(t, uint(42), crudErr.BucketId.MustGet())
	require.Equal(t, "replicaset", crudErr.ReplicasetId.MustGet())

	mayHaveSideEffects, ok := crudErr.MayHaveSideEffects.Get()
	require.True(t, ok)
	require.False(t, mayHaveSideEffects)
}

func TestStorageCall_EncodeMsgpack(t *testing.T) {
	call := crud.StorageCall{
		FuncName:  "func_name",
		Args:      option.SomeAny([]any{"a", "b"}),
		SpaceName: option.SomeString(validSpace),
		Key:       option.SomeAny([]any{"k"}),
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	require.NoError(t, enc.Encode(call))

	var decoded map[string]any
	dec := msgpack.NewDecoder(b)
	require.NoError(t, dec.Decode(&decoded))

	require.Equal(t, map[string]any{
		"func_name":  "func_name",
		"args":       []any{"a", "b"},
		"space_name": validSpace,
		"key":        []any{"k"},
	}, decoded)
}

func TestStorageCall_EncodeMsgpackOmitFields(t *testing.T) {
	call := crud.StorageCall{
		FuncName: "func_name",
		BucketId: option.SomeUint(1020),
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	require.NoError(t, enc.Encode(call))

	var decoded map[string]any
	dec := msgpack.NewDecoder(b)
	require.NoError(t, dec.Decode(&decoded))

	require.Len(t, decoded, 2)
	require.Equal(t, "func_name", decoded["func_name"])
	require.EqualValues(t, uint64(1020), decoded["bucket_id"])
}

func TestStorageCallManyResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"results": []any{
				map[string]any{"returns": []any{"foo", "bar"}},
				map[string]any{"error": map[string]any{
					"class_name": "StorageCallError",
					"err":        "Function \"missing\" is not registered",
					"str":        "StorageCallError: Function \"missing\" is not registered",
				}},
			},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallManyResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)

	require.Len(t, result.Results, 2)
	require.Equal(t, []any{"foo", "bar"}, result.Results[0].Returns)
	require.Nil(t, result.Results[0].Error)
	require.Nil(t, result.Results[1].Returns)
	require.NotNil(t, result.Results[1].Error)
	require.Equal(t, "StorageCallError: Function \"missing\" is not registered",
		result.Results[1].Error.Error())
}

func TestStorageCallManyResult_DecodeMsgpackTyped(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"results": []any{
				map[string]any{"returns": []any{"foo", "bar"}},
			},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	result := crud.NewStorageCallManyResult(reflect.TypeFor[string]())
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)

	require.Len(t, result.Results, 1)
	require.Equal(t, []string{"foo", "bar"}, result.Results[0].Returns)
	require.Nil(t, result.Results[0].Error)
}

func TestStorageCallManyResult_DecodeMsgpackError(t *testing.T) {
	sampleCrudResponse := []any{
		nil,
		map[string]any{
			"class_name":            "StorageCallError",
			"err":                   "Failed to route calls",
			"str":                   "StorageCallError: Failed to route calls",
			"may_have_side_effects": true,
			"replicaset_id":         "replicaset",
		},
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallManyResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.Error(t, err)
	require.Equal(t, "StorageCallError: Failed to route calls", err.Error())

	crudErr, ok := err.(*crud.StorageCallError)
	require.True(t, ok)
	require.Equal(t, "replicaset", crudErr.ReplicasetId.MustGet())
}

func TestStorageCallManyResult_DecodeMsgpackErrorFull(t *testing.T) {
	sampleCrudResponse := []any{
		map[string]any{
			"results": []any{
				map[string]any{"error": map[string]any{
					"class_name":            "StorageCallError",
					"err":                   "Function \"missing\" is not registered",
					"str":                   "StorageCallError: Function \"missing\" is not registered",
					"may_have_side_effects": true,
					"func_name":             "missing",
					"bucket_id":             uint(42),
					"replicaset_id":         "replicaset",
				}},
			},
		},
		nil,
	}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(sampleCrudResponse)
	require.NoError(t, err)

	var result crud.StorageCallManyResult
	decoder := msgpack.NewDecoder(b)
	err = decoder.Decode(&result)
	require.NoError(t, err)

	require.Len(t, result.Results, 1)
	require.Nil(t, result.Results[0].Returns)
	crudErr := result.Results[0].Error
	require.NotNil(t, crudErr)
	require.Equal(t, "StorageCallError", crudErr.Base.ClassName)
	require.Equal(t, "Function \"missing\" is not registered", crudErr.Base.Err)
	require.Equal(t, "missing", crudErr.FuncName.MustGet())
	require.Equal(t, uint(42), crudErr.BucketId.MustGet())
	require.Equal(t, "replicaset", crudErr.ReplicasetId.MustGet())

	mayHaveSideEffects, ok := crudErr.MayHaveSideEffects.Get()
	require.True(t, ok)
	require.True(t, mayHaveSideEffects)
}
