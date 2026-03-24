package crud_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/crud"
)

const validSpace = "test" // Any valid value != default.

const CrudRequestType = iproto.IPROTO_CALL

var reqObject = crud.MapObject{
	"id": uint(24),
}

var reqObjects = []crud.Object{
	crud.MapObject{
		"id": uint(24),
	},
	crud.MapObject{
		"id": uint(25),
	},
}

var reqObjectsOperationsData = []crud.ObjectOperationsData{
	{
		Object: crud.MapObject{
			"id": uint(24),
		},
		Operations: []crud.Operation{
			{
				Operator: crud.Add,
				Field:    "id",
				Value:    uint(1020),
			},
		},
	},
	{
		Object: crud.MapObject{
			"id": uint(25),
		},
		Operations: []crud.Operation{
			{
				Operator: crud.Add,
				Field:    "id",
				Value:    uint(1020),
			},
		},
	},
}

var expectedOpts = map[string]any{
	"timeout": timeout,
}

func extractRequestBody(req tarantool.Request) ([]byte, error) {
	var reqBuf bytes.Buffer
	reqEnc := msgpack.NewEncoder(&reqBuf)

	err := req.Body(nil, reqEnc)
	if err != nil {
		return nil, fmt.Errorf("An unexpected Response.Body() error: %q", err.Error())
	}

	return reqBuf.Bytes(), nil
}

func assertBodyEqual(tb testing.TB, reference tarantool.Request, req tarantool.Request) {
	tb.Helper()

	reqBody, err := extractRequestBody(req)
	require.NoError(tb, err, "An unexpected Response.Body() error")

	refBody, err := extractRequestBody(reference)
	require.NoError(tb, err, "An unexpected Response.Body() error")

	assert.Equal(tb, refBody, reqBody, "Encoded request body mismatch")
}

func BenchmarkLenRequest(b *testing.B) {
	buf := bytes.Buffer{}
	buf.Grow(512 * 1024 * 1024) // Avoid allocs in test.
	enc := msgpack.NewEncoder(&buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		req := crud.NewLenRequest(spaceName).
			Opts(crud.LenOpts{
				Timeout: option.SomeFloat64(3.5),
			})
		if err := req.Body(nil, enc); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSelectRequest(b *testing.B) {
	buf := bytes.Buffer{}
	buf.Grow(512 * 1024 * 1024) // Avoid allocs in test.
	enc := msgpack.NewEncoder(&buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		req := crud.NewSelectRequest(spaceName).
			Opts(crud.SelectOpts{
				Timeout:      option.SomeFloat64(3.5),
				VshardRouter: option.SomeString("asd"),
				Balance:      option.SomeBool(true),
			})
		if err := req.Body(nil, enc); err != nil {
			b.Error(err)
		}
	}
}

func TestRequestsCodes(t *testing.T) {
	tests := []struct {
		req   tarantool.Request
		rtype iproto.Type
	}{
		{req: crud.NewInsertRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewInsertObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewInsertManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewInsertObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewGetRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewUpdateRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewDeleteRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewReplaceRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewReplaceObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewReplaceManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewReplaceObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewUpsertRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewUpsertObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewUpsertManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewUpsertObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewMinRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewMaxRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewSelectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewTruncateRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewLenRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewCountRequest(validSpace), rtype: CrudRequestType},
		{req: crud.NewStorageInfoRequest(), rtype: CrudRequestType},
		{req: crud.NewStatsRequest(), rtype: CrudRequestType},
		{req: crud.NewSchemaRequest(), rtype: CrudRequestType},
	}

	for _, test := range tests {
		assert.Equal(t, test.rtype, test.req.Type(), "An invalid request type")
	}
}

func TestRequestsAsync(t *testing.T) {
	tests := []struct {
		req   tarantool.Request
		async bool
	}{
		{req: crud.NewInsertRequest(validSpace), async: false},
		{req: crud.NewInsertObjectRequest(validSpace), async: false},
		{req: crud.NewInsertManyRequest(validSpace), async: false},
		{req: crud.NewInsertObjectManyRequest(validSpace), async: false},
		{req: crud.NewGetRequest(validSpace), async: false},
		{req: crud.NewUpdateRequest(validSpace), async: false},
		{req: crud.NewDeleteRequest(validSpace), async: false},
		{req: crud.NewReplaceRequest(validSpace), async: false},
		{req: crud.NewReplaceObjectRequest(validSpace), async: false},
		{req: crud.NewReplaceManyRequest(validSpace), async: false},
		{req: crud.NewReplaceObjectManyRequest(validSpace), async: false},
		{req: crud.NewUpsertRequest(validSpace), async: false},
		{req: crud.NewUpsertObjectRequest(validSpace), async: false},
		{req: crud.NewUpsertManyRequest(validSpace), async: false},
		{req: crud.NewUpsertObjectManyRequest(validSpace), async: false},
		{req: crud.NewMinRequest(validSpace), async: false},
		{req: crud.NewMaxRequest(validSpace), async: false},
		{req: crud.NewSelectRequest(validSpace), async: false},
		{req: crud.NewTruncateRequest(validSpace), async: false},
		{req: crud.NewLenRequest(validSpace), async: false},
		{req: crud.NewCountRequest(validSpace), async: false},
		{req: crud.NewStorageInfoRequest(), async: false},
		{req: crud.NewStatsRequest(), async: false},
		{req: crud.NewSchemaRequest(), async: false},
	}

	for _, test := range tests {
		assert.Equal(t, test.async, test.req.Async(), "An invalid async value")
	}
}

func TestRequestsCtx_default(t *testing.T) {
	tests := []struct {
		req      tarantool.Request
		expected context.Context
	}{
		{req: crud.NewInsertRequest(validSpace), expected: nil},
		{req: crud.NewInsertObjectRequest(validSpace), expected: nil},
		{req: crud.NewInsertManyRequest(validSpace), expected: nil},
		{req: crud.NewInsertObjectManyRequest(validSpace), expected: nil},
		{req: crud.NewGetRequest(validSpace), expected: nil},
		{req: crud.NewUpdateRequest(validSpace), expected: nil},
		{req: crud.NewDeleteRequest(validSpace), expected: nil},
		{req: crud.NewReplaceRequest(validSpace), expected: nil},
		{req: crud.NewReplaceObjectRequest(validSpace), expected: nil},
		{req: crud.NewReplaceManyRequest(validSpace), expected: nil},
		{req: crud.NewReplaceObjectManyRequest(validSpace), expected: nil},
		{req: crud.NewUpsertRequest(validSpace), expected: nil},
		{req: crud.NewUpsertObjectRequest(validSpace), expected: nil},
		{req: crud.NewUpsertManyRequest(validSpace), expected: nil},
		{req: crud.NewUpsertObjectManyRequest(validSpace), expected: nil},
		{req: crud.NewMinRequest(validSpace), expected: nil},
		{req: crud.NewMaxRequest(validSpace), expected: nil},
		{req: crud.NewSelectRequest(validSpace), expected: nil},
		{req: crud.NewTruncateRequest(validSpace), expected: nil},
		{req: crud.NewLenRequest(validSpace), expected: nil},
		{req: crud.NewCountRequest(validSpace), expected: nil},
		{req: crud.NewStorageInfoRequest(), expected: nil},
		{req: crud.NewStatsRequest(), expected: nil},
		{req: crud.NewSchemaRequest(), expected: nil},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.req.Ctx(), "An invalid ctx value")
	}
}

func TestRequestsCtx_setter(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		req      tarantool.Request
		expected context.Context
	}{
		{req: crud.NewInsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewInsertObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewInsertManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewInsertObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewGetRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewUpdateRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewDeleteRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewReplaceRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewReplaceObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewReplaceManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewReplaceObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewUpsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewUpsertObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewUpsertManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewUpsertObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewMinRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewMaxRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewSelectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewTruncateRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewLenRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewCountRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.NewStorageInfoRequest().Context(ctx), expected: ctx},
		{req: crud.NewStatsRequest().Context(ctx), expected: ctx},
		{req: crud.NewSchemaRequest().Context(ctx), expected: ctx},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, test.req.Ctx(), "An invalid ctx value")
	}
}

func TestRequestsDefaultValues(t *testing.T) {
	testCases := []struct {
		name   string
		ref    tarantool.Request
		target tarantool.Request
	}{
		{
			name: "InsertRequest",
			ref: tarantool.NewCallRequest("crud.insert").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewInsertRequest(validSpace),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.insert_object").Args(
				[]any{validSpace, map[string]any{}, map[string]any{}}),
			target: crud.NewInsertObjectRequest(validSpace),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_many").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewInsertManyRequest(validSpace),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_object_many").Args(
				[]any{validSpace, []map[string]any{}, map[string]any{}}),
			target: crud.NewInsertObjectManyRequest(validSpace),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCallRequest("crud.get").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewGetRequest(validSpace),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCallRequest("crud.update").Args(
				[]any{validSpace, []any{},
					[]any{}, map[string]any{}}),
			target: crud.NewUpdateRequest(validSpace),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCallRequest("crud.delete").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewDeleteRequest(validSpace),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCallRequest("crud.replace").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewReplaceRequest(validSpace),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCallRequest("crud.replace_object").Args([]any{validSpace,
				map[string]any{}, map[string]any{}}),
			target: crud.NewReplaceObjectRequest(validSpace),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_many").Args([]any{validSpace,
				[]any{}, map[string]any{}}),
			target: crud.NewReplaceManyRequest(validSpace),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_object_many").Args(
				[]any{validSpace, []map[string]any{}, map[string]any{}}),
			target: crud.NewReplaceObjectManyRequest(validSpace),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCallRequest("crud.upsert").Args(
				[]any{validSpace, []any{}, []any{},
					map[string]any{}}),
			target: crud.NewUpsertRequest(validSpace),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object").Args(
				[]any{validSpace, map[string]any{}, []any{},
					map[string]any{}}),
			target: crud.NewUpsertObjectRequest(validSpace),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_many").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewUpsertManyRequest(validSpace),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object_many").Args(
				[]any{validSpace, []any{}, map[string]any{}}),
			target: crud.NewUpsertObjectManyRequest(validSpace),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCallRequest("crud.select").Args(
				[]any{validSpace, nil, map[string]any{}}),
			target: crud.NewSelectRequest(validSpace),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCallRequest("crud.min").Args(
				[]any{validSpace, nil, map[string]any{}}),
			target: crud.NewMinRequest(validSpace),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCallRequest("crud.max").Args(
				[]any{validSpace, nil, map[string]any{}}),
			target: crud.NewMaxRequest(validSpace),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCallRequest("crud.truncate").Args(
				[]any{validSpace, map[string]any{}}),
			target: crud.NewTruncateRequest(validSpace),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCallRequest("crud.len").Args(
				[]any{validSpace, map[string]any{}}),
			target: crud.NewLenRequest(validSpace),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCallRequest("crud.count").Args(
				[]any{validSpace, nil, map[string]any{}}),
			target: crud.NewCountRequest(validSpace),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCallRequest("crud.storage_info").Args(
				[]any{map[string]any{}}),
			target: crud.NewStorageInfoRequest(),
		},
		{
			name: "StatsRequest",
			ref: tarantool.NewCallRequest("crud.stats").Args(
				[]any{}),
			target: crud.NewStatsRequest(),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCallRequest("crud.schema").Args(
				[]any{nil, map[string]any{}}),
			target: crud.NewSchemaRequest(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertBodyEqual(t, tc.ref, tc.target)
		})
	}
}

func TestRequestsSetters(t *testing.T) {
	testCases := []struct {
		name   string
		ref    tarantool.Request
		target tarantool.Request
	}{
		{
			name: "InsertRequest",
			ref: tarantool.NewCallRequest("crud.insert").Args(
				[]any{spaceName, tuple, expectedOpts}),
			target: crud.NewInsertRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.insert_object").Args(
				[]any{spaceName, reqObject, expectedOpts}),
			target: crud.NewInsertObjectRequest(spaceName).Object(reqObject).
				Opts(simpleOperationObjectOpts),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_many").Args(
				[]any{spaceName, tuples, expectedOpts}),
			target: crud.NewInsertManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_object_many").Args(
				[]any{spaceName, reqObjects, expectedOpts}),
			target: crud.NewInsertObjectManyRequest(spaceName).Objects(reqObjects).
				Opts(opObjManyOpts),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCallRequest("crud.get").Args(
				[]any{spaceName, key, expectedOpts}),
			target: crud.NewGetRequest(spaceName).Key(key).Opts(getOpts),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCallRequest("crud.update").Args(
				[]any{spaceName, key, operations, expectedOpts}),
			target: crud.NewUpdateRequest(spaceName).Key(key).Operations(operations).
				Opts(simpleOperationOpts),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCallRequest("crud.delete").Args(
				[]any{spaceName, key, expectedOpts}),
			target: crud.NewDeleteRequest(spaceName).Key(key).Opts(simpleOperationOpts),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCallRequest("crud.replace").Args(
				[]any{spaceName, tuple, expectedOpts}),
			target: crud.NewReplaceRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCallRequest("crud.replace_object").Args(
				[]any{spaceName, reqObject, expectedOpts}),
			target: crud.NewReplaceObjectRequest(spaceName).Object(reqObject).
				Opts(simpleOperationObjectOpts),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_many").Args(
				[]any{spaceName, tuples, expectedOpts}),
			target: crud.NewReplaceManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_object_many").Args(
				[]any{spaceName, reqObjects, expectedOpts}),
			target: crud.NewReplaceObjectManyRequest(spaceName).Objects(reqObjects).
				Opts(opObjManyOpts),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCallRequest("crud.upsert").Args(
				[]any{spaceName, tuple, operations, expectedOpts}),
			target: crud.NewUpsertRequest(spaceName).Tuple(tuple).Operations(operations).
				Opts(simpleOperationOpts),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object").Args(
				[]any{spaceName, reqObject, operations, expectedOpts}),
			target: crud.NewUpsertObjectRequest(spaceName).Object(reqObject).
				Operations(operations).Opts(simpleOperationOpts),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_many").Args(
				[]any{spaceName, tuplesOperationsData, expectedOpts}),
			target: crud.NewUpsertManyRequest(spaceName).
				TuplesOperationsData(tuplesOperationsData).Opts(opManyOpts),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object_many").Args(
				[]any{spaceName, reqObjectsOperationsData, expectedOpts}),
			target: crud.NewUpsertObjectManyRequest(spaceName).
				ObjectsOperationsData(reqObjectsOperationsData).Opts(opManyOpts),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCallRequest("crud.select").Args(
				[]any{spaceName, conditions, expectedOpts}),
			target: crud.NewSelectRequest(spaceName).Conditions(conditions).Opts(selectOpts),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCallRequest("crud.min").Args(
				[]any{spaceName, indexName, expectedOpts}),
			target: crud.NewMinRequest(spaceName).Index(indexName).Opts(minOpts),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCallRequest("crud.max").Args(
				[]any{spaceName, indexName, expectedOpts}),
			target: crud.NewMaxRequest(spaceName).Index(indexName).Opts(maxOpts),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCallRequest("crud.truncate").Args(
				[]any{spaceName, expectedOpts}),
			target: crud.NewTruncateRequest(spaceName).Opts(baseOpts),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCallRequest("crud.len").Args(
				[]any{spaceName, expectedOpts}),
			target: crud.NewLenRequest(spaceName).Opts(baseOpts),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCallRequest("crud.count").Args(
				[]any{spaceName, conditions, expectedOpts}),
			target: crud.NewCountRequest(spaceName).Conditions(conditions).Opts(countOpts),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCallRequest("crud.storage_info").Args(
				[]any{expectedOpts}),
			target: crud.NewStorageInfoRequest().Opts(baseOpts),
		},
		{
			name: "StatsRequest",
			ref: tarantool.NewCallRequest("crud.stats").Args(
				[]any{spaceName}),
			target: crud.NewStatsRequest().Space(spaceName),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCallRequest("crud.schema").Args(
				[]any{nil, schemaOpts},
			),
			target: crud.NewSchemaRequest().Opts(schemaOpts),
		},
		{
			name: "SchemaRequestWithSpace",
			ref: tarantool.NewCallRequest("crud.schema").Args(
				[]any{spaceName, schemaOpts},
			),
			target: crud.NewSchemaRequest().Space(spaceName).Opts(schemaOpts),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertBodyEqual(t, tc.ref, tc.target)
		})
	}
}

func TestRequestsVshardRouter(t *testing.T) {
	testCases := []struct {
		name   string
		ref    tarantool.Request
		target tarantool.Request
	}{
		{
			name: "InsertRequest",
			ref: tarantool.NewCallRequest("crud.insert").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewInsertRequest(validSpace).Opts(crud.InsertOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.insert_object").Args([]any{
				validSpace,
				map[string]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewInsertObjectRequest(validSpace).Opts(crud.InsertObjectOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_many").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewInsertManyRequest(validSpace).Opts(crud.InsertManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.insert_object_many").Args([]any{
				validSpace,
				[]map[string]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewInsertObjectManyRequest(validSpace).Opts(crud.InsertObjectManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCallRequest("crud.get").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewGetRequest(validSpace).Opts(crud.GetOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCallRequest("crud.update").Args([]any{
				validSpace,
				[]any{},
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewUpdateRequest(validSpace).Opts(crud.UpdateOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCallRequest("crud.delete").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewDeleteRequest(validSpace).Opts(crud.DeleteOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCallRequest("crud.replace").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewReplaceRequest(validSpace).Opts(crud.ReplaceOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCallRequest("crud.replace_object").Args([]any{
				validSpace,
				map[string]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewReplaceObjectRequest(validSpace).Opts(crud.ReplaceObjectOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_many").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewReplaceManyRequest(validSpace).Opts(crud.ReplaceManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.replace_object_many").Args([]any{
				validSpace,
				[]map[string]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewReplaceObjectManyRequest(validSpace).Opts(crud.ReplaceObjectManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCallRequest("crud.upsert").Args([]any{
				validSpace,
				[]any{},
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewUpsertRequest(validSpace).Opts(crud.UpsertOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object").Args([]any{
				validSpace,
				map[string]any{},
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewUpsertObjectRequest(validSpace).Opts(crud.UpsertObjectOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_many").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewUpsertManyRequest(validSpace).Opts(crud.UpsertManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCallRequest("crud.upsert_object_many").Args([]any{
				validSpace,
				[]any{},
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewUpsertObjectManyRequest(validSpace).Opts(crud.UpsertObjectManyOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCallRequest("crud.select").Args([]any{
				validSpace,
				nil,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewSelectRequest(validSpace).Opts(crud.SelectOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCallRequest("crud.min").Args([]any{
				validSpace,
				nil,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewMinRequest(validSpace).Opts(crud.MinOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCallRequest("crud.max").Args([]any{
				validSpace,
				nil,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewMaxRequest(validSpace).Opts(crud.MaxOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCallRequest("crud.truncate").Args([]any{
				validSpace,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewTruncateRequest(validSpace).Opts(crud.TruncateOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCallRequest("crud.len").Args([]any{
				validSpace,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewLenRequest(validSpace).Opts(crud.LenOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCallRequest("crud.count").Args([]any{
				validSpace,
				nil,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewCountRequest(validSpace).Opts(crud.CountOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCallRequest("crud.storage_info").Args([]any{
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewStorageInfoRequest().Opts(crud.StorageInfoOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCallRequest("crud.schema").Args([]any{
				nil,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewSchemaRequest().Opts(crud.SchemaOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
		{
			name: "SchemaRequestWithSpace",
			ref: tarantool.NewCallRequest("crud.schema").Args([]any{
				validSpace,
				map[string]any{"vshard_router": "custom_router"},
			}),
			target: crud.NewSchemaRequest().Space(validSpace).Opts(crud.SchemaOpts{
				VshardRouter: option.SomeString("custom_router"),
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertBodyEqual(t, tc.ref, tc.target)
		})
	}
}
