package crud_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/crud"
)

const invalidSpaceMsg = "invalid space"
const invalidIndexMsg = "invalid index"

const invalidSpace = 2
const invalidIndex = 2
const validSpace = "test" // Any valid value != default.
const defaultSpace = 0    // And valid too.
const defaultIndex = 0    // And valid too.

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

var expectedOpts = map[string]interface{}{
	"timeout": timeout,
}

type ValidSchemeResolver struct {
}

func (*ValidSchemeResolver) ResolveSpaceIndex(s, i interface{}) (uint32, uint32, error) {
	var spaceNo, indexNo uint32
	if s != nil {
		spaceNo = uint32(s.(int))
	} else {
		spaceNo = defaultSpace
	}
	if i != nil {
		indexNo = uint32(i.(int))
	} else {
		indexNo = defaultIndex
	}
	if spaceNo == invalidSpace {
		return 0, 0, errors.New(invalidSpaceMsg)
	}
	if indexNo == invalidIndex {
		return 0, 0, errors.New(invalidIndexMsg)
	}
	return spaceNo, indexNo, nil
}

var resolver ValidSchemeResolver

func extractRequestBody(req tarantool.Request,
	resolver tarantool.SchemaResolver) ([]byte, error) {
	var reqBuf bytes.Buffer
	reqEnc := msgpack.NewEncoder(&reqBuf)

	err := req.Body(resolver, reqEnc)
	if err != nil {
		return nil, fmt.Errorf("An unexpected Response.Body() error: %q", err.Error())
	}

	return reqBuf.Bytes(), nil
}

func assertBodyEqual(t testing.TB, reference tarantool.Request, req tarantool.Request) {
	t.Helper()

	reqBody, err := extractRequestBody(req, &resolver)
	if err != nil {
		t.Fatalf("An unexpected Response.Body() error: %q", err.Error())
	}

	refBody, err := extractRequestBody(reference, &resolver)
	if err != nil {
		t.Fatalf("An unexpected Response.Body() error: %q", err.Error())
	}

	if !bytes.Equal(reqBody, refBody) {
		t.Errorf("Encoded request %v != reference %v", reqBody, refBody)
	}
}

func BenchmarkLenRequest(b *testing.B) {
	buf := bytes.Buffer{}
	buf.Grow(512 * 1024 * 1024) // Avoid allocs in test.
	enc := msgpack.NewEncoder(&buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		req := crud.MakeLenRequest(spaceName).
			Opts(crud.LenOpts{
				Timeout: crud.MakeOptFloat64(3.5),
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
		req := crud.MakeSelectRequest(spaceName).
			Opts(crud.SelectOpts{
				Timeout:      crud.MakeOptFloat64(3.5),
				VshardRouter: crud.MakeOptString("asd"),
				Balance:      crud.MakeOptBool(true),
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
		{req: crud.MakeInsertRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeInsertObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeInsertManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeInsertObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeGetRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeUpdateRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeDeleteRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeReplaceRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeReplaceObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeReplaceManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeReplaceObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeUpsertRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeUpsertObjectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeUpsertManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeUpsertObjectManyRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeMinRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeMaxRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeSelectRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeTruncateRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeLenRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeCountRequest(validSpace), rtype: CrudRequestType},
		{req: crud.MakeStorageInfoRequest(), rtype: CrudRequestType},
		{req: crud.MakeStatsRequest(), rtype: CrudRequestType},
		{req: crud.MakeSchemaRequest(), rtype: CrudRequestType},
	}

	for _, test := range tests {
		if rtype := test.req.Type(); rtype != test.rtype {
			t.Errorf("An invalid request type 0x%x, expected 0x%x", rtype, test.rtype)
		}
	}
}

func TestRequestsAsync(t *testing.T) {
	tests := []struct {
		req   tarantool.Request
		async bool
	}{
		{req: crud.MakeInsertRequest(validSpace), async: false},
		{req: crud.MakeInsertObjectRequest(validSpace), async: false},
		{req: crud.MakeInsertManyRequest(validSpace), async: false},
		{req: crud.MakeInsertObjectManyRequest(validSpace), async: false},
		{req: crud.MakeGetRequest(validSpace), async: false},
		{req: crud.MakeUpdateRequest(validSpace), async: false},
		{req: crud.MakeDeleteRequest(validSpace), async: false},
		{req: crud.MakeReplaceRequest(validSpace), async: false},
		{req: crud.MakeReplaceObjectRequest(validSpace), async: false},
		{req: crud.MakeReplaceManyRequest(validSpace), async: false},
		{req: crud.MakeReplaceObjectManyRequest(validSpace), async: false},
		{req: crud.MakeUpsertRequest(validSpace), async: false},
		{req: crud.MakeUpsertObjectRequest(validSpace), async: false},
		{req: crud.MakeUpsertManyRequest(validSpace), async: false},
		{req: crud.MakeUpsertObjectManyRequest(validSpace), async: false},
		{req: crud.MakeMinRequest(validSpace), async: false},
		{req: crud.MakeMaxRequest(validSpace), async: false},
		{req: crud.MakeSelectRequest(validSpace), async: false},
		{req: crud.MakeTruncateRequest(validSpace), async: false},
		{req: crud.MakeLenRequest(validSpace), async: false},
		{req: crud.MakeCountRequest(validSpace), async: false},
		{req: crud.MakeStorageInfoRequest(), async: false},
		{req: crud.MakeStatsRequest(), async: false},
		{req: crud.MakeSchemaRequest(), async: false},
	}

	for _, test := range tests {
		if async := test.req.Async(); async != test.async {
			t.Errorf("An invalid async %t, expected %t", async, test.async)
		}
	}
}

func TestRequestsCtx_default(t *testing.T) {
	tests := []struct {
		req      tarantool.Request
		expected context.Context
	}{
		{req: crud.MakeInsertRequest(validSpace), expected: nil},
		{req: crud.MakeInsertObjectRequest(validSpace), expected: nil},
		{req: crud.MakeInsertManyRequest(validSpace), expected: nil},
		{req: crud.MakeInsertObjectManyRequest(validSpace), expected: nil},
		{req: crud.MakeGetRequest(validSpace), expected: nil},
		{req: crud.MakeUpdateRequest(validSpace), expected: nil},
		{req: crud.MakeDeleteRequest(validSpace), expected: nil},
		{req: crud.MakeReplaceRequest(validSpace), expected: nil},
		{req: crud.MakeReplaceObjectRequest(validSpace), expected: nil},
		{req: crud.MakeReplaceManyRequest(validSpace), expected: nil},
		{req: crud.MakeReplaceObjectManyRequest(validSpace), expected: nil},
		{req: crud.MakeUpsertRequest(validSpace), expected: nil},
		{req: crud.MakeUpsertObjectRequest(validSpace), expected: nil},
		{req: crud.MakeUpsertManyRequest(validSpace), expected: nil},
		{req: crud.MakeUpsertObjectManyRequest(validSpace), expected: nil},
		{req: crud.MakeMinRequest(validSpace), expected: nil},
		{req: crud.MakeMaxRequest(validSpace), expected: nil},
		{req: crud.MakeSelectRequest(validSpace), expected: nil},
		{req: crud.MakeTruncateRequest(validSpace), expected: nil},
		{req: crud.MakeLenRequest(validSpace), expected: nil},
		{req: crud.MakeCountRequest(validSpace), expected: nil},
		{req: crud.MakeStorageInfoRequest(), expected: nil},
		{req: crud.MakeStatsRequest(), expected: nil},
		{req: crud.MakeSchemaRequest(), expected: nil},
	}

	for _, test := range tests {
		if ctx := test.req.Ctx(); ctx != test.expected {
			t.Errorf("An invalid ctx %t, expected %t", ctx, test.expected)
		}
	}
}

func TestRequestsCtx_setter(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		req      tarantool.Request
		expected context.Context
	}{
		{req: crud.MakeInsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeInsertObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeInsertManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeInsertObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeGetRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeUpdateRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeDeleteRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeReplaceRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeReplaceObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeReplaceManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeReplaceObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeUpsertRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeUpsertObjectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeUpsertManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeUpsertObjectManyRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeMinRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeMaxRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeSelectRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeTruncateRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeLenRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeCountRequest(validSpace).Context(ctx), expected: ctx},
		{req: crud.MakeStorageInfoRequest().Context(ctx), expected: ctx},
		{req: crud.MakeStatsRequest().Context(ctx), expected: ctx},
		{req: crud.MakeSchemaRequest().Context(ctx), expected: ctx},
	}

	for _, test := range tests {
		if ctx := test.req.Ctx(); ctx != test.expected {
			t.Errorf("An invalid ctx %t, expected %t", ctx, test.expected)
		}
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
			ref: tarantool.NewCall17Request("crud.insert").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeInsertRequest(validSpace),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.insert_object").Args(
				[]interface{}{validSpace, map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeInsertObjectRequest(validSpace),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_many").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeInsertManyRequest(validSpace),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_object_many").Args(
				[]interface{}{validSpace, []map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeInsertObjectManyRequest(validSpace),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCall17Request("crud.get").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeGetRequest(validSpace),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCall17Request("crud.update").Args(
				[]interface{}{validSpace, []interface{}{},
					[]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeUpdateRequest(validSpace),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCall17Request("crud.delete").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeDeleteRequest(validSpace),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCall17Request("crud.replace").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeReplaceRequest(validSpace),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCall17Request("crud.replace_object").Args([]interface{}{validSpace,
				map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeReplaceObjectRequest(validSpace),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_many").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeReplaceManyRequest(validSpace),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_object_many").Args(
				[]interface{}{validSpace, []map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.MakeReplaceObjectManyRequest(validSpace),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCall17Request("crud.upsert").Args(
				[]interface{}{validSpace, []interface{}{}, []interface{}{},
					map[string]interface{}{}}),
			target: crud.MakeUpsertRequest(validSpace),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object").Args(
				[]interface{}{validSpace, map[string]interface{}{}, []interface{}{},
					map[string]interface{}{}}),
			target: crud.MakeUpsertObjectRequest(validSpace),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_many").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeUpsertManyRequest(validSpace),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object_many").Args(
				[]interface{}{validSpace, []interface{}{}, map[string]interface{}{}}),
			target: crud.MakeUpsertObjectManyRequest(validSpace),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCall17Request("crud.select").Args(
				[]interface{}{validSpace, nil, map[string]interface{}{}}),
			target: crud.MakeSelectRequest(validSpace),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCall17Request("crud.min").Args(
				[]interface{}{validSpace, nil, map[string]interface{}{}}),
			target: crud.MakeMinRequest(validSpace),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCall17Request("crud.max").Args(
				[]interface{}{validSpace, nil, map[string]interface{}{}}),
			target: crud.MakeMaxRequest(validSpace),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCall17Request("crud.truncate").Args(
				[]interface{}{validSpace, map[string]interface{}{}}),
			target: crud.MakeTruncateRequest(validSpace),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCall17Request("crud.len").Args(
				[]interface{}{validSpace, map[string]interface{}{}}),
			target: crud.MakeLenRequest(validSpace),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCall17Request("crud.count").Args(
				[]interface{}{validSpace, nil, map[string]interface{}{}}),
			target: crud.MakeCountRequest(validSpace),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCall17Request("crud.storage_info").Args(
				[]interface{}{map[string]interface{}{}}),
			target: crud.MakeStorageInfoRequest(),
		},
		{
			name: "StatsRequest",
			ref: tarantool.NewCall17Request("crud.stats").Args(
				[]interface{}{}),
			target: crud.MakeStatsRequest(),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCall17Request("crud.schema").Args(
				[]interface{}{nil, map[string]interface{}{}}),
			target: crud.MakeSchemaRequest(),
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
			ref: tarantool.NewCall17Request("crud.insert").Args(
				[]interface{}{spaceName, tuple, expectedOpts}),
			target: crud.MakeInsertRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.insert_object").Args(
				[]interface{}{spaceName, reqObject, expectedOpts}),
			target: crud.MakeInsertObjectRequest(spaceName).Object(reqObject).
				Opts(simpleOperationObjectOpts),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_many").Args(
				[]interface{}{spaceName, tuples, expectedOpts}),
			target: crud.MakeInsertManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_object_many").Args(
				[]interface{}{spaceName, reqObjects, expectedOpts}),
			target: crud.MakeInsertObjectManyRequest(spaceName).Objects(reqObjects).
				Opts(opObjManyOpts),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCall17Request("crud.get").Args(
				[]interface{}{spaceName, key, expectedOpts}),
			target: crud.MakeGetRequest(spaceName).Key(key).Opts(getOpts),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCall17Request("crud.update").Args(
				[]interface{}{spaceName, key, operations, expectedOpts}),
			target: crud.MakeUpdateRequest(spaceName).Key(key).Operations(operations).
				Opts(simpleOperationOpts),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCall17Request("crud.delete").Args(
				[]interface{}{spaceName, key, expectedOpts}),
			target: crud.MakeDeleteRequest(spaceName).Key(key).Opts(simpleOperationOpts),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCall17Request("crud.replace").Args(
				[]interface{}{spaceName, tuple, expectedOpts}),
			target: crud.MakeReplaceRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCall17Request("crud.replace_object").Args(
				[]interface{}{spaceName, reqObject, expectedOpts}),
			target: crud.MakeReplaceObjectRequest(spaceName).Object(reqObject).
				Opts(simpleOperationObjectOpts),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_many").Args(
				[]interface{}{spaceName, tuples, expectedOpts}),
			target: crud.MakeReplaceManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_object_many").Args(
				[]interface{}{spaceName, reqObjects, expectedOpts}),
			target: crud.MakeReplaceObjectManyRequest(spaceName).Objects(reqObjects).
				Opts(opObjManyOpts),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCall17Request("crud.upsert").Args(
				[]interface{}{spaceName, tuple, operations, expectedOpts}),
			target: crud.MakeUpsertRequest(spaceName).Tuple(tuple).Operations(operations).
				Opts(simpleOperationOpts),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object").Args(
				[]interface{}{spaceName, reqObject, operations, expectedOpts}),
			target: crud.MakeUpsertObjectRequest(spaceName).Object(reqObject).
				Operations(operations).Opts(simpleOperationOpts),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_many").Args(
				[]interface{}{spaceName, tuplesOperationsData, expectedOpts}),
			target: crud.MakeUpsertManyRequest(spaceName).
				TuplesOperationsData(tuplesOperationsData).Opts(opManyOpts),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object_many").Args(
				[]interface{}{spaceName, reqObjectsOperationsData, expectedOpts}),
			target: crud.MakeUpsertObjectManyRequest(spaceName).
				ObjectsOperationsData(reqObjectsOperationsData).Opts(opManyOpts),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCall17Request("crud.select").Args(
				[]interface{}{spaceName, conditions, expectedOpts}),
			target: crud.MakeSelectRequest(spaceName).Conditions(conditions).Opts(selectOpts),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCall17Request("crud.min").Args(
				[]interface{}{spaceName, indexName, expectedOpts}),
			target: crud.MakeMinRequest(spaceName).Index(indexName).Opts(minOpts),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCall17Request("crud.max").Args(
				[]interface{}{spaceName, indexName, expectedOpts}),
			target: crud.MakeMaxRequest(spaceName).Index(indexName).Opts(maxOpts),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCall17Request("crud.truncate").Args(
				[]interface{}{spaceName, expectedOpts}),
			target: crud.MakeTruncateRequest(spaceName).Opts(baseOpts),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCall17Request("crud.len").Args(
				[]interface{}{spaceName, expectedOpts}),
			target: crud.MakeLenRequest(spaceName).Opts(baseOpts),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCall17Request("crud.count").Args(
				[]interface{}{spaceName, conditions, expectedOpts}),
			target: crud.MakeCountRequest(spaceName).Conditions(conditions).Opts(countOpts),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCall17Request("crud.storage_info").Args(
				[]interface{}{expectedOpts}),
			target: crud.MakeStorageInfoRequest().Opts(baseOpts),
		},
		{
			name: "StatsRequest",
			ref: tarantool.NewCall17Request("crud.stats").Args(
				[]interface{}{spaceName}),
			target: crud.MakeStatsRequest().Space(spaceName),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCall17Request("crud.schema").Args(
				[]interface{}{nil, schemaOpts},
			),
			target: crud.MakeSchemaRequest().Opts(schemaOpts),
		},
		{
			name: "SchemaRequestWithSpace",
			ref: tarantool.NewCall17Request("crud.schema").Args(
				[]interface{}{spaceName, schemaOpts},
			),
			target: crud.MakeSchemaRequest().Space(spaceName).Opts(schemaOpts),
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
			ref: tarantool.NewCall17Request("crud.insert").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeInsertRequest(validSpace).Opts(crud.InsertOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.insert_object").Args([]interface{}{
				validSpace,
				map[string]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeInsertObjectRequest(validSpace).Opts(crud.InsertObjectOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_many").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeInsertManyRequest(validSpace).Opts(crud.InsertManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_object_many").Args([]interface{}{
				validSpace,
				[]map[string]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeInsertObjectManyRequest(validSpace).Opts(crud.InsertObjectManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCall17Request("crud.get").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeGetRequest(validSpace).Opts(crud.GetOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCall17Request("crud.update").Args([]interface{}{
				validSpace,
				[]interface{}{},
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeUpdateRequest(validSpace).Opts(crud.UpdateOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCall17Request("crud.delete").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeDeleteRequest(validSpace).Opts(crud.DeleteOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCall17Request("crud.replace").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeReplaceRequest(validSpace).Opts(crud.ReplaceOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCall17Request("crud.replace_object").Args([]interface{}{
				validSpace,
				map[string]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeReplaceObjectRequest(validSpace).Opts(crud.ReplaceObjectOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_many").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeReplaceManyRequest(validSpace).Opts(crud.ReplaceManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_object_many").Args([]interface{}{
				validSpace,
				[]map[string]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeReplaceObjectManyRequest(validSpace).Opts(crud.ReplaceObjectManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCall17Request("crud.upsert").Args([]interface{}{
				validSpace,
				[]interface{}{},
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeUpsertRequest(validSpace).Opts(crud.UpsertOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object").Args([]interface{}{
				validSpace,
				map[string]interface{}{},
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeUpsertObjectRequest(validSpace).Opts(crud.UpsertObjectOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_many").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeUpsertManyRequest(validSpace).Opts(crud.UpsertManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object_many").Args([]interface{}{
				validSpace,
				[]interface{}{},
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeUpsertObjectManyRequest(validSpace).Opts(crud.UpsertObjectManyOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCall17Request("crud.select").Args([]interface{}{
				validSpace,
				nil,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeSelectRequest(validSpace).Opts(crud.SelectOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCall17Request("crud.min").Args([]interface{}{
				validSpace,
				nil,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeMinRequest(validSpace).Opts(crud.MinOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCall17Request("crud.max").Args([]interface{}{
				validSpace,
				nil,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeMaxRequest(validSpace).Opts(crud.MaxOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCall17Request("crud.truncate").Args([]interface{}{
				validSpace,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeTruncateRequest(validSpace).Opts(crud.TruncateOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCall17Request("crud.len").Args([]interface{}{
				validSpace,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeLenRequest(validSpace).Opts(crud.LenOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCall17Request("crud.count").Args([]interface{}{
				validSpace,
				nil,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeCountRequest(validSpace).Opts(crud.CountOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCall17Request("crud.storage_info").Args([]interface{}{
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeStorageInfoRequest().Opts(crud.StorageInfoOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "SchemaRequest",
			ref: tarantool.NewCall17Request("crud.schema").Args([]interface{}{
				nil,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeSchemaRequest().Opts(crud.SchemaOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
		{
			name: "SchemaRequestWithSpace",
			ref: tarantool.NewCall17Request("crud.schema").Args([]interface{}{
				validSpace,
				map[string]interface{}{"vshard_router": "custom_router"},
			}),
			target: crud.MakeSchemaRequest().Space(validSpace).Opts(crud.SchemaOpts{
				VshardRouter: crud.MakeOptString("custom_router"),
			}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertBodyEqual(t, tc.ref, tc.target)
		})
	}
}
