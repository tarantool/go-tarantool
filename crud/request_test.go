package crud_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/crud"
	"github.com/tarantool/go-tarantool/test_helpers"
)

const invalidSpaceMsg = "invalid space"
const invalidIndexMsg = "invalid index"

const invalidSpace = 2
const invalidIndex = 2
const validSpace = "test" // Any valid value != default.
const defaultSpace = 0    // And valid too.
const defaultIndex = 0    // And valid too.

const CrudRequestCode = tarantool.Call17RequestCode

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

func (*ValidSchemeResolver) ResolveSpaceIndex(s, i interface{}) (spaceNo, indexNo uint32, err error) {
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

func assertBodyEqual(t testing.TB, reference tarantool.Request, req tarantool.Request) {
	t.Helper()

	reqBody, err := test_helpers.ExtractRequestBody(req, &resolver, newEncoder)
	if err != nil {
		t.Fatalf("An unexpected Response.Body() error: %q", err.Error())
	}

	refBody, err := test_helpers.ExtractRequestBody(reference, &resolver, newEncoder)
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
	enc := newEncoder(&buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		req := crud.NewLenRequest(spaceName).
			Opts(crud.LenOpts{
				Timeout: crud.MakeOptUint(3),
			})
		if err := req.Body(nil, enc); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkSelectRequest(b *testing.B) {
	buf := bytes.Buffer{}
	buf.Grow(512 * 1024 * 1024) // Avoid allocs in test.
	enc := newEncoder(&buf)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		req := crud.NewSelectRequest(spaceName).
			Opts(crud.SelectOpts{
				Timeout:      crud.MakeOptUint(3),
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
		req  tarantool.Request
		code int32
	}{
		{req: crud.NewInsertRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewInsertObjectRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewInsertManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewInsertObjectManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewGetRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewUpdateRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewDeleteRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewReplaceRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewReplaceObjectRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewReplaceManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewReplaceObjectManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewUpsertRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewUpsertObjectRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewUpsertManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewUpsertObjectManyRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewMinRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewMaxRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewSelectRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewTruncateRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewLenRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewCountRequest(validSpace), code: CrudRequestCode},
		{req: crud.NewStorageInfoRequest(), code: CrudRequestCode},
		{req: crud.NewStatsRequest(), code: CrudRequestCode},
	}

	for _, test := range tests {
		if code := test.req.Code(); code != test.code {
			t.Errorf("An invalid request code 0x%x, expected 0x%x", code, test.code)
		}
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
			ref: tarantool.NewCall17Request("crud.insert").Args([]interface{}{validSpace, []interface{}{},
				map[string]interface{}{}}),
			target: crud.NewInsertRequest(validSpace),
		},
		{
			name: "InsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.insert_object").Args([]interface{}{validSpace, map[string]interface{}{},
				map[string]interface{}{}}),
			target: crud.NewInsertObjectRequest(validSpace),
		},
		{
			name: "InsertManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_many").Args([]interface{}{validSpace, []interface{}{},
				map[string]interface{}{}}),
			target: crud.NewInsertManyRequest(validSpace),
		},
		{
			name: "InsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.insert_object_many").Args([]interface{}{validSpace, []map[string]interface{}{},
				map[string]interface{}{}}),
			target: crud.NewInsertObjectManyRequest(validSpace),
		},
		{
			name: "GetRequest",
			ref: tarantool.NewCall17Request("crud.get").Args([]interface{}{validSpace, []interface{}{},
				map[string]interface{}{}}),
			target: crud.NewGetRequest(validSpace),
		},
		{
			name: "UpdateRequest",
			ref: tarantool.NewCall17Request("crud.update").Args([]interface{}{validSpace, []interface{}{},
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewUpdateRequest(validSpace),
		},
		{
			name: "DeleteRequest",
			ref: tarantool.NewCall17Request("crud.delete").Args([]interface{}{validSpace, []interface{}{},
				map[string]interface{}{}}),
			target: crud.NewDeleteRequest(validSpace),
		},
		{
			name: "ReplaceRequest",
			ref: tarantool.NewCall17Request("crud.replace").Args([]interface{}{validSpace, []interface{}{},
				map[string]interface{}{}}),
			target: crud.NewReplaceRequest(validSpace),
		},
		{
			name: "ReplaceObjectRequest",
			ref: tarantool.NewCall17Request("crud.replace_object").Args([]interface{}{validSpace,
				map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.NewReplaceObjectRequest(validSpace),
		},
		{
			name: "ReplaceManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_many").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewReplaceManyRequest(validSpace),
		},
		{
			name: "ReplaceObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.replace_object_many").Args([]interface{}{validSpace,
				[]map[string]interface{}{}, map[string]interface{}{}}),
			target: crud.NewReplaceObjectManyRequest(validSpace),
		},
		{
			name: "UpsertRequest",
			ref: tarantool.NewCall17Request("crud.upsert").Args([]interface{}{validSpace, []interface{}{},
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewUpsertRequest(validSpace),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object").Args([]interface{}{validSpace,
				map[string]interface{}{}, []interface{}{}, map[string]interface{}{}}),
			target: crud.NewUpsertObjectRequest(validSpace),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_many").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewUpsertManyRequest(validSpace),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object_many").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewUpsertObjectManyRequest(validSpace),
		},
		{
			name: "SelectRequest",
			ref: tarantool.NewCall17Request("crud.select").Args([]interface{}{validSpace,
				nil, map[string]interface{}{}}),
			target: crud.NewSelectRequest(validSpace),
		},
		{
			name: "MinRequest",
			ref: tarantool.NewCall17Request("crud.min").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewMinRequest(validSpace),
		},
		{
			name: "MaxRequest",
			ref: tarantool.NewCall17Request("crud.max").Args([]interface{}{validSpace,
				[]interface{}{}, map[string]interface{}{}}),
			target: crud.NewMaxRequest(validSpace),
		},
		{
			name: "TruncateRequest",
			ref: tarantool.NewCall17Request("crud.truncate").Args([]interface{}{validSpace,
				map[string]interface{}{}}),
			target: crud.NewTruncateRequest(validSpace),
		},
		{
			name: "LenRequest",
			ref: tarantool.NewCall17Request("crud.len").Args([]interface{}{validSpace,
				map[string]interface{}{}}),
			target: crud.NewLenRequest(validSpace),
		},
		{
			name: "CountRequest",
			ref: tarantool.NewCall17Request("crud.count").Args([]interface{}{validSpace,
				nil, map[string]interface{}{}}),
			target: crud.NewCountRequest(validSpace),
		},
		{
			name: "StorageInfoRequest",
			ref: tarantool.NewCall17Request("crud.storage_info").Args(
				[]interface{}{map[string]interface{}{}}),
			target: crud.NewStorageInfoRequest(),
		},
		{
			name: "StatsRequest",
			ref: tarantool.NewCall17Request("crud.stats").Args(
				[]interface{}{}),
			target: crud.NewStatsRequest(),
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
			name:   "InsertRequest",
			ref:    tarantool.NewCall17Request("crud.insert").Args([]interface{}{spaceName, tuple, expectedOpts}),
			target: crud.NewInsertRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name:   "InsertObjectRequest",
			ref:    tarantool.NewCall17Request("crud.insert_object").Args([]interface{}{spaceName, reqObject, expectedOpts}),
			target: crud.NewInsertObjectRequest(spaceName).Object(reqObject).Opts(simpleOperationObjectOpts),
		},
		{
			name:   "InsertManyRequest",
			ref:    tarantool.NewCall17Request("crud.insert_many").Args([]interface{}{spaceName, tuples, expectedOpts}),
			target: crud.NewInsertManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name:   "InsertObjectManyRequest",
			ref:    tarantool.NewCall17Request("crud.insert_object_many").Args([]interface{}{spaceName, reqObjects, expectedOpts}),
			target: crud.NewInsertObjectManyRequest(spaceName).Objects(reqObjects).Opts(opObjManyOpts),
		},
		{
			name:   "GetRequest",
			ref:    tarantool.NewCall17Request("crud.get").Args([]interface{}{spaceName, key, expectedOpts}),
			target: crud.NewGetRequest(spaceName).Key(key).Opts(getOpts),
		},
		{
			name:   "UpdateRequest",
			ref:    tarantool.NewCall17Request("crud.update").Args([]interface{}{spaceName, key, operations, expectedOpts}),
			target: crud.NewUpdateRequest(spaceName).Key(key).Operations(operations).Opts(simpleOperationOpts),
		},
		{
			name:   "DeleteRequest",
			ref:    tarantool.NewCall17Request("crud.delete").Args([]interface{}{spaceName, key, expectedOpts}),
			target: crud.NewDeleteRequest(spaceName).Key(key).Opts(simpleOperationOpts),
		},
		{
			name:   "ReplaceRequest",
			ref:    tarantool.NewCall17Request("crud.replace").Args([]interface{}{spaceName, tuple, expectedOpts}),
			target: crud.NewReplaceRequest(spaceName).Tuple(tuple).Opts(simpleOperationOpts),
		},
		{
			name:   "ReplaceObjectRequest",
			ref:    tarantool.NewCall17Request("crud.replace_object").Args([]interface{}{spaceName, reqObject, expectedOpts}),
			target: crud.NewReplaceObjectRequest(spaceName).Object(reqObject).Opts(simpleOperationObjectOpts),
		},
		{
			name:   "ReplaceManyRequest",
			ref:    tarantool.NewCall17Request("crud.replace_many").Args([]interface{}{spaceName, tuples, expectedOpts}),
			target: crud.NewReplaceManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts),
		},
		{
			name:   "ReplaceObjectManyRequest",
			ref:    tarantool.NewCall17Request("crud.replace_object_many").Args([]interface{}{spaceName, reqObjects, expectedOpts}),
			target: crud.NewReplaceObjectManyRequest(spaceName).Objects(reqObjects).Opts(opObjManyOpts),
		},
		{
			name:   "UpsertRequest",
			ref:    tarantool.NewCall17Request("crud.upsert").Args([]interface{}{spaceName, tuple, operations, expectedOpts}),
			target: crud.NewUpsertRequest(spaceName).Tuple(tuple).Operations(operations).Opts(simpleOperationOpts),
		},
		{
			name: "UpsertObjectRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object").Args([]interface{}{spaceName, reqObject,
				operations, expectedOpts}),
			target: crud.NewUpsertObjectRequest(spaceName).Object(reqObject).Operations(operations).Opts(simpleOperationOpts),
		},
		{
			name: "UpsertManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_many").Args([]interface{}{spaceName,
				tuplesOperationsData, expectedOpts}),
			target: crud.NewUpsertManyRequest(spaceName).TuplesOperationsData(tuplesOperationsData).Opts(opManyOpts),
		},
		{
			name: "UpsertObjectManyRequest",
			ref: tarantool.NewCall17Request("crud.upsert_object_many").Args([]interface{}{spaceName,
				reqObjectsOperationsData, expectedOpts}),
			target: crud.NewUpsertObjectManyRequest(spaceName).ObjectsOperationsData(reqObjectsOperationsData).Opts(opManyOpts),
		},
		{
			name:   "SelectRequest",
			ref:    tarantool.NewCall17Request("crud.select").Args([]interface{}{spaceName, conditions, expectedOpts}),
			target: crud.NewSelectRequest(spaceName).Conditions(conditions).Opts(selectOpts),
		},
		{
			name:   "MinRequest",
			ref:    tarantool.NewCall17Request("crud.min").Args([]interface{}{spaceName, indexName, expectedOpts}),
			target: crud.NewMinRequest(spaceName).Index(indexName).Opts(minOpts),
		},
		{
			name:   "MaxRequest",
			ref:    tarantool.NewCall17Request("crud.max").Args([]interface{}{spaceName, indexName, expectedOpts}),
			target: crud.NewMaxRequest(spaceName).Index(indexName).Opts(maxOpts),
		},
		{
			name:   "TruncateRequest",
			ref:    tarantool.NewCall17Request("crud.truncate").Args([]interface{}{spaceName, expectedOpts}),
			target: crud.NewTruncateRequest(spaceName).Opts(baseOpts),
		},
		{
			name:   "LenRequest",
			ref:    tarantool.NewCall17Request("crud.len").Args([]interface{}{spaceName, expectedOpts}),
			target: crud.NewLenRequest(spaceName).Opts(baseOpts),
		},
		{
			name:   "CountRequest",
			ref:    tarantool.NewCall17Request("crud.count").Args([]interface{}{spaceName, conditions, expectedOpts}),
			target: crud.NewCountRequest(spaceName).Conditions(conditions).Opts(countOpts),
		},
		{
			name:   "StorageInfoRequest",
			ref:    tarantool.NewCall17Request("crud.storage_info").Args([]interface{}{expectedOpts}),
			target: crud.NewStorageInfoRequest().Opts(baseOpts),
		},
		{
			name:   "StatsRequest",
			ref:    tarantool.NewCall17Request("crud.stats").Args([]interface{}{spaceName}),
			target: crud.NewStatsRequest().Space(spaceName),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assertBodyEqual(t, tc.ref, tc.target)
		})
	}
}
