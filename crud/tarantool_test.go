package crud_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/crud"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var server = "127.0.0.1:3013"
var spaceNo = uint32(617)
var spaceName = "test"
var invalidSpaceName = "invalid"
var indexNo = uint32(0)
var indexName = "primary_index"
var opts = tarantool.Opts{
	Timeout: 5 * time.Second,
	User:    "test",
	Pass:    "test",
}

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	InitScript:   "testdata/config.lua",
	Listen:       server,
	User:         opts.User,
	Pass:         opts.Pass,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 10,
	RetryTimeout: 500 * time.Millisecond,
}

var timeout = uint(1)

var operations = []crud.Operation{
	{
		Operator: crud.Assign,
		Field:    "name",
		Value:    "bye",
	},
}

var selectOpts = crud.SelectOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var countOpts = crud.CountOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var getOpts = crud.GetOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var minOpts = crud.MinOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var maxOpts = crud.MaxOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var baseOpts = crud.BaseOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var simpleOperationOpts = crud.SimpleOperationOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var simpleOperationObjectOpts = crud.SimpleOperationObjectOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var opManyOpts = crud.OperationManyOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var opObjManyOpts = crud.OperationObjectManyOpts{
	Timeout: crud.MakeOptUint(timeout),
}

var conditions = []crud.Condition{
	{
		Operator: crud.Lt,
		Field:    "id",
		Value:    uint(1020),
	},
}

var key = []interface{}{uint(1019)}

var tuples = generateTuples()
var objects = generateObjects()

var tuple = []interface{}{uint(1019), nil, "bla"}
var object = crud.MapObject{
	"id":   uint(1019),
	"name": "bla",
}

func connect(t testing.TB) *tarantool.Connection {
	for i := 0; i < 10; i++ {
		conn, err := tarantool.Connect(server, opts)
		if err != nil {
			t.Fatalf("Failed to connect: %s", err)
		}

		ret := struct {
			_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
			Result   bool
		}{}
		err = conn.Do(tarantool.NewCall17Request("is_ready")).GetTyped(&ret)
		if err != nil {
			t.Fatalf("Failed to check is_ready: %s", err)
		}

		if ret.Result {
			return conn
		}

		time.Sleep(time.Second)
	}

	t.Fatalf("Failed to wait for a ready state connect.")
	return nil
}

var testProcessDataCases = []struct {
	name            string
	expectedRespLen int
	req             tarantool.Request
}{
	{
		"Select",
		2,
		crud.MakeSelectRequest(spaceName).
			Conditions(conditions).
			Opts(selectOpts),
	},
	{
		"Get",
		2,
		crud.MakeGetRequest(spaceName).
			Key(key).
			Opts(getOpts),
	},
	{
		"Update",
		2,
		crud.MakeUpdateRequest(spaceName).
			Key(key).
			Operations(operations).
			Opts(simpleOperationOpts),
	},
	{
		"Delete",
		2,
		crud.MakeDeleteRequest(spaceName).
			Key(key).
			Opts(simpleOperationOpts),
	},
	{
		"Min",
		2,
		crud.MakeMinRequest(spaceName).Opts(minOpts),
	},
	{
		"Min",
		2,
		crud.MakeMinRequest(spaceName).Index(indexName).Opts(minOpts),
	},
	{
		"Max",
		2,
		crud.MakeMaxRequest(spaceName).Opts(maxOpts),
	},
	{
		"Max",
		2,
		crud.MakeMaxRequest(spaceName).Index(indexName).Opts(maxOpts),
	},
	{
		"Truncate",
		1,
		crud.MakeTruncateRequest(spaceName).Opts(baseOpts),
	},
	{
		"Len",
		1,
		crud.MakeLenRequest(spaceName).Opts(baseOpts),
	},
	{
		"Count",
		2,
		crud.MakeCountRequest(spaceName).
			Conditions(conditions).
			Opts(countOpts),
	},
	{
		"Stats",
		1,
		crud.MakeStatsRequest().Space(spaceName),
	},
	{
		"StorageInfo",
		1,
		crud.MakeStorageInfoRequest().Opts(baseOpts),
	},
}

var testResultWithErrCases = []struct {
	name string
	resp interface{}
	req  tarantool.Request
}{
	{
		"BaseResult",
		&crud.Result{},
		crud.MakeSelectRequest(invalidSpaceName).Opts(selectOpts),
	},
	{
		"ManyResult",
		&crud.Result{},
		crud.MakeReplaceManyRequest(invalidSpaceName).Opts(opManyOpts),
	},
	{
		"NumberResult",
		&crud.CountResult{},
		crud.MakeCountRequest(invalidSpaceName).Opts(countOpts),
	},
	{
		"BoolResult",
		&crud.TruncateResult{},
		crud.MakeTruncateRequest(invalidSpaceName).Opts(baseOpts),
	},
}

var tuplesOperationsData = generateTuplesOperationsData(tuples, operations)
var objectsOperationData = generateObjectsOperationsData(objects, operations)

var testGenerateDataCases = []struct {
	name                string
	expectedRespLen     int
	expectedTuplesCount int
	req                 tarantool.Request
}{
	{
		"Insert",
		1,
		1,
		crud.MakeInsertRequest(spaceName).
			Tuple(tuple).
			Opts(simpleOperationOpts),
	},
	{
		"InsertObject",
		1,
		1,
		crud.MakeInsertObjectRequest(spaceName).
			Object(object).
			Opts(simpleOperationObjectOpts),
	},
	{
		"InsertMany",
		1,
		10,
		crud.MakeInsertManyRequest(spaceName).
			Tuples(tuples).
			Opts(opManyOpts),
	},
	{
		"InsertObjectMany",
		1,
		10,
		crud.MakeInsertObjectManyRequest(spaceName).
			Objects(objects).
			Opts(opObjManyOpts),
	},
	{
		"Replace",
		1,
		1,
		crud.MakeReplaceRequest(spaceName).
			Tuple(tuple).
			Opts(simpleOperationOpts),
	},
	{
		"ReplaceObject",
		1,
		1,
		crud.MakeReplaceObjectRequest(spaceName).
			Object(object).
			Opts(simpleOperationObjectOpts),
	},
	{
		"ReplaceMany",
		1,
		10,
		crud.MakeReplaceManyRequest(spaceName).
			Tuples(tuples).
			Opts(opManyOpts),
	},
	{
		"ReplaceObjectMany",
		1,
		10,
		crud.MakeReplaceObjectManyRequest(spaceName).
			Objects(objects).
			Opts(opObjManyOpts),
	},
	{
		"Upsert",
		1,
		1,
		crud.MakeUpsertRequest(spaceName).
			Tuple(tuple).
			Operations(operations).
			Opts(simpleOperationOpts),
	},
	{
		"UpsertObject",
		1,
		1,
		crud.MakeUpsertObjectRequest(spaceName).
			Object(object).
			Operations(operations).
			Opts(simpleOperationOpts),
	},
	{
		"UpsertMany",
		1,
		10,
		crud.MakeUpsertManyRequest(spaceName).
			TuplesOperationsData(tuplesOperationsData).
			Opts(opManyOpts),
	},
	{
		"UpsertObjectMany",
		1,
		10,
		crud.MakeUpsertObjectManyRequest(spaceName).
			ObjectsOperationsData(objectsOperationData).
			Opts(opManyOpts),
	},
}

func generateTuples() []crud.Tuple {
	tpls := []crud.Tuple{}
	for i := 1010; i < 1020; i++ {
		tpls = append(tpls, []interface{}{uint(i), nil, "bla"})
	}

	return tpls
}

func generateTuplesOperationsData(tpls []crud.Tuple,
	operations []crud.Operation) []crud.TupleOperationsData {
	tuplesOperationsData := []crud.TupleOperationsData{}
	for _, tpl := range tpls {
		tuplesOperationsData = append(tuplesOperationsData, crud.TupleOperationsData{
			Tuple:      tpl,
			Operations: operations,
		})
	}

	return tuplesOperationsData
}

func generateObjects() []crud.Object {
	objs := []crud.Object{}
	for i := 1010; i < 1020; i++ {
		objs = append(objs, crud.MapObject{
			"id":   uint(i),
			"name": "bla",
		})
	}

	return objs
}

func generateObjectsOperationsData(objs []crud.Object,
	operations []crud.Operation) []crud.ObjectOperationsData {
	objectsOperationsData := []crud.ObjectOperationsData{}
	for _, obj := range objs {
		objectsOperationsData = append(objectsOperationsData, crud.ObjectOperationsData{
			Object:     obj,
			Operations: operations,
		})
	}

	return objectsOperationsData
}

func getCrudError(req tarantool.Request, crudError interface{}) (interface{}, error) {
	var err []interface{}
	var ok bool

	rtype := req.Type()
	if crudError != nil {
		if rtype == iproto.IPROTO_CALL {
			return crudError, nil
		}

		if err, ok = crudError.([]interface{}); !ok {
			return nil, fmt.Errorf("Incorrect CRUD error format")
		}

		if len(err) < 1 {
			return nil, fmt.Errorf("Incorrect CRUD error format")
		}

		if err[0] != nil {
			return err[0], nil
		}
	}

	return nil, nil
}

func testCrudRequestPrepareData(t *testing.T, conn tarantool.Connector) {
	t.Helper()

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewReplaceRequest(spaceName).Tuple(
			[]interface{}{uint(i), nil, "bla"})
		if _, err := conn.Do(req).Get(); err != nil {
			t.Fatalf("Unable to prepare tuples: %s", err)
		}
	}
}

func testSelectGeneratedData(t *testing.T, conn tarantool.Connector,
	expectedTuplesCount int) {
	req := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(20).
		Iterator(tarantool.IterGe).
		Key([]interface{}{uint(1010)})
	resp, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != expectedTuplesCount {
		t.Fatalf("Response Data len %d != %d", len(resp.Data), expectedTuplesCount)
	}
}

func testCrudRequestCheck(t *testing.T, req tarantool.Request,
	resp *tarantool.Response, err error, expectedLen int) {
	t.Helper()

	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err.Error())
	}

	if resp == nil {
		t.Fatalf("Response is nil after Do CRUD request")
	}

	if len(resp.Data) < expectedLen {
		t.Fatalf("Response Body len < %#v, actual len %#v",
			expectedLen, len(resp.Data))
	}

	// resp.Data[0] - CRUD res.
	// resp.Data[1] - CRUD err.
	if expectedLen >= 2 && resp.Data[1] != nil {
		if crudErr, err := getCrudError(req, resp.Data[1]); err != nil {
			t.Fatalf("Failed to get CRUD error: %#v", err)
		} else if crudErr != nil {
			t.Fatalf("Failed to perform CRUD request on CRUD side: %#v", crudErr)
		}
	}
}

func TestCrudGenerateData(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testGenerateDataCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}

			resp, err := conn.Do(testCase.req).Get()
			testCrudRequestCheck(t, testCase.req, resp,
				err, testCase.expectedRespLen)

			testSelectGeneratedData(t, conn, testCase.expectedTuplesCount)

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}
		})
	}
}

func TestCrudProcessData(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testProcessDataCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCrudRequestPrepareData(t, conn)
			resp, err := conn.Do(testCase.req).Get()
			testCrudRequestCheck(t, testCase.req, resp,
				err, testCase.expectedRespLen)
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}
		})
	}
}

func TestUnflattenRows_IncorrectParams(t *testing.T) {
	invalidMetadata := []interface{}{
		map[interface{}]interface{}{
			"name": true,
			"type": "number",
		},
		map[interface{}]interface{}{
			"name": "name",
			"type": "string",
		},
	}

	tpls := []interface{}{
		tuple,
	}

	// Format tuples with invalid format with UnflattenRows.
	objs, err := crud.UnflattenRows(tpls, invalidMetadata)
	require.Nil(t, objs)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "unexpected space format")
}

func TestUnflattenRows(t *testing.T) {
	var (
		ok         bool
		err        error
		expectedId uint64
		actualId   uint64
		res        map[interface{}]interface{}
		metadata   []interface{}
		tpls       []interface{}
	)

	conn := connect(t)
	defer conn.Close()

	// Do `replace`.
	req := crud.MakeReplaceRequest(spaceName).
		Tuple(tuple).
		Opts(simpleOperationOpts)
	resp, err := conn.Do(req).Get()
	testCrudRequestCheck(t, req, resp, err, 2)

	if res, ok = resp.Data[0].(map[interface{}]interface{}); !ok {
		t.Fatalf("Unexpected CRUD result: %#v", resp.Data[0])
	}

	if rawMetadata, ok := res["metadata"]; !ok {
		t.Fatalf("Failed to get CRUD metadata")
	} else {
		if metadata, ok = rawMetadata.([]interface{}); !ok {
			t.Fatalf("Unexpected CRUD metadata: %#v", rawMetadata)
		}
	}

	if rawTuples, ok := res["rows"]; !ok {
		t.Fatalf("Failed to get CRUD rows")
	} else {
		if tpls, ok = rawTuples.([]interface{}); !ok {
			t.Fatalf("Unexpected CRUD rows: %#v", rawTuples)
		}
	}

	// Format `replace` result with UnflattenRows.
	objs, err := crud.UnflattenRows(tpls, metadata)
	if err != nil {
		t.Fatalf("Failed to unflatten rows: %#v", err)
	}
	if len(objs) < 1 {
		t.Fatalf("Unexpected unflatten rows result: %#v", objs)
	}

	if _, ok := objs[0]["bucket_id"]; ok {
		delete(objs[0], "bucket_id")
	} else {
		t.Fatalf("Expected `bucket_id` field")
	}

	require.Equal(t, len(object), len(objs[0]))
	if expectedId, err = test_helpers.ConvertUint64(object["id"]); err != nil {
		t.Fatalf("Unexpected `id` type")
	}

	if actualId, err = test_helpers.ConvertUint64(objs[0]["id"]); err != nil {
		t.Fatalf("Unexpected `id` type")
	}

	require.Equal(t, expectedId, actualId)
	require.Equal(t, object["name"], objs[0]["name"])
}

func TestResultWithErr(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testResultWithErrCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := conn.Do(testCase.req).GetTyped(testCase.resp)
			if err == nil {
				t.Fatalf("Expected CRUD fails with error, but error is not received")
			}
			require.Contains(t, err.Error(), "Space \"invalid\" doesn't exist")
		})
	}
}

func TestBoolResult(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeTruncateRequest(spaceName).Opts(baseOpts)
	resp := crud.TruncateResult{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err.Error())
	}

	if resp.Value != true {
		t.Fatalf("Unexpected response value: %#v != %#v", resp.Value, true)
	}

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]interface{}{uint(i)})
		conn.Do(req).Get()
	}
}

func TestNumberResult(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeCountRequest(spaceName).Opts(countOpts)
	resp := crud.CountResult{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err.Error())
	}

	if resp.Value != 10 {
		t.Fatalf("Unexpected response value: %#v != %#v", resp.Value, 10)
	}

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]interface{}{uint(i)})
		conn.Do(req).Get()
	}
}

func TestBaseResult(t *testing.T) {
	expectedMetadata := []crud.FieldFormat{
		{
			Name:       "bucket_id",
			Type:       "unsigned",
			IsNullable: true,
		},
		{
			Name:       "id",
			Type:       "unsigned",
			IsNullable: false,
		},
		{
			Name:       "name",
			Type:       "string",
			IsNullable: false,
		},
	}

	conn := connect(t)
	defer conn.Close()

	req := crud.MakeSelectRequest(spaceName).Opts(selectOpts)
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err)
	}

	require.ElementsMatch(t, resp.Metadata, expectedMetadata)

	if len(resp.Rows.([]interface{})) != 10 {
		t.Fatalf("Unexpected rows: %#v", resp.Rows)
	}

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]interface{}{uint(i)})
		conn.Do(req).Get()
	}
}

func TestManyResult(t *testing.T) {
	expectedMetadata := []crud.FieldFormat{
		{
			Name:       "bucket_id",
			Type:       "unsigned",
			IsNullable: true,
		},
		{
			Name:       "id",
			Type:       "unsigned",
			IsNullable: false,
		},
		{
			Name:       "name",
			Type:       "string",
			IsNullable: false,
		},
	}

	conn := connect(t)
	defer conn.Close()

	req := crud.MakeReplaceManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts)
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err.Error())
	}

	require.ElementsMatch(t, resp.Metadata, expectedMetadata)

	if len(resp.Rows.([]interface{})) != 10 {
		t.Fatalf("Unexpected rows: %#v", resp.Rows)
	}

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]interface{}{uint(i)})
		conn.Do(req).Get()
	}
}

func TestStorageInfoResult(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeStorageInfoRequest().Opts(baseOpts)
	resp := crud.StorageInfoResult{}

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err.Error())
	}

	if resp.Info == nil {
		t.Fatalf("Failed to Do CRUD storage info request")
	}

	for _, info := range resp.Info {
		if info.Status != "running" {
			t.Fatalf("Unexpected Status: %s != running", info.Status)
		}

		if info.IsMaster != true {
			t.Fatalf("Unexpected IsMaster: %v != true", info.IsMaster)
		}

		if msg := info.Message; msg != "" {
			t.Fatalf("Unexpected Message: %s", msg)
		}
	}
}

func TestGetAdditionalOpts(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeGetRequest(spaceName).Key(key).Opts(crud.GetOpts{
		Timeout:       crud.MakeOptUint(1),
		Fields:        crud.MakeOptTuple([]interface{}{"name"}),
		Mode:          crud.MakeOptString("read"),
		PreferReplica: crud.MakeOptBool(true),
		Balance:       crud.MakeOptBool(true),
	})
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	if err != nil {
		t.Fatalf("Failed to Do CRUD request: %s", err)
	}
}

var testMetadataCases = []struct {
	name string
	req  tarantool.Request
}{
	{
		"Insert",
		crud.MakeInsertRequest(spaceName).
			Tuple(tuple).
			Opts(crud.InsertOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertObject",
		crud.MakeInsertObjectRequest(spaceName).
			Object(object).
			Opts(crud.InsertObjectOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertMany",
		crud.MakeInsertManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.InsertManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertObjectMany",
		crud.MakeInsertObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.InsertObjectManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Replace",
		crud.MakeReplaceRequest(spaceName).
			Tuple(tuple).
			Opts(crud.ReplaceOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceObject",
		crud.MakeReplaceObjectRequest(spaceName).
			Object(object).
			Opts(crud.ReplaceObjectOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceMany",
		crud.MakeReplaceManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.ReplaceManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceObjectMany",
		crud.MakeReplaceObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.ReplaceObjectManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Upsert",
		crud.MakeUpsertRequest(spaceName).
			Tuple(tuple).
			Operations(operations).
			Opts(crud.UpsertOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertObject",
		crud.MakeUpsertObjectRequest(spaceName).
			Object(object).
			Operations(operations).
			Opts(crud.UpsertObjectOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertMany",
		crud.MakeUpsertManyRequest(spaceName).
			TuplesOperationsData(tuplesOperationsData).
			Opts(crud.UpsertManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertObjectMany",
		crud.MakeUpsertObjectManyRequest(spaceName).
			ObjectsOperationsData(objectsOperationData).
			Opts(crud.UpsertObjectManyOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Select",
		crud.MakeSelectRequest(spaceName).
			Conditions(conditions).
			Opts(crud.SelectOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Get",
		crud.MakeGetRequest(spaceName).
			Key(key).
			Opts(crud.GetOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Update",
		crud.MakeUpdateRequest(spaceName).
			Key(key).
			Operations(operations).
			Opts(crud.UpdateOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Delete",
		crud.MakeDeleteRequest(spaceName).
			Key(key).
			Opts(crud.DeleteOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Min",
		crud.MakeMinRequest(spaceName).
			Opts(crud.MinOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
	{
		"Max",
		crud.MakeMaxRequest(spaceName).
			Opts(crud.MaxOpts{
				FetchLatestMetadata: crud.MakeOptBool(true),
			}),
	},
}

func TestFetchLatestMetadataOption(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testMetadataCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}

			resp := crud.Result{}

			err := conn.Do(testCase.req).GetTyped(&resp)
			if err != nil {
				t.Fatalf("Failed to Do CRUD request: %s", err)
			}

			if len(resp.Metadata) == 0 {
				t.Fatalf("Failed to get relevant metadata")
			}

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}
		})
	}
}

var testNoreturnCases = []struct {
	name string
	req  tarantool.Request
}{
	{
		"Insert",
		crud.MakeInsertRequest(spaceName).
			Tuple(tuple).
			Opts(crud.InsertOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertObject",
		crud.MakeInsertObjectRequest(spaceName).
			Object(object).
			Opts(crud.InsertObjectOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertMany",
		crud.MakeInsertManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.InsertManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"InsertObjectMany",
		crud.MakeInsertObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.InsertObjectManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"Replace",
		crud.MakeReplaceRequest(spaceName).
			Tuple(tuple).
			Opts(crud.ReplaceOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceObject",
		crud.MakeReplaceObjectRequest(spaceName).
			Object(object).
			Opts(crud.ReplaceObjectOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceMany",
		crud.MakeReplaceManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.ReplaceManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"ReplaceObjectMany",
		crud.MakeReplaceObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.ReplaceObjectManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"Upsert",
		crud.MakeUpsertRequest(spaceName).
			Tuple(tuple).
			Operations(operations).
			Opts(crud.UpsertOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertObject",
		crud.MakeUpsertObjectRequest(spaceName).
			Object(object).
			Operations(operations).
			Opts(crud.UpsertObjectOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertMany",
		crud.MakeUpsertManyRequest(spaceName).
			TuplesOperationsData(tuplesOperationsData).
			Opts(crud.UpsertManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"UpsertObjectMany",
		crud.MakeUpsertObjectManyRequest(spaceName).
			ObjectsOperationsData(objectsOperationData).
			Opts(crud.UpsertObjectManyOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"Update",
		crud.MakeUpdateRequest(spaceName).
			Key(key).
			Operations(operations).
			Opts(crud.UpdateOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
	{
		"Delete",
		crud.MakeDeleteRequest(spaceName).
			Key(key).
			Opts(crud.DeleteOpts{
				Noreturn: crud.MakeOptBool(true),
			}),
	},
}

func TestNoreturnOption(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testNoreturnCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}

			resp, err := conn.Do(testCase.req).Get()
			if err != nil {
				t.Fatalf("Failed to Do CRUD request: %s", err)
			}

			if len(resp.Data) == 0 {
				t.Fatalf("Expected explicit nil")
			}

			if resp.Data[0] != nil {
				t.Fatalf("Expected nil result, got %v", resp.Data[0])
			}

			if len(resp.Data) >= 2 && resp.Data[1] != nil {
				t.Fatalf("Expected no returned errors, got %v", resp.Data[1])
			}

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}
		})
	}
}

func TestNoreturnOptionTyped(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	for _, testCase := range testNoreturnCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}

			resp := crud.Result{}

			err := conn.Do(testCase.req).GetTyped(&resp)
			if err != nil {
				t.Fatalf("Failed to Do CRUD request: %s", err)
			}

			if resp.Rows != nil {
				t.Fatalf("Expected nil rows, got %v", resp.Rows)
			}

			if len(resp.Metadata) != 0 {
				t.Fatalf("Expected no metadata")
			}

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]interface{}{uint(i)})
				conn.Do(req).Get()
			}
		})
	}
}

func getTestSchema(t *testing.T) crud.Schema {
	schema := crud.Schema{
		"test": crud.SpaceSchema{
			Format: []crud.FieldFormat{
				crud.FieldFormat{
					Name:       "id",
					Type:       "unsigned",
					IsNullable: false,
				},
				{
					Name:       "bucket_id",
					Type:       "unsigned",
					IsNullable: true,
				},
				{
					Name:       "name",
					Type:       "string",
					IsNullable: false,
				},
			},
			Indexes: map[uint32]crud.Index{
				0: {
					Id:     0,
					Name:   "primary_index",
					Type:   "TREE",
					Unique: true,
					Parts: []crud.IndexPart{
						{
							Fieldno:     1,
							Type:        "unsigned",
							ExcludeNull: false,
							IsNullable:  false,
						},
					},
				},
			},
		},
	}

	// https://github.com/tarantool/tarantool/issues/4091
	uniqueIssue, err := test_helpers.IsTarantoolVersionLess(2, 2, 1)
	require.Equal(t, err, nil, "expected version check to succeed")

	if uniqueIssue {
		for sk, sv := range schema {
			for ik, iv := range sv.Indexes {
				iv.Unique = false
				sv.Indexes[ik] = iv
			}
			schema[sk] = sv
		}
	}

	// https://github.com/tarantool/tarantool/commit/17c9c034933d726925910ce5bf8b20e8e388f6e3
	excludeNullUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 8, 1)
	require.Equal(t, err, nil, "expected version check to succeed")

	if excludeNullUnsupported {
		for sk, sv := range schema {
			for ik, iv := range sv.Indexes {
				for pk, pv := range iv.Parts {
					// Struct default value.
					pv.ExcludeNull = false
					iv.Parts[pk] = pv
				}
				sv.Indexes[ik] = iv
			}
			schema[sk] = sv
		}
	}

	return schema
}

func TestSchemaTyped(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeSchemaRequest()
	var result crud.SchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.Equal(t, err, nil, "Expected CRUD request to succeed")
	require.Equal(t, result.Value, getTestSchema(t), "map with \"test\" schema expected")
}

func TestSpaceSchemaTyped(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeSchemaRequest().Space("test")
	var result crud.SpaceSchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.Equal(t, err, nil, "Expected CRUD request to succeed")
	require.Equal(t, result.Value, getTestSchema(t)["test"], "map with \"test\" schema expected")
}

func TestSpaceSchemaTypedError(t *testing.T) {
	conn := connect(t)
	defer conn.Close()

	req := crud.MakeSchemaRequest().Space("not_exist")
	var result crud.SpaceSchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.NotEqual(t, err, nil, "Expected CRUD request to fail")
	require.Regexp(t, "Space \"not_exist\" doesn't exist", err.Error())
}

func TestUnitEmptySchema(t *testing.T) {
	// We need to create another cluster with no spaces
	// to test `{}` schema, so let's at least add a unit test.
	conn := connect(t)
	defer conn.Close()

	req := tarantool.NewEvalRequest("return {}")
	var result crud.SchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.Equal(t, err, nil, "Expected CRUD request to succeed")
	require.Equal(t, result.Value, crud.Schema{}, "empty schema expected")
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(startOpts)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
