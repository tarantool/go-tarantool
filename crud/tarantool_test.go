package crud_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-option"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/crud"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

var server = "127.0.0.1:3013"
var spaceNo = uint32(617)
var spaceName = "test"
var invalidSpaceName = "invalid"
var indexNo = uint32(0)
var indexName = "primary_index"

var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

var opts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	Dialer:       dialer,
	InitScript:   "testdata/config.lua",
	Listen:       server,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 10,
	RetryTimeout: 500 * time.Millisecond,
}

var timeout = float64(1.1)

var operations = []crud.Operation{
	// Insert new fields,
	// because double update of the same field results in an error.
	{
		Operator: crud.Insert,
		Field:    4,
		Value:    0,
	},
	{
		Operator: crud.Insert,
		Field:    5,
		Value:    0,
	},
	{
		Operator: crud.Insert,
		Field:    6,
		Value:    0,
	},
	{
		Operator: crud.Insert,
		Field:    7,
		Value:    0,
	},
	{
		Operator: crud.Insert,
		Field:    8,
		Value:    0,
	},
	{
		Operator: crud.Add,
		Field:    4,
		Value:    1,
	},
	{
		Operator: crud.Sub,
		Field:    5,
		Value:    1,
	},
	{
		Operator: crud.And,
		Field:    6,
		Value:    1,
	},
	{
		Operator: crud.Or,
		Field:    7,
		Value:    1,
	},
	{
		Operator: crud.Xor,
		Field:    8,
		Value:    1,
	},
	{
		Operator: crud.Delete,
		Field:    4,
		Value:    5,
	},
	{
		Operator: crud.Assign,
		Field:    "name",
		Value:    "bye",
	},
}

var selectOpts = crud.SelectOpts{
	Timeout: option.SomeFloat64(timeout),
}

var countOpts = crud.CountOpts{
	Timeout: option.SomeFloat64(timeout),
}

var getOpts = crud.GetOpts{
	Timeout: option.SomeFloat64(timeout),
}

var minOpts = crud.MinOpts{
	Timeout: option.SomeFloat64(timeout),
}

var maxOpts = crud.MaxOpts{
	Timeout: option.SomeFloat64(timeout),
}

var baseOpts = crud.BaseOpts{
	Timeout: option.SomeFloat64(timeout),
}

var simpleOperationOpts = crud.SimpleOperationOpts{
	Timeout: option.SomeFloat64(timeout),
}

var simpleOperationObjectOpts = crud.SimpleOperationObjectOpts{
	Timeout: option.SomeFloat64(timeout),
}

var opManyOpts = crud.OperationManyOpts{
	Timeout: option.SomeFloat64(timeout),
}

var opObjManyOpts = crud.OperationObjectManyOpts{
	Timeout: option.SomeFloat64(timeout),
}

var schemaOpts = crud.SchemaOpts{
	Timeout: option.SomeFloat64(timeout),
	Cached:  option.SomeBool(false),
}

var conditions = []crud.Condition{
	{
		Operator: crud.Lt,
		Field:    "id",
		Value:    uint(1020),
	},
}

var key = []any{uint(1019)}

var tuples = generateTuples()
var objects = generateObjects()

var tuple = []any{uint(1019), nil, "bla"}
var object = crud.MapObject{
	"id":   uint(1019),
	"name": "bla",
}

func connect(tb testing.TB) *tarantool.Connection {
	tb.Helper()

	for range 10 {
		ctx, cancel := test_helpers.GetConnectContext()
		conn, err := tarantool.Connect(ctx, dialer, opts)
		cancel()
		require.NoError(tb, err, "Failed to connect")

		ret := struct {
			_msgpack struct{} `msgpack:",asArray"`
			Result   bool
		}{}
		err = conn.Do(tarantool.NewCallRequest("is_ready")).GetTyped(&ret)
		require.NoError(tb, err, "Failed to check is_ready")

		if ret.Result {
			return conn
		}

		time.Sleep(time.Second)
	}

	require.Fail(tb, "Failed to wait for a ready state connect.")
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
	{
		"Schema",
		1,
		crud.MakeSchemaRequest().Opts(schemaOpts),
	},
}

var testResultWithErrCases = []struct {
	name string
	resp any
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
		crud.MakeReplaceManyRequest(invalidSpaceName).Tuples(tuples).Opts(opManyOpts),
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
		tpls = append(tpls, []any{uint(i), nil, "bla"})
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

func getCrudError(req tarantool.Request, crudError any) (any, error) {
	var err []any
	var ok bool

	rtype := req.Type()
	if crudError != nil {
		if rtype == iproto.IPROTO_CALL {
			return crudError, nil
		}

		if err, ok = crudError.([]any); !ok {
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
			[]any{uint(i), nil, "bla"})
		_, err := conn.Do(req).Get()
		require.NoError(t, err, "Unable to prepare tuples")
	}
}

func testSelectGeneratedData(t *testing.T, conn tarantool.Connector,
	expectedTuplesCount int) {
	t.Helper()

	req := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(20).
		Iterator(tarantool.IterGe).
		Key([]any{uint(1010)})
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to Select")
	require.Len(t, data, expectedTuplesCount, "Response Data len mismatch")
}

func testCrudRequestCheck(t *testing.T, req tarantool.Request,
	data []any, err error, expectedLen int) {
	t.Helper()

	require.NoError(t, err, "Failed to Do CRUD request")
	require.GreaterOrEqual(t, len(data), expectedLen, "Response Body len")

	// resp.Data[0] - CRUD res.
	// resp.Data[1] - CRUD err.
	if expectedLen >= 2 && data[1] != nil {
		crudErr, getErr := getCrudError(req, data[1])
		require.NoError(t, getErr, "Failed to get CRUD error")
		require.Nil(t, crudErr, "Failed to perform CRUD request on CRUD side")
	}
}

func TestCrudGenerateData(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testGenerateDataCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}

			data, err := conn.Do(testCase.req).Get()
			testCrudRequestCheck(t, testCase.req, data,
				err, testCase.expectedRespLen)

			testSelectGeneratedData(t, conn, testCase.expectedTuplesCount)

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}
		})
	}
}

func TestCrudProcessData(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testProcessDataCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCrudRequestPrepareData(t, conn)
			data, err := conn.Do(testCase.req).Get()
			testCrudRequestCheck(t, testCase.req, data,
				err, testCase.expectedRespLen)
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}
		})
	}
}

func TestCrudUpdateSplice(t *testing.T) {
	test_helpers.SkipIfCrudSpliceBroken(t)

	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeUpdateRequest(spaceName).
		Key(key).
		Operations([]crud.Operation{
			{
				Operator: crud.Splice,
				Field:    "name",
				Pos:      1,
				Len:      1,
				Replace:  "!!",
			},
		}).
		Opts(simpleOperationOpts)

	testCrudRequestPrepareData(t, conn)
	data, err := conn.Do(req).Get()
	testCrudRequestCheck(t, req, data,
		err, 2)
}

func TestCrudUpsertSplice(t *testing.T) {
	test_helpers.SkipIfCrudSpliceBroken(t)

	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeUpsertRequest(spaceName).
		Tuple(tuple).
		Operations([]crud.Operation{
			{
				Operator: crud.Splice,
				Field:    "name",
				Pos:      1,
				Len:      1,
				Replace:  "!!",
			},
		}).
		Opts(simpleOperationOpts)

	testCrudRequestPrepareData(t, conn)
	data, err := conn.Do(req).Get()
	testCrudRequestCheck(t, req, data,
		err, 2)
}

func TestCrudUpsertObjectSplice(t *testing.T) {
	test_helpers.SkipIfCrudSpliceBroken(t)

	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeUpsertObjectRequest(spaceName).
		Object(object).
		Operations([]crud.Operation{
			{
				Operator: crud.Splice,
				Field:    "name",
				Pos:      1,
				Len:      1,
				Replace:  "!!",
			},
		}).
		Opts(simpleOperationOpts)

	testCrudRequestPrepareData(t, conn)
	data, err := conn.Do(req).Get()
	testCrudRequestCheck(t, req, data,
		err, 2)
}

func TestUnflattenRows_IncorrectParams(t *testing.T) {
	invalidMetadata := []any{
		map[any]any{
			"name": true,
			"type": "number",
		},
		map[any]any{
			"name": "name",
			"type": "string",
		},
	}

	tpls := []any{
		tuple,
	}

	// Format tuples with invalid format with UnflattenRows.
	objs, err := crud.UnflattenRows(tpls, invalidMetadata)
	require.Nil(t, objs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected space format")
}

func TestUnflattenRows(t *testing.T) {
	var (
		ok         bool
		err        error
		expectedId uint64
		actualId   uint64
		res        map[any]any
		metadata   []any
		tpls       []any
	)

	conn := connect(t)
	defer func() { _ = conn.Close() }()

	// Do `replace`.
	req := crud.MakeReplaceRequest(spaceName).
		Tuple(tuple).
		Opts(simpleOperationOpts)
	data, err := conn.Do(req).Get()
	testCrudRequestCheck(t, req, data, err, 2)

	res, ok = data[0].(map[any]any)
	require.True(t, ok, "Unexpected CRUD result")

	rawMetadata, ok := res["metadata"]
	require.True(t, ok, "Failed to get CRUD metadata")
	metadata, ok = rawMetadata.([]any)
	require.True(t, ok, "Unexpected CRUD metadata")

	rawTuples, ok := res["rows"]
	require.True(t, ok, "Failed to get CRUD rows")
	tpls, ok = rawTuples.([]any)
	require.True(t, ok, "Unexpected CRUD rows")

	// Format `replace` result with UnflattenRows.
	objs, err := crud.UnflattenRows(tpls, metadata)
	require.NoError(t, err, "Failed to unflatten rows")
	require.GreaterOrEqual(t, len(objs), 1, "Unexpected unflatten rows result")

	_, ok = objs[0]["bucket_id"]
	require.True(t, ok, "Expected `bucket_id` field")
	delete(objs[0], "bucket_id")

	require.Len(t, object, len(objs[0]))
	expectedId, err = test_helpers.ConvertUint64(object["id"])
	require.NoError(t, err, "Unexpected `id` type")

	actualId, err = test_helpers.ConvertUint64(objs[0]["id"])
	require.NoError(t, err, "Unexpected `id` type")

	require.Equal(t, expectedId, actualId)
	require.Equal(t, object["name"], objs[0]["name"])
}

func TestResultWithErr(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testResultWithErrCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := conn.Do(testCase.req).GetTyped(testCase.resp)
			require.Error(t, err, "Expected CRUD fails with error, but error is not received")
			require.Contains(t, err.Error(), "Space \"invalid\" doesn't exist")
		})
	}
}

func TestBoolResult(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeTruncateRequest(spaceName).Opts(baseOpts)
	resp := crud.TruncateResult{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")
	require.True(t, resp.Value, "Unexpected response value")

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]any{uint(i)})
		_, err := conn.Do(req).Get()
		require.NoError(t, err)
	}
}

func TestNumberResult(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeCountRequest(spaceName).Opts(countOpts)
	resp := crud.CountResult{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")
	require.Equal(t, uint64(10), resp.Value, "Unexpected response value")

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]any{uint(i)})
		_, err = conn.Do(req).Get()
		require.NoError(t, err)
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
	defer func() { _ = conn.Close() }()

	req := crud.MakeSelectRequest(spaceName).Opts(selectOpts)
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")

	require.ElementsMatch(t, resp.Metadata, expectedMetadata)

	rows, ok := resp.Rows.([]any)
	require.True(t, ok, "Unexpected rows type")
	require.Len(t, rows, 10, "Unexpected rows count")

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]any{uint(i)})
		_, err = conn.Do(req).Get()
		require.NoError(t, err)
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
	defer func() { _ = conn.Close() }()

	req := crud.MakeReplaceManyRequest(spaceName).Tuples(tuples).Opts(opManyOpts)
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")

	require.ElementsMatch(t, resp.Metadata, expectedMetadata)

	rows, ok := resp.Rows.([]any)
	require.True(t, ok, "Unexpected rows type")
	require.Len(t, rows, 10, "Unexpected rows count")

	for i := 1010; i < 1020; i++ {
		req := tarantool.NewDeleteRequest(spaceName).
			Key([]any{uint(i)})
		_, err := conn.Do(req).Get()
		require.NoError(t, err)
	}
}

func TestStorageInfoResult(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeStorageInfoRequest().Opts(baseOpts)
	resp := crud.StorageInfoResult{}

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")
	require.NotNil(t, resp.Info, "Failed to Do CRUD storage info request")

	for _, info := range resp.Info {
		require.Equal(t, "running", info.Status, "Unexpected Status")
		require.True(t, info.IsMaster, "Unexpected IsMaster")
		require.Empty(t, info.Message, "Unexpected Message")
	}
}

func TestGetAdditionalOpts(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeGetRequest(spaceName).Key(key).Opts(crud.GetOpts{
		Timeout:       option.SomeFloat64(1.1),
		Fields:        option.SomeAny([]any{"name"}),
		Mode:          option.SomeString("read"),
		PreferReplica: option.SomeBool(true),
		Balance:       option.SomeBool(true),
	})
	resp := crud.Result{}

	testCrudRequestPrepareData(t, conn)

	err := conn.Do(req).GetTyped(&resp)
	require.NoError(t, err, "Failed to Do CRUD request")
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
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"InsertObject",
		crud.MakeInsertObjectRequest(spaceName).
			Object(object).
			Opts(crud.InsertObjectOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"InsertMany",
		crud.MakeInsertManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.InsertManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"InsertObjectMany",
		crud.MakeInsertObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.InsertObjectManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Replace",
		crud.MakeReplaceRequest(spaceName).
			Tuple(tuple).
			Opts(crud.ReplaceOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"ReplaceObject",
		crud.MakeReplaceObjectRequest(spaceName).
			Object(object).
			Opts(crud.ReplaceObjectOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"ReplaceMany",
		crud.MakeReplaceManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.ReplaceManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"ReplaceObjectMany",
		crud.MakeReplaceObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.ReplaceObjectManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Upsert",
		crud.MakeUpsertRequest(spaceName).
			Tuple(tuple).
			Operations(operations).
			Opts(crud.UpsertOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"UpsertObject",
		crud.MakeUpsertObjectRequest(spaceName).
			Object(object).
			Operations(operations).
			Opts(crud.UpsertObjectOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"UpsertMany",
		crud.MakeUpsertManyRequest(spaceName).
			TuplesOperationsData(tuplesOperationsData).
			Opts(crud.UpsertManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"UpsertObjectMany",
		crud.MakeUpsertObjectManyRequest(spaceName).
			ObjectsOperationsData(objectsOperationData).
			Opts(crud.UpsertObjectManyOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Select",
		crud.MakeSelectRequest(spaceName).
			Conditions(conditions).
			Opts(crud.SelectOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Get",
		crud.MakeGetRequest(spaceName).
			Key(key).
			Opts(crud.GetOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Update",
		crud.MakeUpdateRequest(spaceName).
			Key(key).
			Operations(operations).
			Opts(crud.UpdateOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Delete",
		crud.MakeDeleteRequest(spaceName).
			Key(key).
			Opts(crud.DeleteOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Min",
		crud.MakeMinRequest(spaceName).
			Opts(crud.MinOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
	{
		"Max",
		crud.MakeMaxRequest(spaceName).
			Opts(crud.MaxOpts{
				FetchLatestMetadata: option.SomeBool(true),
			}),
	},
}

func TestFetchLatestMetadataOption(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testMetadataCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}

			resp := crud.Result{}

			err := conn.Do(testCase.req).GetTyped(&resp)
			require.NoError(t, err, "Failed to Do CRUD request")
			require.NotEmpty(t, resp.Metadata, "Failed to get relevant metadata")

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err = conn.Do(req).Get()
				require.NoError(t, err)
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
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"InsertObject",
		crud.MakeInsertObjectRequest(spaceName).
			Object(object).
			Opts(crud.InsertObjectOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"InsertMany",
		crud.MakeInsertManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.InsertManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"InsertObjectMany",
		crud.MakeInsertObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.InsertObjectManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"Replace",
		crud.MakeReplaceRequest(spaceName).
			Tuple(tuple).
			Opts(crud.ReplaceOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"ReplaceObject",
		crud.MakeReplaceObjectRequest(spaceName).
			Object(object).
			Opts(crud.ReplaceObjectOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"ReplaceMany",
		crud.MakeReplaceManyRequest(spaceName).
			Tuples(tuples).
			Opts(crud.ReplaceManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"ReplaceObjectMany",
		crud.MakeReplaceObjectManyRequest(spaceName).
			Objects(objects).
			Opts(crud.ReplaceObjectManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"Upsert",
		crud.MakeUpsertRequest(spaceName).
			Tuple(tuple).
			Operations(operations).
			Opts(crud.UpsertOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"UpsertObject",
		crud.MakeUpsertObjectRequest(spaceName).
			Object(object).
			Operations(operations).
			Opts(crud.UpsertObjectOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"UpsertMany",
		crud.MakeUpsertManyRequest(spaceName).
			TuplesOperationsData(tuplesOperationsData).
			Opts(crud.UpsertManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"UpsertObjectMany",
		crud.MakeUpsertObjectManyRequest(spaceName).
			ObjectsOperationsData(objectsOperationData).
			Opts(crud.UpsertObjectManyOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"Update",
		crud.MakeUpdateRequest(spaceName).
			Key(key).
			Operations(operations).
			Opts(crud.UpdateOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
	{
		"Delete",
		crud.MakeDeleteRequest(spaceName).
			Key(key).
			Opts(crud.DeleteOpts{
				Noreturn: option.SomeBool(true),
			}),
	},
}

func TestNoreturnOption(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testNoreturnCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}

			data, err := conn.Do(testCase.req).Get()
			require.NoError(t, err, "Failed to Do CRUD request")
			require.NotEmpty(t, data, "Expected data with explicit nil value")
			require.Nil(t, data[0], "Expected nil result")
			if len(data) >= 2 {
				require.Nil(t, data[1], "Expected no returned errors")
			}

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err = conn.Do(req).Get()
				require.NoError(t, err)
			}
		})
	}
}

func TestNoreturnOptionTyped(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testNoreturnCases {
		t.Run(testCase.name, func(t *testing.T) {
			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err := conn.Do(req).Get()
				require.NoError(t, err)
			}

			resp := crud.Result{}

			err := conn.Do(testCase.req).GetTyped(&resp)
			require.NoError(t, err, "Failed to Do CRUD request")
			require.Nil(t, resp.Rows, "Expected nil rows")
			require.Empty(t, resp.Metadata, "Expected no metadata")

			for i := 1010; i < 1020; i++ {
				req := tarantool.NewDeleteRequest(spaceName).
					Key([]any{uint(i)})
				_, err = conn.Do(req).Get()
				require.NoError(t, err)
			}
		})
	}
}

func getTestSchema(t *testing.T) crud.Schema {
	t.Helper()

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
	require.NoError(t, err, "expected version check to succeed")

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
	require.NoError(t, err, "expected version check to succeed")

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
	defer func() { _ = conn.Close() }()

	req := crud.MakeSchemaRequest()
	var result crud.SchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.NoError(t, err, "Expected CRUD request to succeed")
	require.Equal(t, getTestSchema(t), result.Value, "map with \"test\" schema expected")
}

func TestSpaceSchemaTyped(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeSchemaRequest().Space("test")
	var result crud.SpaceSchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.NoError(t, err, "Expected CRUD request to succeed")
	require.Equal(t, getTestSchema(t)["test"], result.Value, "map with \"test\" schema expected")
}

func TestSpaceSchemaTypedError(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := crud.MakeSchemaRequest().Space("not_exist")
	var result crud.SpaceSchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.Error(t, err, "Expected CRUD request to fail")
	require.Regexp(t, "Space \"not_exist\" doesn't exist", err.Error())
}

func TestUnitEmptySchema(t *testing.T) {
	// We need to create another cluster with no spaces
	// to test `{}` schema, so let's at least add a unit test.
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	req := tarantool.NewEvalRequest("return {}")
	var result crud.SchemaResult

	err := conn.Do(req).GetTyped(&result)
	require.NoError(t, err, "Expected CRUD request to succeed")
	require.Equal(t, crud.Schema{}, result.Value, "empty schema expected")
}

var testStorageYieldCases = []struct {
	name string
	req  tarantool.Request
}{
	{
		"Count",
		crud.MakeCountRequest(spaceName).
			Opts(crud.CountOpts{
				YieldEvery: option.SomeUint(500),
			}),
	},
	{
		"Select",
		crud.MakeSelectRequest(spaceName).
			Opts(crud.SelectOpts{
				YieldEvery: option.SomeUint(500),
			}),
	},
}

func TestYieldEveryOption(t *testing.T) {
	conn := connect(t)
	defer func() { _ = conn.Close() }()

	for _, testCase := range testStorageYieldCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := conn.Do(testCase.req).Get()
			require.NoError(t, err, "Failed to Do CRUD request")
		})
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(startOpts)
	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}
	defer test_helpers.StopTarantoolWithCleanup(inst)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
