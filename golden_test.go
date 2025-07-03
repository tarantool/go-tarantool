package tarantool_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
)

// golden_test.go contains tests that will check that the msgpack
// encoding of the requests body matches the golden files.
//
// Algorithm to add new test:
// 1. Create a new test record in TestGolden (name + Request).
// 2. Run the test with the flag -generate-golden to generate the golden file.
//    (for example: `go test . -run=TestGolden -v -generate-golden`)
// 3. Verify that JSON representation of the msgpack is the same as expected.
// 4. Commit the test record.
//
// Example of JSON representation of the msgpack
// ```
//    golden_test.go:80: writing golden file testdata/requests/select-with-optionals.msgpack
//    golden_test.go:38: {
//    golden_test.go:38:   "IPROTO_FETCH_POSITION[31]": true,
//    golden_test.go:38:   "IPROTO_INDEX_ID[17]": 0,
//    golden_test.go:38:   "IPROTO_ITERATOR[20]": 5,
//    golden_test.go:38:   "IPROTO_KEY[32]": [],
//    golden_test.go:38:   "IPROTO_LIMIT[18]": 123,
//    golden_test.go:38:   "IPROTO_OFFSET[19]": 123,
//    golden_test.go:38:   "IPROTO_SPACE_NAME[94]": "table_name"
//    golden_test.go:38: }
// ```
//
//
// In case of any changes in the msgpack encoding/tests, the test will fail with next error:
//
// ```
// === RUN   TestGolden/testdata/requests/select-with-after.msgpack
//    golden_test.go:109:
//        	Error Trace:	../go-tarantool/golden_test.go:73
//        	            				../go-tarantool/golden_test.go:109
//        	Error:      	Not equal:
//        	            	expected: []byte{0x87, 0x14, 0x5, 0x13, 0x0, ..., 0x33, 0x33, 0x33}
//        	            	actual  : []byte{0x87, 0x14, 0x5, 0x13, 0x0, ..., 0x33, 0x33, 0x33}
//
//        	            	Diff:
//        	            	--- Expected
//        	            	+++ Actual
//        	            	@@ -1,3 +1,3 @@
//        	            	 ([]uint8) (len=105) {
//        	            	- 00000000  87 14 05 13 ... 6e  |......{^.table_n|
//        	            	+ 00000000  87 14 05 13 ... 6e  |......|^.table_n|
//        	            	  00000010  61 6d 65 11 ... ff  |ame.. ...args...|
//        	Test:       	TestGolden/testdata/requests/select-with-after.msgpack
//        	Messages:   	golden file content is not equal to actual
//    golden_test.go:109: expected:
//    golden_test.go:63: {
//    golden_test.go:63:   "IPROTO_AFTER_TUPLE[47]": [
//    golden_test.go:63:     1,
//    golden_test.go:63:     "args",
//    golden_test.go:63:     3,
//    golden_test.go:63:     "2024-01-01T03:00:00+03:00",
//    golden_test.go:63:     "gZMIqvDBS3SYYcSrWiZjCA==",
//    golden_test.go:63:     1.2
//    golden_test.go:63:   ],
//    golden_test.go:63:   "IPROTO_INDEX_ID[17]": 0,
//    golden_test.go:63:   "IPROTO_ITERATOR[20]": 5,
//    golden_test.go:63:   "IPROTO_KEY[32]": [
//    golden_test.go:63:     1,
//    golden_test.go:63:     "args",
//    golden_test.go:63:     3,
//    golden_test.go:63:     "2024-01-01T03:00:00+03:00",
//    golden_test.go:63:     "gZMIqvDBS3SYYcSrWiZjCA==",
//    golden_test.go:63:     1.2
//    golden_test.go:63:   ],
//    golden_test.go:63:   "IPROTO_LIMIT[18]": 123,
//    golden_test.go:63:   "IPROTO_OFFSET[19]": 0,
//    golden_test.go:63:   "IPROTO_SPACE_NAME[94]": "table_name"
//    golden_test.go:63: }
//    golden_test.go:109: actual:
//    golden_test.go:63: {
//    golden_test.go:63:   "IPROTO_AFTER_TUPLE[47]": [
//    golden_test.go:63:     1,
//    golden_test.go:63:     "args",
//    golden_test.go:63:     3,
//    golden_test.go:63:     "2024-01-01T03:00:00+03:00",
//    golden_test.go:63:     "gZMIqvDBS3SYYcSrWiZjCA==",
//    golden_test.go:63:     1.2
//    golden_test.go:63:   ],
//    golden_test.go:63:   "IPROTO_INDEX_ID[17]": 0,
//    golden_test.go:63:   "IPROTO_ITERATOR[20]": 5,
//    golden_test.go:63:   "IPROTO_KEY[32]": [
//    golden_test.go:63:     1,
//    golden_test.go:63:     "args",
//    golden_test.go:63:     3,
//    golden_test.go:63:     "2024-01-01T03:00:00+03:00",
//    golden_test.go:63:     "gZMIqvDBS3SYYcSrWiZjCA==",
//    golden_test.go:63:     1.2
//    golden_test.go:63:   ],
//    golden_test.go:63:   "IPROTO_LIMIT[18]": 124,
//    golden_test.go:63:   "IPROTO_OFFSET[19]": 0,
//    golden_test.go:63:   "IPROTO_SPACE_NAME[94]": "table_name"
//    golden_test.go:63: }
// --- FAIL: TestGolden/testdata/requests/select-with-after.msgpack (0.00s)
// ```
// Use it to debug the test.
//
// If you want to update the golden file, run delete old file and rerun the test.

func logMsgpackAsJsonConvert(t *testing.T, data []byte) {
	t.Helper()

	var decodedMsgpack map[int]interface{}

	decoder := msgpack.NewDecoder(bytes.NewReader(data))
	require.NoError(t, decoder.Decode(&decodedMsgpack))

	decodedConvertedMsgpack := map[string]interface{}{}
	for k, v := range decodedMsgpack {
		decodedConvertedMsgpack[fmt.Sprintf("%s[%d]", iproto.Key(k).String(), k)] = v
	}

	encodedJson, err := json.MarshalIndent(decodedConvertedMsgpack, "", "  ")
	require.NoError(t, err, "failed to convert msgpack to json")

	for _, line := range bytes.Split(encodedJson, []byte("\n")) {
		t.Log(string(line))
	}
}

func compareGoldenMsgpackAndPrintDiff(t *testing.T, name string, data []byte) {
	t.Helper()

	testContent, err := os.ReadFile(name)
	require.NoError(t, err, "failed to read golden file", name)

	if assert.Equal(t, testContent, data, "golden file content is not equal to actual") {
		return
	}

	t.Logf("expected:\n")
	logMsgpackAsJsonConvert(t, testContent)
	t.Logf("actual:\n")
	logMsgpackAsJsonConvert(t, data)
}

func fileExists(name string) bool {
	_, err := os.Stat(name)
	return !os.IsNotExist(err)
}

const (
	pathPrefix = "testdata/requests"
)

var (
	generateGolden = flag.Bool("generate-golden", false,
		"generate golden files if they do not exist")
)

type goldenTestCase struct {
	Name     string
	Test     func(t *testing.T) tarantool.Request
	Request  tarantool.Request
	Resolver tarantool.SchemaResolver
}

func (tc goldenTestCase) Execute(t *testing.T) {
	t.Helper()

	if tc.Request != nil {
		if tc.Test != nil {
			require.FailNow(t, "both Test and Request must not be set")
		}

		tc.Test = func(t *testing.T) tarantool.Request {
			return tc.Request
		}
	}
	if tc.Resolver == nil {
		tc.Resolver = &dummySchemaResolver{}
	}

	name := path.Join(pathPrefix, tc.Name)

	t.Run(name, func(t *testing.T) {
		var out bytes.Buffer
		encoder := msgpack.NewEncoder(&out)

		req := tc.Test(t)
		require.NotNil(t, req, "failed to create request")

		err := req.Body(&dummySchemaResolver{}, encoder)
		require.NoError(t, err, "failed to encode request")

		goldenFileExists := fileExists(name)
		generateGoldenIsSet := *generateGolden && !goldenFileExists

		switch {
		case !goldenFileExists && generateGoldenIsSet:
			t.Logf("writing golden file %s", name)
			err := os.WriteFile(name, out.Bytes(), 0644)
			require.NoError(t, err, "failed to write golden file", name)
			logMsgpackAsJsonConvert(t, out.Bytes())
		case !goldenFileExists && !generateGoldenIsSet:
			assert.FailNow(t, "golden file does not exist")
		}

		compareGoldenMsgpackAndPrintDiff(t, name, out.Bytes())
	})
}

type dummySchemaResolver struct{}

func interfaceToUint32(in interface{}) (uint32, bool) {
	switch val := reflect.ValueOf(in); val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint32(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uint32(val.Uint()), true
	default:
		return 0, false
	}
}

func (d dummySchemaResolver) ResolveSpace(in interface{}) (uint32, error) {
	if num, ok := interfaceToUint32(in); ok {
		return num, nil
	}
	return 0, fmt.Errorf("unexpected space type %T", in)
}

func (d dummySchemaResolver) ResolveIndex(in interface{}, _ uint32) (uint32, error) {
	if in == nil {
		return 0, nil
	} else if num, ok := interfaceToUint32(in); ok {
		return num, nil
	}
	return 0, fmt.Errorf("unexpected index type %T", in)
}

func (d dummySchemaResolver) NamesUseSupported() bool {
	return true
}

func TestGolden(t *testing.T) {
	precachedUUID := uuid.MustParse("819308aa-f0c1-4b74-9861-c4ab5a266308")
	precachedTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	precachedTuple := []interface{}{1, "args", 3, precachedTime, precachedUUID, 1.2}

	precachedUpdateOps := tarantool.NewOperations().
		Add(1, "test").
		Assign(2, "fest").
		Delete(3, "").
		Insert(4, "insert").
		Splice(5, 6, 7, "splice").
		Subtract(6, "subtract").
		BitwiseAnd(7, 10).
		BitwiseOr(8, 11).
		BitwiseXor(9, 12)

	testCases := []goldenTestCase{
		{
			Name:    "commit-raw.msgpack",
			Request: tarantool.NewCommitRequest(),
		},
		{
			Name:    "commit-with-sync.msgpack",
			Request: tarantool.NewCommitRequest().IsSync(true),
		},
		{
			Name:    "commit-with-sync-false.msgpack",
			Request: tarantool.NewCommitRequest().IsSync(false),
		},
		{
			Name:    "begin.msgpack",
			Request: tarantool.NewBeginRequest(),
		},
		{
			Name: "begin-with-txn-isolation.msgpack",
			Request: tarantool.NewBeginRequest().
				TxnIsolation(tarantool.ReadCommittedLevel),
		},
		{
			Name: "begin-with-txn-isolation-is-sync.msgpack",
			Request: tarantool.NewBeginRequest().
				TxnIsolation(tarantool.ReadCommittedLevel).
				IsSync(true),
		},
		{
			Name: "begin-with-txn-isolation-is-sync-timeout.msgpack",
			Request: tarantool.NewBeginRequest().
				TxnIsolation(tarantool.ReadCommittedLevel).
				IsSync(true).
				Timeout(2 * time.Second),
		},
		{
			Name:    "rollback.msgpack",
			Request: tarantool.NewRollbackRequest(),
		},
		{
			Name:    "ping.msgpack",
			Request: tarantool.NewPingRequest(),
		},
		{
			Name:    "call-no-args.msgpack",
			Request: tarantool.NewCallRequest("function.name"),
		},
		{
			Name: "call-with-args.msgpack",
			Request: tarantool.NewCallRequest("function.name").Args(
				[]interface{}{1, 2, 3},
			),
		},
		{
			Name: "call-with-args-mixed.msgpack",
			Request: tarantool.NewCallRequest("function.name").Args(
				[]interface{}{1, "args", 3, precachedTime, precachedUUID, 1.2},
			),
		},
		{
			Name:    "call-with-args-nil.msgpack",
			Request: tarantool.NewCallRequest("function.name").Args(nil),
		},
		{
			Name:    "call-with-args-empty-array.msgpack",
			Request: tarantool.NewCallRequest("function.name").Args([]int{}),
		},
		{
			Name:    "eval.msgpack",
			Request: tarantool.NewEvalRequest("function_name()"),
		},
		{
			Name:    "eval-with-nil.msgpack",
			Request: tarantool.NewEvalRequest("function_name()").Args(nil),
		},
		{
			Name:    "eval-with-empty-array.msgpack",
			Request: tarantool.NewEvalRequest("function_name()").Args([]int{}),
		},
		{
			Name:    "eval-with-args.msgpack",
			Request: tarantool.NewEvalRequest("function_name(...)").Args(precachedTuple),
		},
		{
			Name:    "eval-with-single-number.msgpack",
			Request: tarantool.NewEvalRequest("function_name()").Args(1),
		},
		{
			Name:    "delete-raw.msgpack",
			Request: tarantool.NewDeleteRequest("table_name"),
		},
		{
			Name: "delete-snumber-inumber.msgpack",
			Request: tarantool.NewDeleteRequest(246).
				Index(123).Key([]interface{}{123}),
		},
		{
			Name: "delete-snumber-iname.msgpack",
			Request: tarantool.NewDeleteRequest(246).
				Index("index_name").Key([]interface{}{123}),
		},
		{
			Name: "delete-sname-inumber.msgpack",
			Request: tarantool.NewDeleteRequest("table_name").
				Index(123).Key([]interface{}{123}),
		},
		{
			Name: "delete-sname-iname.msgpack",
			Request: tarantool.NewDeleteRequest("table_name").
				Index("index_name").Key([]interface{}{123}),
		},
		{
			Name: "replace-sname.msgpack",
			Request: tarantool.NewReplaceRequest("table_name").
				Tuple(precachedTuple),
		},
		{
			Name: "replace-snumber.msgpack",
			Request: tarantool.NewReplaceRequest(123).
				Tuple(precachedTuple),
		},
		{
			Name: "insert-sname.msgpack",
			Request: tarantool.NewReplaceRequest("table_name").
				Tuple(precachedTuple),
		},
		{
			Name: "insert-snumber.msgpack",
			Request: tarantool.NewReplaceRequest(123).
				Tuple(precachedTuple),
		},
		{
			Name:    "call16.msgpack",
			Request: tarantool.NewCall16Request("function.name"),
		},
		{
			Name: "call16-with-args.msgpack",
			Request: tarantool.NewCall16Request("function.name").
				Args(precachedTuple),
		},
		{
			Name:    "call16-with-args-nil.msgpack",
			Request: tarantool.NewCall16Request("function.name").Args(nil),
		},
		{
			Name:    "call17.msgpack",
			Request: tarantool.NewCall17Request("function.name"),
		},
		{
			Name:    "call17-with-args-nil.msgpack",
			Request: tarantool.NewCall17Request("function.name").Args(nil),
		},
		{
			Name: "call17-with-args.msgpack",
			Request: tarantool.NewCall17Request("function.name").
				Args(precachedTuple),
		},
		{
			Name:    "update.msgpack",
			Request: tarantool.NewUpdateRequest("table_name"),
		},
		{
			Name: "update-snumber-iname.msgpack",
			Request: tarantool.NewUpdateRequest(123).
				Index("index_name").Key([]interface{}{123}).
				Operations(precachedUpdateOps),
		},
		{
			Name: "update-sname-iname.msgpack",
			Request: tarantool.NewUpdateRequest("table_name").
				Index("index_name").Key([]interface{}{123}).
				Operations(precachedUpdateOps),
		},
		{
			Name: "update-sname-inumber.msgpack",
			Request: tarantool.NewUpdateRequest("table_name").
				Index(123).Key([]interface{}{123}).
				Operations(precachedUpdateOps),
		},
		{
			Name:    "upsert.msgpack",
			Request: tarantool.NewUpsertRequest("table_name"),
		},
		{
			Name: "upsert-snumber.msgpack",
			Request: tarantool.NewUpsertRequest(123).
				Operations(precachedUpdateOps).
				Tuple(precachedTuple),
		},
		{
			Name: "upsert-sname.msgpack",
			Request: tarantool.NewUpsertRequest("table_name").
				Operations(precachedUpdateOps).
				Tuple(precachedTuple),
		},
		{
			Name:    "select",
			Request: tarantool.NewSelectRequest("table_name"),
		},
		{
			Name: "select-sname-iname.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Index("index_name"),
		},
		{
			Name: "select-sname-inumber.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Index(123),
		},
		{
			Name: "select-snumber-iname.msgpack",
			Request: tarantool.NewSelectRequest(123).
				Index("index_name"),
		},
		{
			Name: "select-snumber-inumber.msgpack",
			Request: tarantool.NewSelectRequest(123).
				Index(123),
		},
		{
			Name: "select-with-key.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Key(precachedTuple),
		},
		{
			Name: "select-key-sname-iname.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Index("index_name").Key(precachedTuple),
		},
		{
			Name: "select-key-sname-inumber.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Index(123).Key(precachedTuple),
		},
		{
			Name: "select-key-snumber-iname.msgpack",
			Request: tarantool.NewSelectRequest(123).
				Index("index_name").Key(precachedTuple),
		},
		{
			Name: "select-key-snumber-inumber.msgpack",
			Request: tarantool.NewSelectRequest(123).
				Index(123).Key(precachedTuple),
		},
		{
			Name: "select-with-optionals.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				Offset(123).Limit(123).Iterator(tarantool.IterGe).
				FetchPos(true),
		},
		{
			Name: "select-with-after.msgpack",
			Request: tarantool.NewSelectRequest("table_name").
				After(precachedTuple).Limit(123).Iterator(tarantool.IterGe).
				Key(precachedTuple),
		},
	}

	for _, tc := range testCases {
		tc.Execute(t)
	}
}
