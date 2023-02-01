package crud_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/markphelps/optional"
	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/crud"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var server = "127.0.0.1:3013"
var spaceName = "test"
var opts = tarantool.Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	InitScript:   "testdata/config.lua",
	Listen:       server,
	User:         opts.User,
	Pass:         opts.Pass,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 3,
	RetryTimeout: 500 * time.Millisecond,
}

var crudOpts = crud.SelectOpts{
	Timeout: optional.NewUint(1),
	First:   optional.NewInt(5),
}

var conditions = []crud.Condition{
	{
		Operator: "<",
		KeyName:  "id",
		KeyValue: uint(1020),
	},
}

var testProcessDataCases = []struct {
	name            string
	expectedRespLen int
	req             tarantool.Request
}{
	{
		"Select",
		2,
		crud.NewSelectRequest(spaceName).
			Conditions(conditions).
			Opts(crudOpts),
	},
}

func getCrudError(req tarantool.Request, crudError interface{}) (interface{}, error) {
	var err []interface{}
	var ok bool

	code := req.Code()
	if crudError != nil {
		if code == tarantool.Call17RequestCode {
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
	if expectedLen >= 2 {
		if crudErr, err := getCrudError(req, resp.Data[1]); err != nil {
			t.Fatalf("Failed to get CRUD error: %#v", err)
		} else if crudErr != nil {
			t.Fatalf("Failed to perform CRUD request on CRUD side: %#v", crudErr)
		}
	}
}

func TestCrudProcessData(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	for _, testCase := range testProcessDataCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCrudRequestPrepareData(t, conn)
			resp, err := conn.Do(testCase.req).Get()
			testCrudRequestCheck(t, testCase.req, resp,
				err, testCase.expectedRespLen)

			for i := 1010; i < 1020; i++ {
				conn.Delete(spaceName, nil, []interface{}{uint(i)})
			}
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
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
