package crud_test

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/crud"
)

const (
	exampleServer = "127.0.0.1:3013"
	exampleSpace  = "test"
)

var exampleOpts = tarantool.Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

func exampleConnect() *tarantool.Connection {
	conn, err := tarantool.Connect(exampleServer, exampleOpts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
	return conn
}

// ExampleResult_rowsInterface demonstrates how to use a helper type Result
// to decode a crud response. In this example, rows are decoded as an
// interface{} type.
func ExampleResult_rowsInterface() {
	conn := exampleConnect()
	req := crud.MakeReplaceRequest(exampleSpace).
		Tuple([]interface{}{uint(2010), nil, "bla"})

	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		fmt.Printf("Failed to execute request: %s", err)
		return
	}

	fmt.Println(ret.Metadata)
	fmt.Println(ret.Rows)
	// Output:
	// [{id unsigned false} {bucket_id unsigned true} {name string false}]
	// [[2010 45 bla]]
}

// ExampleResult_rowsCustomType demonstrates how to use a helper type Result
// to decode a crud response. In this example, rows are decoded as a
// custom type.
func ExampleResult_rowsCustomType() {
	conn := exampleConnect()
	req := crud.MakeReplaceRequest(exampleSpace).
		Tuple([]interface{}{uint(2010), nil, "bla"})

	type Tuple struct {
		_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
		Id       uint64
		BucketId uint64
		Name     string
	}
	ret := crud.MakeResult(reflect.TypeOf(Tuple{}))

	if err := conn.Do(req).GetTyped(&ret); err != nil {
		fmt.Printf("Failed to execute request: %s", err)
		return
	}

	fmt.Println(ret.Metadata)
	rows := ret.Rows.([]Tuple)
	fmt.Println(rows)
	// Output:
	// [{id unsigned false} {bucket_id unsigned true} {name string false}]
	// [{{} 2010 45 bla}]
}

// ExampleResult_many demonstrates that there is no difference in a
// response from *ManyRequest.
func ExampleResult_many() {
	conn := exampleConnect()
	req := crud.MakeReplaceManyRequest(exampleSpace).
		Tuples([]crud.Tuple{
			[]interface{}{uint(2010), nil, "bla"},
			[]interface{}{uint(2011), nil, "bla"},
		})

	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		fmt.Printf("Failed to execute request: %s", err)
		return
	}

	fmt.Println(ret.Metadata)
	fmt.Println(ret.Rows)
	// Output:
	// [{id unsigned false} {bucket_id unsigned true} {name string false}]
	// [[2010 45 bla] [2011 4 bla]]
}

// ExampleResult_error demonstrates how to use a helper type Result
// to handle a crud error.
func ExampleResult_error() {
	conn := exampleConnect()
	req := crud.MakeReplaceRequest("not_exist").
		Tuple([]interface{}{uint(2010), nil, "bla"})

	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		crudErr := err.(crud.Error)
		fmt.Printf("Failed to execute request: %s", crudErr)
	} else {
		fmt.Println(ret.Metadata)
		fmt.Println(ret.Rows)
	}
	// Output:
	// Failed to execute request: ReplaceError: Space "not_exist" doesn't exist
}

// ExampleResult_errorMany demonstrates how to use a helper type Result
// to handle a crud error for a *ManyRequest.
func ExampleResult_errorMany() {
	conn := exampleConnect()
	initReq := crud.MakeReplaceRequest("not_exist").
		Tuple([]interface{}{uint(2010), nil, "bla"})
	if _, err := conn.Do(initReq).Get(); err != nil {
		fmt.Printf("Failed to initialize the example: %s\n", err)
	}

	req := crud.MakeInsertManyRequest(exampleSpace).
		Tuples([]crud.Tuple{
			[]interface{}{uint(2010), nil, "bla"},
			[]interface{}{uint(2010), nil, "bla"},
		})
	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		crudErr := err.(crud.ErrorMany)
		// We need to trim the error message to make the example repeatable.
		errmsg := crudErr.Error()[:10]
		fmt.Printf("Failed to execute request: %s", errmsg)
	} else {
		fmt.Println(ret.Metadata)
		fmt.Println(ret.Rows)
	}
	// Output:
	// Failed to execute request: CallError:
}
