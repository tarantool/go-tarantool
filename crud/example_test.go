package crud_test

import (
	"fmt"
	"reflect"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/crud"
)

const (
	exampleServer = "127.0.0.1:3013"
	exampleSpace  = "test"
)

var exampleOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
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

// ExampleResult_operationData demonstrates how to obtain information
// about erroneous objects from crud.Error using `OperationData` field.
func ExampleResult_operationData() {
	conn := exampleConnect()
	req := crud.MakeInsertObjectManyRequest(exampleSpace).Objects([]crud.Object{
		crud.MapObject{
			"id":        2,
			"bucket_id": 3,
			"name":      "Makar",
		},
		crud.MapObject{
			"id":        2,
			"bucket_id": 3,
			"name":      "Vasya",
		},
		crud.MapObject{
			"id":        3,
			"bucket_id": 5,
		},
	})

	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		crudErrs := err.(crud.ErrorMany)
		fmt.Println("Erroneous data:")
		for _, crudErr := range crudErrs.Errors {
			fmt.Println(crudErr.OperationData)
		}
	} else {
		fmt.Println(ret.Metadata)
		fmt.Println(ret.Rows)
	}

	// Output:
	// Erroneous data:
	// [2 3 Vasya]
	// map[bucket_id:5 id:3]
}

// ExampleResult_operationDataCustomType demonstrates the ability
// to cast `OperationData` field, extracted from a CRUD error during decoding
// using crud.Result, to a custom type.
// The type of `OperationData` is determined as the crud.Result row type.
func ExampleResult_operationDataCustomType() {
	conn := exampleConnect()
	req := crud.MakeInsertObjectManyRequest(exampleSpace).Objects([]crud.Object{
		crud.MapObject{
			"id":        1,
			"bucket_id": 3,
			"name":      "Makar",
		},
		crud.MapObject{
			"id":        1,
			"bucket_id": 3,
			"name":      "Vasya",
		},
		crud.MapObject{
			"id":        3,
			"bucket_id": 5,
		},
	})

	type Tuple struct {
		Id       uint64 `msgpack:"id,omitempty"`
		BucketId uint64 `msgpack:"bucket_id,omitempty"`
		Name     string `msgpack:"name,omitempty"`
	}

	ret := crud.MakeResult(reflect.TypeOf(Tuple{}))
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		crudErrs := err.(crud.ErrorMany)
		fmt.Println("Erroneous data:")
		for _, crudErr := range crudErrs.Errors {
			operationData := crudErr.OperationData.(Tuple)
			fmt.Println(operationData)
		}
	} else {
		fmt.Println(ret.Metadata)
		fmt.Println(ret.Rows)
	}
	// Output:
	// Erroneous data:
	// {1 3 Vasya}
	// {3 5 }
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

// ExampleResult_noreturn demonstrates noreturn request: a data change
// request where you don't need to retrieve the result, just want to know
// whether it was successful or not.
func ExampleResult_noreturn() {
	conn := exampleConnect()
	req := crud.MakeReplaceManyRequest(exampleSpace).
		Tuples([]crud.Tuple{
			[]interface{}{uint(2010), nil, "bla"},
			[]interface{}{uint(2011), nil, "bla"},
		}).
		Opts(crud.ReplaceManyOpts{
			Noreturn: crud.MakeOptBool(true),
		})

	ret := crud.Result{}
	if err := conn.Do(req).GetTyped(&ret); err != nil {
		fmt.Printf("Failed to execute request: %s", err)
		return
	}

	fmt.Println(ret.Metadata)
	fmt.Println(ret.Rows)
	// Output:
	// []
	// <nil>
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

func ExampleSelectRequest_pagination() {
	conn := exampleConnect()

	const (
		fromTuple = 5
		allTuples = 10
	)
	var tuple interface{}
	for i := 0; i < allTuples; i++ {
		req := crud.MakeReplaceRequest(exampleSpace).
			Tuple([]interface{}{uint(3000 + i), nil, "bla"})
		ret := crud.Result{}
		if err := conn.Do(req).GetTyped(&ret); err != nil {
			fmt.Printf("Failed to initialize the example: %s\n", err)
			return
		}
		if i == fromTuple {
			tuple = ret.Rows.([]interface{})[0]
		}
	}

	req := crud.MakeSelectRequest(exampleSpace).
		Opts(crud.SelectOpts{
			First: crud.MakeOptInt(2),
			After: crud.MakeOptTuple(tuple),
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
	// [[3006 32 bla] [3007 33 bla]]
}
