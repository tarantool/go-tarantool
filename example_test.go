package tarantool_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Id       uint
	Msg      string
	Name     string
}

func exampleConnect(opts tarantool.Opts) *tarantool.Connection {
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
	return conn
}

// Example demonstrates how to use SSL transport.
func ExampleSslOpts() {
	var opts = tarantool.Opts{
		User:      "test",
		Pass:      "test",
		Transport: "ssl",
		Ssl: tarantool.SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	}
	_, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
}

func ExampleIntKey() {
	conn := exampleConnect(opts)
	defer conn.Close()

	const space = "test"
	const index = "primary"
	tuple := []interface{}{int(1111), "hello", "world"}
	conn.Do(tarantool.NewReplaceRequest(space).Tuple(tuple)).Get()

	var t []Tuple
	err := conn.Do(tarantool.NewSelectRequest(space).
		Index(index).
		Iterator(tarantool.IterEq).
		Key(tarantool.IntKey{1111}),
	).GetTyped(&t)
	fmt.Println("Error", err)
	fmt.Println("Data", t)
	// Output:
	// Error <nil>
	// Data [{{} 1111 hello world}]
}

func ExampleUintKey() {
	conn := exampleConnect(opts)
	defer conn.Close()

	const space = "test"
	const index = "primary"
	tuple := []interface{}{uint(1111), "hello", "world"}
	conn.Do(tarantool.NewReplaceRequest(space).Tuple(tuple)).Get()

	var t []Tuple
	err := conn.Do(tarantool.NewSelectRequest(space).
		Index(index).
		Iterator(tarantool.IterEq).
		Key(tarantool.UintKey{1111}),
	).GetTyped(&t)
	fmt.Println("Error", err)
	fmt.Println("Data", t)
	// Output:
	// Error <nil>
	// Data [{{} 1111 hello world}]
}

func ExampleStringKey() {
	conn := exampleConnect(opts)
	defer conn.Close()

	const space = "teststring"
	const index = "primary"
	tuple := []interface{}{"any", []byte{0x01, 0x02}}
	conn.Do(tarantool.NewReplaceRequest(space).Tuple(tuple)).Get()

	t := []struct {
		Key   string
		Value []byte
	}{}
	err := conn.Do(tarantool.NewSelectRequest(space).
		Index(index).
		Iterator(tarantool.IterEq).
		Key(tarantool.StringKey{"any"}),
	).GetTyped(&t)
	fmt.Println("Error", err)
	fmt.Println("Data", t)
	// Output:
	// Error <nil>
	// Data [{any [1 2]}]
}

func ExampleIntIntKey() {
	conn := exampleConnect(opts)
	defer conn.Close()

	const space = "testintint"
	const index = "primary"
	tuple := []interface{}{1, 2, "foo"}
	conn.Do(tarantool.NewReplaceRequest(space).Tuple(tuple)).Get()

	t := []struct {
		Key1  int
		Key2  int
		Value string
	}{}
	err := conn.Do(tarantool.NewSelectRequest(space).
		Index(index).
		Iterator(tarantool.IterEq).
		Key(tarantool.IntIntKey{1, 2}),
	).GetTyped(&t)
	fmt.Println("Error", err)
	fmt.Println("Data", t)
	// Output:
	// Error <nil>
	// Data [{1 2 foo}]
}

func ExamplePingRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Ping a Tarantool instance to check connection.
	resp, err := conn.Do(tarantool.NewPingRequest()).Get()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}

// To pass contexts to request objects, use the Context() method.
// Pay attention that when using context with request objects,
// the timeout option for Connection will not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func ExamplePingRequest_Context() {
	conn := exampleConnect(opts)
	defer conn.Close()

	timeout := time.Nanosecond

	// This way you may set the a common timeout for requests with a context.
	rootCtx, cancelRoot := context.WithTimeout(context.Background(), timeout)
	defer cancelRoot()

	// This context will be canceled with the root after commonTimeout.
	ctx, cancel := context.WithCancel(rootCtx)
	defer cancel()

	req := tarantool.NewPingRequest().Context(ctx)

	// Ping a Tarantool instance to check connection.
	resp, err := conn.Do(req).Get()
	fmt.Println("Ping Resp", resp)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Resp <nil>
	// Ping Error context is done
}

func ExampleSelectRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	for i := 1111; i <= 1112; i++ {
		conn.Do(tarantool.NewReplaceRequest(spaceNo).
			Tuple([]interface{}{uint(i), "hello", "world"}),
		).Get()
	}

	key := []interface{}{uint(1111)}
	resp, err := conn.Do(tarantool.NewSelectRequest(617).
		Limit(100).
		Iterator(tarantool.IterEq).
		Key(key),
	).Get()

	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	var res []Tuple
	err = conn.Do(tarantool.NewSelectRequest("test").
		Index("primary").
		Limit(100).
		Iterator(tarantool.IterEq).
		Key(key),
	).GetTyped(&res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)

	// Output:
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is [{{} 1111 hello world}]
}

func ExampleInsertRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Insert a new tuple { 31, 1 }.
	resp, err := conn.Do(tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{uint(31), "test", "one"}),
	).Get()
	fmt.Println("Insert 31")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Insert a new tuple { 32, 1 }.
	resp, err = conn.Do(tarantool.NewInsertRequest("test").
		Tuple(&Tuple{Id: 32, Msg: "test", Name: "one"}),
	).Get()
	fmt.Println("Insert 32")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key { 31 }.
	conn.Do(tarantool.NewDeleteRequest("test").
		Index("primary").
		Key([]interface{}{uint(31)}),
	).Get()
	// Delete tuple with primary key { 32 }.
	conn.Do(tarantool.NewDeleteRequest("test").
		Index(indexNo).
		Key([]interface{}{uint(31)}),
	).Get()
	// Output:
	// Insert 31
	// Error <nil>
	// Code 0
	// Data [[31 test one]]
	// Insert 32
	// Error <nil>
	// Code 0
	// Data [[32 test one]]
}

func ExampleDeleteRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Insert a new tuple { 35, 1 }.
	conn.Do(tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{uint(35), "test", "one"}),
	).Get()
	// Insert a new tuple { 36, 1 }.
	conn.Do(tarantool.NewInsertRequest("test").
		Tuple(&Tuple{Id: 36, Msg: "test", Name: "one"}),
	).Get()

	// Delete tuple with primary key { 35 }.
	resp, err := conn.Do(tarantool.NewDeleteRequest(spaceNo).
		Index(indexNo).
		Key([]interface{}{uint(35)}),
	).Get()
	fmt.Println("Delete 35")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key { 36 }.
	resp, err = conn.Do(tarantool.NewDeleteRequest("test").
		Index("primary").
		Key([]interface{}{uint(36)}),
	).Get()
	fmt.Println("Delete 36")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Delete 35
	// Error <nil>
	// Code 0
	// Data [[35 test one]]
	// Delete 36
	// Error <nil>
	// Code 0
	// Data [[36 test one]]
}

func ExampleReplaceRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Insert a new tuple { 13, 1 }.
	conn.Do(tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{uint(13), "test", "one"}),
	).Get()

	// Replace a tuple with primary key 13.
	// Note, Tuple is defined within tests, and has EncdodeMsgpack and
	// DecodeMsgpack methods.
	resp, err := conn.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]interface{}{uint(13), 1}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple([]interface{}{uint(13), 1}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple(&Tuple{Id: 13, Msg: "test", Name: "eleven"}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple(&Tuple{Id: 13, Msg: "test", Name: "twelve"}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Replace 13
	// Error <nil>
	// Code 0
	// Data [[13 1]]
	// Replace 13
	// Error <nil>
	// Code 0
	// Data [[13 1]]
	// Replace 13
	// Error <nil>
	// Code 0
	// Data [[13 test eleven]]
	// Replace 13
	// Error <nil>
	// Code 0
	// Data [[13 test twelve]]
}

func ExampleUpdateRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	for i := 1111; i <= 1112; i++ {
		conn.Do(tarantool.NewReplaceRequest(spaceNo).
			Tuple([]interface{}{uint(i), "hello", "world"}),
		).Get()
	}

	req := tarantool.NewUpdateRequest(617).
		Key(tarantool.IntKey{1111}).
		Operations(tarantool.NewOperations().Assign(1, "bye"))
	resp, err := conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do update request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	req = tarantool.NewUpdateRequest("test").
		Index("primary").
		Key(tarantool.IntKey{1111}).
		Operations(tarantool.NewOperations().Assign(1, "hello"))
	fut := conn.Do(req)
	resp, err = fut.Get()
	if err != nil {
		fmt.Printf("error in do async update request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	// Output:
	// response is []interface {}{[]interface {}{0x457, "bye", "world"}}
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
}

func ExampleUpsertRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	var req tarantool.Request
	req = tarantool.NewUpsertRequest(617).
		Tuple([]interface{}{uint(1113), "first", "first"}).
		Operations(tarantool.NewOperations().Assign(1, "updated"))
	resp, err := conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do select upsert is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	req = tarantool.NewUpsertRequest("test").
		Tuple([]interface{}{uint(1113), "second", "second"}).
		Operations(tarantool.NewOperations().Assign(2, "updated"))
	fut := conn.Do(req)
	resp, err = fut.Get()
	if err != nil {
		fmt.Printf("error in do async upsert request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	req = tarantool.NewSelectRequest(617).
		Limit(100).
		Key(tarantool.IntKey{1113})
	resp, err = conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do select request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	// Output:
	// response is []interface {}{}
	// response is []interface {}{}
	// response is []interface {}{[]interface {}{0x459, "first", "updated"}}
}

func ExampleCallRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Call a function 'simple_concat' with arguments.
	resp, err := conn.Do(tarantool.NewCallRequest("simple_concat").
		Args([]interface{}{"1"}),
	).Get()
	fmt.Println("Call simple_concat()")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Call simple_concat()
	// Error <nil>
	// Code 0
	// Data [11]
}

func ExampleEvalRequest() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Run raw Lua code.
	resp, err := conn.Do(tarantool.NewEvalRequest("return 1 + 2")).Get()
	fmt.Println("Eval 'return 1 + 2'")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Eval 'return 1 + 2'
	// Error <nil>
	// Code 0
	// Data [3]
}

// To use SQL to query a tarantool instance, use ExecuteRequest.
//
// Pay attention that with different types of queries (DDL, DQL, DML etc.)
// some fields of the response structure (MetaData and InfoAutoincrementIds
// in SQLInfo) may be nil.
func ExampleExecuteRequest() {
	// Tarantool supports SQL since version 2.0.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if isLess {
		return
	}

	conn := exampleConnect(opts)
	defer conn.Close()

	req := tarantool.NewExecuteRequest(
		"CREATE TABLE SQL_TEST (id INTEGER PRIMARY KEY, name STRING)")
	resp, err := conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// There are 4 options to pass named parameters to an SQL query:
	// 1) The simple map;
	sqlBind1 := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	// 2) Any type of structure;
	sqlBind2 := struct {
		Id   int
		Name string
	}{1, "test"}

	// 3) It is possible to use []tarantool.KeyValueBind;
	sqlBind3 := []interface{}{
		tarantool.KeyValueBind{Key: "id", Value: 1},
		tarantool.KeyValueBind{Key: "name", Value: "test"},
	}

	// 4) []interface{} slice with tarantool.KeyValueBind items inside;
	sqlBind4 := []tarantool.KeyValueBind{
		{"id", 1},
		{"name", "test"},
	}

	// 1)
	req = tarantool.NewExecuteRequest(
		"CREATE TABLE SQL_TEST (id INTEGER PRIMARY KEY, name STRING)")
	req = req.Args(sqlBind1)
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// 2)
	req = req.Args(sqlBind2)
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// 3)
	req = req.Args(sqlBind3)
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// 4)
	req = req.Args(sqlBind4)
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// The way to pass positional arguments to an SQL query.
	req = tarantool.NewExecuteRequest(
		"SELECT id FROM SQL_TEST WHERE id=? AND name=?").
		Args([]interface{}{2, "test"})
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// The way to pass SQL expression with using custom packing/unpacking for
	// a type.
	var res []Tuple
	req = tarantool.NewExecuteRequest(
		"SELECT id, name, name FROM SQL_TEST WHERE id=?").
		Args([]interface{}{2})
	err = conn.Do(req).GetTyped(&res)
	fmt.Println("ExecuteTyped")
	fmt.Println("Error", err)
	fmt.Println("Data", res)

	// For using different types of parameters (positioned/named), collect all
	// items in []interface{}.
	// All "named" items must be passed with tarantool.KeyValueBind{}.
	req = tarantool.NewExecuteRequest(
		"SELECT id FROM SQL_TEST WHERE id=? AND name=?").
		Args([]interface{}{tarantool.KeyValueBind{"id", 1}, "test"})
	resp, err = conn.Do(req).Get()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)
}

func ExampleProtocolVersion() {
	conn := exampleConnect(opts)
	defer conn.Close()

	clientProtocolInfo := conn.ClientProtocolInfo()
	fmt.Println("Connector client protocol version:", clientProtocolInfo.Version)
	fmt.Println("Connector client protocol features:", clientProtocolInfo.Features)
	// Output:
	// Connector client protocol version: 4
	// Connector client protocol features: [StreamsFeature TransactionsFeature ErrorExtensionFeature WatchersFeature PaginationFeature]
}

func getTestTxnOpts() tarantool.Opts {
	txnOpts := opts.Clone()

	// Assert that server supports expected protocol features
	txnOpts.RequiredProtocolInfo = tarantool.ProtocolInfo{
		Version: tarantool.ProtocolVersion(1),
		Features: []tarantool.ProtocolFeature{
			tarantool.StreamsFeature,
			tarantool.TransactionsFeature,
		},
	}

	return txnOpts
}

func ExampleCommitRequest() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	conn := exampleConnect(txnOpts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "commit_hello", "commit_world"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(1001)})
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream before commit: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Commit transaction
	req = tarantool.NewCommitRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Commit: %s", err.Error())
		return
	}
	fmt.Printf("Commit transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after commit: response is %#v\n", resp.Data)
}

func ExampleRollbackRequest() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	conn := exampleConnect(txnOpts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(2001), "rollback_hello", "rollback_world"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(2001)})
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", resp.Data)
}

func ExampleBeginRequest_TxnIsolation() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	conn := exampleConnect(txnOpts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest().
		TxnIsolation(tarantool.ReadConfirmedLevel).
		Timeout(500 * time.Millisecond)
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(2001), "rollback_hello", "rollback_world"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(2001)})
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", resp.Data)
}

func ExampleFuture_GetIterator() {
	conn := exampleConnect(opts)
	defer conn.Close()

	const timeout = 3 * time.Second
	fut := conn.Do(tarantool.NewCallRequest("push_func").
		Args([]interface{}{4}),
	)

	var it tarantool.ResponseIterator
	for it = fut.GetIterator().WithTimeout(timeout); it.Next(); {
		resp := it.Value()
		if resp.Code == tarantool.PushCode {
			// It is a push message.
			fmt.Printf("push message: %v\n", resp.Data[0])
		} else if resp.Code == tarantool.OkCode {
			// It is a regular response.
			fmt.Printf("response: %v", resp.Data[0])
		} else {
			fmt.Printf("an unexpected response code %d", resp.Code)
		}
	}
	if err := it.Err(); err != nil {
		fmt.Printf("error in call of push_func is %v", err)
		return
	}
	// Output:
	// push message: 1
	// push message: 2
	// push message: 3
	// push message: 4
	// response: 4
}

func ExampleConnect() {
	conn, err := tarantool.Connect("127.0.0.1:3013", tarantool.Opts{
		Timeout:     5 * time.Second,
		User:        "test",
		Pass:        "test",
		Concurrency: 32,
	})
	if err != nil {
		fmt.Println("No connection available")
		return
	}
	defer conn.Close()
	if conn != nil {
		fmt.Println("Connection is ready")
	}
	// Output:
	// Connection is ready
}

// Example demonstrates how to retrieve information with space schema.
func ExampleSchema() {
	conn := exampleConnect(opts)
	defer conn.Close()

	schema := conn.Schema
	if schema.SpacesById == nil {
		fmt.Println("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		fmt.Println("schema.Spaces is nil")
	}

	space1 := schema.Spaces["test"]
	space2 := schema.SpacesById[616]
	fmt.Printf("Space 1 ID %d %s\n", space1.Id, space1.Name)
	fmt.Printf("Space 2 ID %d %s\n", space2.Id, space2.Name)
	// Output:
	// Space 1 ID 617 test
	// Space 2 ID 616 schematest
}

// Example demonstrates how to retrieve information with space schema.
func ExampleSpace() {
	conn := exampleConnect(opts)
	defer conn.Close()

	// Save Schema to a local variable to avoid races
	schema := conn.Schema
	if schema.SpacesById == nil {
		fmt.Println("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		fmt.Println("schema.Spaces is nil")
	}

	// Access Space objects by name or ID.
	space1 := schema.Spaces["test"]
	space2 := schema.SpacesById[616] // It's a map.
	fmt.Printf("Space 1 ID %d %s %s\n", space1.Id, space1.Name, space1.Engine)
	fmt.Printf("Space 1 ID %d %t\n", space1.FieldsCount, space1.Temporary)

	// Access index information by name or ID.
	index1 := space1.Indexes["primary"]
	index2 := space2.IndexesById[3] // It's a map.
	fmt.Printf("Index %d %s\n", index1.Id, index1.Name)

	// Access index fields information by index.
	indexField1 := index1.Fields[0] // It's a slice.
	indexField2 := index2.Fields[1] // It's a slice.
	fmt.Println(indexField1, indexField2)

	// Access space fields information by name or id (index).
	spaceField1 := space2.Fields["name0"]
	spaceField2 := space2.FieldsById[3]
	fmt.Printf("SpaceField 1 %s %s\n", spaceField1.Name, spaceField1.Type)
	fmt.Printf("SpaceField 2 %s %s\n", spaceField2.Name, spaceField2.Type)

	// Output:
	// Space 1 ID 617 test memtx
	// Space 1 ID 0 false
	// Index 0 primary
	// &{0 unsigned} &{2 string}
	// SpaceField 1 name0 unsigned
	// SpaceField 2 name3 unsigned
}

// To use prepared statements to query a tarantool instance, call NewPrepared.
func ExampleConnection_NewPrepared() {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil || isLess {
		return
	}

	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout: 5 * time.Second,
		User:    "test",
		Pass:    "test",
	}
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		fmt.Printf("Failed to connect: %s", err.Error())
	}

	stmt, err := conn.NewPrepared("SELECT 1")
	if err != nil {
		fmt.Printf("Failed to connect: %s", err.Error())
	}

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	_, err = conn.Do(executeReq).Get()
	if err != nil {
		fmt.Printf("Failed to execute prepared stmt")
	}

	_, err = conn.Do(unprepareReq).Get()
	if err != nil {
		fmt.Printf("Failed to prepare")
	}
}

func ExampleConnection_NewWatcher() {
	const key = "foo"
	const value = "bar"

	// Tarantool watchers since version 2.10
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       5 * time.Second,
		Reconnect:     5 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
		// You need to require the feature to create a watcher.
		RequiredProtocolInfo: tarantool.ProtocolInfo{
			Features: []tarantool.ProtocolFeature{tarantool.WatchersFeature},
		},
	}
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		fmt.Printf("Failed to connect: %s\n", err)
		return
	}
	defer conn.Close()

	callback := func(event tarantool.WatchEvent) {
		fmt.Printf("event connection: %s\n", event.Conn.Addr())
		fmt.Printf("event key: %s\n", event.Key)
		fmt.Printf("event value: %v\n", event.Value)
	}
	watcher, err := conn.NewWatcher(key, callback)
	if err != nil {
		fmt.Printf("Failed to connect: %s\n", err)
		return
	}
	defer watcher.Unregister()

	conn.Do(tarantool.NewBroadcastRequest(key).Value(value)).Get()
	time.Sleep(time.Second)
}

// ExampleConnection_CloseGraceful_force demonstrates how to force close
// a connection with graceful close in progress after a while.
func ExampleConnection_CloseGraceful_force() {
	conn := exampleConnect(opts)

	eval := `local fiber = require('fiber')
	local time = ...
	fiber.sleep(time)
`
	req := tarantool.NewEvalRequest(eval).Args([]interface{}{10})
	fut := conn.Do(req)

	done := make(chan struct{})
	go func() {
		conn.CloseGraceful()
		fmt.Println("Connection.CloseGraceful() done!")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		fmt.Println("Force Connection.Close()!")
		conn.Close()
	}
	<-done

	fmt.Println("Result:")
	fmt.Println(fut.Get())
	// Output:
	// Force Connection.Close()!
	// Connection.CloseGraceful() done!
	// Result:
	// <nil> connection closed by client (0x4001)
}
