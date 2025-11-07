package tarantool_test

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` // nolint: structcheck,unused
	Id       uint
	Msg      string
	Name     string
}

func exampleConnect(dialer tarantool.Dialer, opts tarantool.Opts) *tarantool.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
	return conn
}

func ExampleIntKey() {
	conn := exampleConnect(dialer, opts)
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
	conn := exampleConnect(dialer, opts)
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
	conn := exampleConnect(dialer, opts)
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
	conn := exampleConnect(dialer, opts)
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
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Ping a Tarantool instance to check connection.
	data, err := conn.Do(tarantool.NewPingRequest()).Get()
	fmt.Println("Ping Data", data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Data []
	// Ping Error <nil>
}

// To pass contexts to request objects, use the Context() method.
// Pay attention that when using context with request objects,
// the timeout option for Connection will not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func ExamplePingRequest_Context() {
	conn := exampleConnect(dialer, opts)
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
	data, err := conn.Do(req).Get()
	fmt.Println("Ping Resp data", data)
	fmt.Println("Ping Error", regexp.MustCompile("[0-9]+").ReplaceAllString(err.Error(), "N"))
	// Output:
	// Ping Resp data []
	// Ping Error context is done (request ID N): context deadline exceeded
}

func ExampleSelectRequest() {
	conn := exampleConnect(dialer, opts)
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
	).GetResponse()

	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	selResp, ok := resp.(*tarantool.SelectResponse)
	if !ok {
		fmt.Print("wrong response type")
		return
	}

	pos, err := selResp.Pos()
	if err != nil {
		fmt.Printf("error in Pos: %v", err)
		return
	}
	fmt.Printf("pos for Select is %v\n", pos)

	data, err := resp.Decode()
	if err != nil {
		fmt.Printf("error while decoding: %v", err)
		return
	}
	fmt.Printf("response is %#v\n", data)

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
	// pos for Select is []
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is [{{} 1111 hello world}]
}

func ExampleSelectRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewSelectRequest(spaceName)
	req.Index(indexName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleInsertRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Insert a new tuple { 31, 1 }.
	data, err := conn.Do(tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{uint(31), "test", "one"}),
	).Get()
	fmt.Println("Insert 31")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	// Insert a new tuple { 32, 1 }.
	data, err = conn.Do(tarantool.NewInsertRequest("test").
		Tuple(&Tuple{Id: 32, Msg: "test", Name: "one"}),
	).Get()
	fmt.Println("Insert 32")
	fmt.Println("Error", err)
	fmt.Println("Data", data)

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
	// Data [[31 test one]]
	// Insert 32
	// Error <nil>
	// Data [[32 test one]]
}

func ExampleInsertRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewInsertRequest(spaceName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleDeleteRequest() {
	conn := exampleConnect(dialer, opts)
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
	data, err := conn.Do(tarantool.NewDeleteRequest(spaceNo).
		Index(indexNo).
		Key([]interface{}{uint(35)}),
	).Get()
	fmt.Println("Delete 35")
	fmt.Println("Error", err)
	fmt.Println("Data", data)

	// Delete tuple with primary key { 36 }.
	data, err = conn.Do(tarantool.NewDeleteRequest("test").
		Index("primary").
		Key([]interface{}{uint(36)}),
	).Get()
	fmt.Println("Delete 36")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	// Output:
	// Delete 35
	// Error <nil>
	// Data [[35 test one]]
	// Delete 36
	// Error <nil>
	// Data [[36 test one]]
}

func ExampleDeleteRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewDeleteRequest(spaceName)
	req.Index(indexName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleReplaceRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Insert a new tuple { 13, 1 }.
	conn.Do(tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{uint(13), "test", "one"}),
	).Get()

	// Replace a tuple with primary key 13.
	// Note, Tuple is defined within tests, and has EncdodeMsgpack and
	// DecodeMsgpack methods.
	data, err := conn.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]interface{}{uint(13), 1}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	data, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple([]interface{}{uint(13), 1}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	data, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple(&Tuple{Id: 13, Msg: "test", Name: "eleven"}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	data, err = conn.Do(tarantool.NewReplaceRequest("test").
		Tuple(&Tuple{Id: 13, Msg: "test", Name: "twelve"}),
	).Get()
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	// Output:
	// Replace 13
	// Error <nil>
	// Data [[13 1]]
	// Replace 13
	// Error <nil>
	// Data [[13 1]]
	// Replace 13
	// Error <nil>
	// Data [[13 test eleven]]
	// Replace 13
	// Error <nil>
	// Data [[13 test twelve]]
}

func ExampleReplaceRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewReplaceRequest(spaceName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleUpdateRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	for i := 1111; i <= 1112; i++ {
		conn.Do(tarantool.NewReplaceRequest(spaceNo).
			Tuple([]interface{}{uint(i), "text", 1, 1, 1, 1, 1}),
		).Get()
	}

	req := tarantool.NewUpdateRequest(617).
		Key(tarantool.IntKey{1111}).
		Operations(tarantool.NewOperations().
			Add(2, 1).
			Subtract(3, 1).
			BitwiseAnd(4, 1).
			BitwiseOr(5, 1).
			BitwiseXor(6, 1).
			Splice(1, 1, 2, "!!").
			Insert(7, "new").
			Assign(7, "updated"))
	data, err := conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do update request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", data)
	// Output:
	// response is []interface {}{[]interface {}{0x457, "t!!t", 2, 0, 1, 1, 0, "updated"}}
}

func ExampleUpdateRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewUpdateRequest(spaceName)
	req.Index(indexName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleUpsertRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	var req tarantool.Request
	req = tarantool.NewUpsertRequest(617).
		Tuple([]interface{}{uint(1113), "first", "first"}).
		Operations(tarantool.NewOperations().Assign(1, "updated"))
	data, err := conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do select upsert is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", data)

	req = tarantool.NewUpsertRequest("test").
		Tuple([]interface{}{uint(1113), "second", "second"}).
		Operations(tarantool.NewOperations().Assign(2, "updated"))
	fut := conn.Do(req)
	data, err = fut.Get()
	if err != nil {
		fmt.Printf("error in do async upsert request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", data)

	req = tarantool.NewSelectRequest(617).
		Limit(100).
		Key(tarantool.IntKey{1113})
	data, err = conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do select request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", data)
	// Output:
	// response is []interface {}{}
	// response is []interface {}{}
	// response is []interface {}{[]interface {}{0x459, "first", "updated"}}
}

func ExampleUpsertRequest_spaceAndIndexNames() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewUpsertRequest(spaceName)
	data, err := conn.Do(req).Get()

	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

func ExampleCallRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Call a function 'simple_concat' with arguments.
	data, err := conn.Do(tarantool.NewCallRequest("simple_concat").
		Args([]interface{}{"1"}),
	).Get()
	fmt.Println("Call simple_concat()")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	// Output:
	// Call simple_concat()
	// Error <nil>
	// Data [11]
}

func ExampleEvalRequest() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Run raw Lua code.
	data, err := conn.Do(tarantool.NewEvalRequest("return 1 + 2")).Get()
	fmt.Println("Eval 'return 1 + 2'")
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	// Output:
	// Eval 'return 1 + 2'
	// Error <nil>
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

	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewExecuteRequest(
		"CREATE TABLE SQL_TEST (id INTEGER PRIMARY KEY, name STRING)")
	resp, err := conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)

	data, err := resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)

	exResp, ok := resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}

	metaData, err := exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)

	sqlInfo, err := exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

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
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

	// 2)
	req = req.Args(sqlBind2)
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

	// 3)
	req = req.Args(sqlBind3)
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

	// 4)
	req = req.Args(sqlBind4)
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

	// The way to pass positional arguments to an SQL query.
	req = tarantool.NewExecuteRequest(
		"SELECT id FROM SQL_TEST WHERE id=? AND name=?").
		Args([]interface{}{2, "test"})
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)

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
	resp, err = conn.Do(req).GetResponse()
	fmt.Println("Execute")
	fmt.Println("Error", err)
	data, err = resp.Decode()
	fmt.Println("Error", err)
	fmt.Println("Data", data)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	fmt.Println("MetaData", metaData)
	fmt.Println("Error", err)
	sqlInfo, err = exResp.SQLInfo()
	fmt.Println("SQL Info", sqlInfo)
	fmt.Println("Error", err)
}

func getTestTxnDialer() tarantool.Dialer {
	txnDialer := dialer

	// Assert that server supports expected protocol features.
	txnDialer.RequiredProtocolInfo = tarantool.ProtocolInfo{
		Version: tarantool.ProtocolVersion(1),
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_STREAMS,
			iproto.IPROTO_FEATURE_TRANSACTIONS,
		},
	}

	return txnDialer
}

func ExampleCommitRequest() {
	var req tarantool.Request
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnDialer := getTestTxnDialer()
	conn := exampleConnect(txnDialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	data, err := stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", data)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "commit_hello", "commit_world"})
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", data)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(1001)})
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream before commit: response is %#v\n", data)

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", data)

	// Commit transaction
	req = tarantool.NewCommitRequest()
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Commit: %s", err.Error())
		return
	}
	fmt.Printf("Commit transaction: response is %#v\n", data)

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after commit: response is %#v\n", data)
}

func ExampleRollbackRequest() {
	var req tarantool.Request
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnDialer := getTestTxnDialer()
	conn := exampleConnect(txnDialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	data, err := stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", data)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(2001), "rollback_hello", "rollback_world"})
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", data)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(2001)})
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", data)

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", data)

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", data)
}

func ExampleBeginRequest_TxnIsolation() {
	var req tarantool.Request
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnDialer := getTestTxnDialer()
	conn := exampleConnect(txnDialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest().
		TxnIsolation(tarantool.ReadConfirmedLevel).
		Timeout(500 * time.Millisecond)
	data, err := stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", data)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(2001), "rollback_hello", "rollback_world"})
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", data)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{uint(2001)})
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", data)

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	data, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", data)

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", data)
}

func ExampleBeginRequest_IsSync() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Tarantool supports IS_SYNC flag for BeginRequest since version 3.1.0.
	isLess, err := test_helpers.IsTarantoolVersionLess(3, 1, 0)
	if err != nil || isLess {
		return
	}

	stream, err := conn.NewStream()
	if err != nil {
		fmt.Printf("error getting the stream: %s\n", err)
		return
	}

	// Begin transaction with synchronous mode.
	req := tarantool.NewBeginRequest().IsSync(true)
	resp, err := stream.Do(req).GetResponse()
	switch {
	case err != nil:
		fmt.Printf("error getting the response: %s\n", err)
	case resp.Header().Error != tarantool.ErrorNo:
		fmt.Printf("response error code: %s\n", resp.Header().Error)
	default:
		fmt.Println("Success.")
	}
}

func ExampleCommitRequest_IsSync() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Tarantool supports IS_SYNC flag for CommitRequest since version 3.1.0.
	isLess, err := test_helpers.IsTarantoolVersionLess(3, 1, 0)
	if err != nil || isLess {
		return
	}

	var req tarantool.Request

	stream, err := conn.NewStream()
	if err != nil {
		fmt.Printf("error getting the stream: %s\n", err)
		return
	}

	// Begin transaction.
	req = tarantool.NewBeginRequest()
	resp, err := stream.Do(req).GetResponse()
	switch {
	case err != nil:
		fmt.Printf("error getting the response: %s\n", err)
		return
	case resp.Header().Error != tarantool.ErrorNo:
		fmt.Printf("response error code: %s\n", resp.Header().Error)
		return
	}

	// Insert in stream.
	req = tarantool.NewReplaceRequest("test").Tuple([]interface{}{1, "test"})
	resp, err = stream.Do(req).GetResponse()
	switch {
	case err != nil:
		fmt.Printf("error getting the response: %s\n", err)
		return
	case resp.Header().Error != tarantool.ErrorNo:
		fmt.Printf("response error code: %s\n", resp.Header().Error)
		return
	}

	// Commit transaction in sync mode.
	req = tarantool.NewCommitRequest().IsSync(true)
	resp, err = stream.Do(req).GetResponse()
	switch {
	case err != nil:
		fmt.Printf("error getting the response: %s\n", err)
	case resp.Header().Error != tarantool.ErrorNo:
		fmt.Printf("response error code: %s\n", resp.Header().Error)
	default:
		fmt.Println("Success.")
	}
}

func ExampleErrorNo() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	req := tarantool.NewPingRequest()
	resp, err := conn.Do(req).GetResponse()
	if err != nil {
		fmt.Printf("error getting the response: %s\n", err)
		return
	}

	if resp.Header().Error != tarantool.ErrorNo {
		fmt.Printf("response error code: %s\n", resp.Header().Error)
	} else {
		fmt.Println("Success.")
	}
	// Output:
	// Success.
}

func ExampleConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	dialer := tarantool.NetDialer{
		Address:  server,
		User:     "test",
		Password: "test",
	}

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{
		Timeout:     5 * time.Second,
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

func ExampleConnect_reconnects() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}

	opts := tarantool.Opts{
		Timeout:       5 * time.Second,
		Concurrency:   32,
		Reconnect:     time.Second,
		MaxReconnects: 10,
	}

	var conn *tarantool.Connection
	var err error

	for i := uint(0); i < opts.MaxReconnects; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		conn, err = tarantool.Connect(ctx, dialer, opts)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(opts.Reconnect)
	}
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
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	schema, err := tarantool.GetSchema(conn)
	if err != nil {
		fmt.Printf("unexpected error: %s\n", err.Error())
	}
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

// Example demonstrates how to update the connection schema.
func ExampleConnection_SetSchema() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Get the actual schema.
	schema, err := tarantool.GetSchema(conn)
	if err != nil {
		fmt.Printf("unexpected error: %s\n", err.Error())
	}
	// Update the current schema to match the actual one.
	conn.SetSchema(schema)
}

// Example demonstrates how to retrieve information with space schema.
func ExampleSpace() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// Save Schema to a local variable to avoid races
	schema, err := tarantool.GetSchema(conn)
	if err != nil {
		fmt.Printf("unexpected error: %s\n", err.Error())
	}
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
	// {0 unsigned} {2 string}
	// SpaceField 1 name0 unsigned
	// SpaceField 2 name3 unsigned
}

// ExampleConnection_Do demonstrates how to send a request and process
// a response.
func ExampleConnection_Do() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// It could be any request.
	req := tarantool.NewReplaceRequest("test").
		Tuple([]interface{}{int(1111), "foo", "bar"})

	// We got a future, the request actually not performed yet.
	future := conn.Do(req)

	// When the future receives the response, the result of the Future is set
	// and becomes available. We could wait for that moment with Future.Get()
	// or Future.GetTyped() methods.
	data, err := future.Get()
	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}

	// Output:
	// [[1111 foo bar]]
}

// ExampleConnection_Do_failure demonstrates how to send a request and process
// failure.
func ExampleConnection_Do_failure() {
	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	// It could be any request.
	req := tarantool.NewCallRequest("not_exist")

	// We got a future, the request actually not performed yet.
	future := conn.Do(req)

	// When the future receives the response, the result of the Future is set
	// and becomes available. We could wait for that moment with Future.Get(),
	// Future.GetResponse() or Future.GetTyped() methods.
	resp, err := future.GetResponse()
	if err != nil {
		fmt.Printf("Error in the future: %s\n", err)
	}
	// Optional step: check a response error.
	// It allows checking that response has or hasn't an error without decoding.
	if resp.Header().Error != tarantool.ErrorNo {
		fmt.Printf("Response error: %s\n", resp.Header().Error)
	}

	data, err := future.Get()
	if err != nil {
		fmt.Printf("Data: %v\n", data)
	}

	if err != nil {
		// We don't print the error here to keep the example reproducible.
		// fmt.Printf("Failed to execute the request: %s\n", err)
		if resp == nil {
			// Something happens in a client process (timeout, IO error etc).
			fmt.Printf("Resp == nil, ClientErr = %s\n", err.(tarantool.ClientError))
		} else {
			// Response exist. So it could be a Tarantool error or a decode
			// error. We need to check the error code.
			fmt.Printf("Error code from the response: %d\n", resp.Header().Error)
			if resp.Header().Error == tarantool.ErrorNo {
				fmt.Printf("Decode error: %s\n", err)
			} else {
				code := err.(tarantool.Error).Code
				fmt.Printf("Error code from the error: %d\n", code)
				fmt.Printf("Error short from the error: %s\n", code)
			}
		}
	}

	// Output:
	// Response error: ER_NO_SUCH_PROC
	// Data: []
	// Error code from the response: 33
	// Error code from the error: 33
	// Error short from the error: ER_NO_SUCH_PROC
}

// To use prepared statements to query a tarantool instance, call NewPrepared.
func ExampleConnection_NewPrepared() {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil || isLess {
		return
	}

	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	opts := tarantool.Opts{
		Timeout: 5 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, opts)
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

	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
		// You can require the feature explicitly.
		RequiredProtocolInfo: tarantool.ProtocolInfo{
			Features: []iproto.Feature{iproto.IPROTO_FEATURE_WATCHERS},
		},
	}

	opts := tarantool.Opts{
		Timeout:       5 * time.Second,
		Reconnect:     5 * time.Second,
		MaxReconnects: 3,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, opts)
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
	conn := exampleConnect(dialer, opts)

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
	// [] connection closed by client (0x4001)
}

func ExampleWatchOnceRequest() {
	const key = "foo"
	const value = "bar"

	// WatchOnce request present in Tarantool since version 3.0
	isLess, err := test_helpers.IsTarantoolVersionLess(3, 0, 0)
	if err != nil || isLess {
		return
	}

	conn := exampleConnect(dialer, opts)
	defer conn.Close()

	conn.Do(tarantool.NewBroadcastRequest(key).Value(value)).Get()

	data, err := conn.Do(tarantool.NewWatchOnceRequest(key)).Get()
	if err != nil {
		fmt.Printf("Failed to execute the request: %s\n", err)
	} else {
		fmt.Println(data)
	}
}

// This example demonstrates how to use an existing socket file descriptor
// to establish a connection with Tarantool. This can be useful if the socket fd
// was inherited from the Tarantool process itself.
// For details, please see TestFdDialer in tarantool_test.go.
func ExampleFdDialer() {
	addr := dialer.Address
	c, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("can't establish connection: %v\n", err)
		return
	}
	f, err := c.(*net.TCPConn).File()
	if err != nil {
		fmt.Printf("unexpected error: %v\n", err)
		return
	}
	dialer := tarantool.FdDialer{
		Fd: f.Fd(),
	}
	// Use an existing socket fd to create connection with Tarantool.
	conn, err := tarantool.Connect(context.Background(), dialer, opts)
	if err != nil {
		fmt.Printf("connect error: %v\n", err)
		return
	}
	_, err = conn.Do(tarantool.NewPingRequest()).Get()
	fmt.Println(err)
	// Output:
	// <nil>
}
