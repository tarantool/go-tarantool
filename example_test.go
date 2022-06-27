package tarantool_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Id       uint
	Msg      string
	Name     string
}

func example_connect() *tarantool.Connection {
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

func ExampleConnection_Select() {
	conn := example_connect()
	defer conn.Close()

	conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	conn.Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"})

	resp, err := conn.Select(517, 0, 0, 100, tarantool.IterEq, []interface{}{uint(1111)})

	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	resp, err = conn.Select("test", "primary", 0, 100, tarantool.IterEq, tarantool.IntKey{1111})
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	// Output:
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
}

func ExampleConnection_SelectTyped() {
	conn := example_connect()
	defer conn.Close()
	var res []Tuple

	err := conn.SelectTyped(517, 0, 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)

	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	err = conn.SelectTyped("test", "primary", 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	// Output:
	// response is [{{} 1111 hello world}]
	// response is [{{} 1111 hello world}]
}

func ExampleConnection_SelectAsync() {
	conn := example_connect()
	defer conn.Close()
	spaceNo := uint32(517)

	conn.Insert(spaceNo, []interface{}{uint(16), "test", "one"})
	conn.Insert(spaceNo, []interface{}{uint(17), "test", "one"})
	conn.Insert(spaceNo, []interface{}{uint(18), "test", "one"})

	var futs [3]*tarantool.Future
	futs[0] = conn.SelectAsync("test", "primary", 0, 2, tarantool.IterLe, tarantool.UintKey{16})
	futs[1] = conn.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{17})
	futs[2] = conn.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{18})
	var t []Tuple
	err := futs[0].GetTyped(&t)
	fmt.Println("Future", 0, "Error", err)
	fmt.Println("Future", 0, "Data", t)

	resp, err := futs[1].Get()
	fmt.Println("Future", 1, "Error", err)
	fmt.Println("Future", 1, "Data", resp.Data)

	resp, err = futs[2].Get()
	fmt.Println("Future", 2, "Error", err)
	fmt.Println("Future", 2, "Data", resp.Data)
	// Output:
	// Future 0 Error <nil>
	// Future 0 Data [{{} 16 val 16 bla} {{} 15 val 15 bla}]
	// Future 1 Error <nil>
	// Future 1 Data [[17 val 17 bla]]
	// Future 2 Error <nil>
	// Future 2 Data [[18 val 18 bla]]
}

func ExampleSelectRequest() {
	conn := example_connect()
	defer conn.Close()

	req := tarantool.NewSelectRequest(517).
		Limit(100).
		Key(tarantool.IntKey{1111})
	resp, err := conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in do select request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	req = tarantool.NewSelectRequest("test").
		Index("primary").
		Limit(100).
		Key(tarantool.IntKey{1111})
	fut := conn.Do(req)
	resp, err = fut.Get()
	if err != nil {
		fmt.Printf("error in do async select request is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	// Output:
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
	// response is []interface {}{[]interface {}{0x457, "hello", "world"}}
}

func ExampleUpdateRequest() {
	conn := example_connect()
	defer conn.Close()

	req := tarantool.NewUpdateRequest(517).
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
	conn := example_connect()
	defer conn.Close()

	var req tarantool.Request
	req = tarantool.NewUpsertRequest(517).
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

	req = tarantool.NewSelectRequest(517).
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

func ExampleFuture_GetIterator() {
	conn := example_connect()
	defer conn.Close()

	const timeout = 3 * time.Second
	// Or any other Connection.*Async() call.
	fut := conn.Call17Async("push_func", []interface{}{4})

	var it tarantool.ResponseIterator
	for it = fut.GetIterator().WithTimeout(timeout); it.Next(); {
		resp := it.Value()
		if resp.Code == tarantool.PushCode {
			// It is a push message.
			fmt.Printf("push message: %d\n", resp.Data[0].(uint64))
		} else if resp.Code == tarantool.OkCode {
			// It is a regular response.
			fmt.Printf("response: %d", resp.Data[0].(uint64))
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

func ExampleConnection_Ping() {
	conn := example_connect()
	defer conn.Close()

	// Ping a Tarantool instance to check connection.
	resp, err := conn.Ping()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}

func ExampleConnection_Insert() {
	conn := example_connect()
	defer conn.Close()

	// Insert a new tuple { 31, 1 }.
	resp, err := conn.Insert(spaceNo, []interface{}{uint(31), "test", "one"})
	fmt.Println("Insert 31")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Insert a new tuple { 32, 1 }.
	resp, err = conn.Insert("test", &Tuple{Id: 32, Msg: "test", Name: "one"})
	fmt.Println("Insert 32")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key { 31 }.
	conn.Delete("test", "primary", []interface{}{uint(31)})
	// Delete tuple with primary key { 32 }.
	conn.Delete(spaceNo, indexNo, []interface{}{uint(32)})
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

func ExampleConnection_Delete() {
	conn := example_connect()
	defer conn.Close()

	// Insert a new tuple { 35, 1 }.
	conn.Insert(spaceNo, []interface{}{uint(35), "test", "one"})
	// Insert a new tuple { 36, 1 }.
	conn.Insert("test", &Tuple{Id: 36, Msg: "test", Name: "one"})

	// Delete tuple with primary key { 35 }.
	resp, err := conn.Delete(spaceNo, indexNo, []interface{}{uint(35)})
	fmt.Println("Delete 35")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key { 36 }.
	resp, err = conn.Delete("test", "primary", []interface{}{uint(36)})
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

func ExampleConnection_Replace() {
	conn := example_connect()
	defer conn.Close()

	// Insert a new tuple { 13, 1 }.
	conn.Insert(spaceNo, []interface{}{uint(13), "test", "one"})

	// Replace a tuple with primary key 13.
	// Note, Tuple is defined within tests, and has EncdodeMsgpack and
	// DecodeMsgpack methods.
	resp, err := conn.Replace(spaceNo, []interface{}{uint(13), 1})
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Replace("test", []interface{}{uint(13), 1})
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Replace("test", &Tuple{Id: 13, Msg: "test", Name: "eleven"})
	fmt.Println("Replace 13")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = conn.Replace("test", &Tuple{Id: 13, Msg: "test", Name: "twelve"})
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

func ExampleConnection_Update() {
	conn := example_connect()
	defer conn.Close()

	// Insert a new tuple { 14, 1 }.
	conn.Insert(spaceNo, []interface{}{uint(14), "test", "one"})

	// Update tuple with primary key { 14 }.
	resp, err := conn.Update(spaceName, indexName, []interface{}{uint(14)}, []interface{}{[]interface{}{"=", 1, "bye"}})
	fmt.Println("Update 14")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Update 14
	// Error <nil>
	// Code 0
	// Data [[14 bye bla]]
}

func ExampleConnection_Call() {
	conn := example_connect()
	defer conn.Close()

	// Call a function 'simple_incr' with arguments.
	resp, err := conn.Call17("simple_incr", []interface{}{1})
	fmt.Println("Call simple_incr()")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Call simple_incr()
	// Error <nil>
	// Code 0
	// Data [2]
}

func ExampleConnection_Eval() {
	conn := example_connect()
	defer conn.Close()

	// Run raw Lua code.
	resp, err := conn.Eval("return 1 + 2", []interface{}{})
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

func ExampleConnect() {
	conn, err := tarantool.Connect("127.0.0.1:3013", tarantool.Opts{
		Timeout:     500 * time.Millisecond,
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
	conn := example_connect()
	defer conn.Close()

	schema := conn.Schema
	if schema.SpacesById == nil {
		fmt.Println("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		fmt.Println("schema.Spaces is nil")
	}

	space1 := schema.Spaces["test"]
	space2 := schema.SpacesById[516]
	fmt.Printf("Space 1 ID %d %s\n", space1.Id, space1.Name)
	fmt.Printf("Space 2 ID %d %s\n", space2.Id, space2.Name)
	// Output:
	// Space 1 ID 517 test
	// Space 2 ID 516 schematest
}

// Example demonstrates how to retrieve information with space schema.
func ExampleSpace() {
	conn := example_connect()
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
	space2 := schema.SpacesById[516] // It's a map.
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
	// Space 1 ID 517 test memtx
	// Space 1 ID 0 false
	// Index 0 primary
	// &{0 unsigned} &{2 string}
	// SpaceField 1 name0 unsigned
	// SpaceField 2 name3 unsigned
}

// To use SQL to query a tarantool instance, call Execute.
//
// Pay attention that with different types of queries (DDL, DQL, DML etc.)
// some fields of the response structure (MetaData and InfoAutoincrementIds in SQLInfo) may be nil.
func ExampleConnection_Execute() {
	// Tarantool supports SQL since version 2.0.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if isLess {
		return
	}
	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	client, err := tarantool.Connect(server, opts)
	if err != nil {
		fmt.Printf("Failed to connect: %s", err.Error())
	}

	resp, err := client.Execute("CREATE TABLE SQL_TEST (id INTEGER PRIMARY KEY, name STRING)", []interface{}{})
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// there are 4 options to pass named parameters to an SQL query
	// the simple map:
	sqlBind1 := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	// any type of structure
	sqlBind2 := struct {
		Id   int
		Name string
	}{1, "test"}

	// it is possible to use []tarantool.KeyValueBind
	sqlBind3 := []interface{}{
		tarantool.KeyValueBind{Key: "id", Value: 1},
		tarantool.KeyValueBind{Key: "name", Value: "test"},
	}

	// or []interface{} slice with tarantool.KeyValueBind items inside
	sqlBind4 := []tarantool.KeyValueBind{
		{"id", 1},
		{"name", "test"},
	}

	// the next usage
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=:id AND name=:name", sqlBind1)
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// the same as
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=:id AND name=:name", sqlBind2)
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// the same as
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=:id AND name=:name", sqlBind3)
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// the same as
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=:id AND name=:name", sqlBind4)
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// the way to pass positional arguments to an SQL query
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=? AND name=?", []interface{}{2, "test"})
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)

	// the way to pass SQL expression with using custom packing/unpacking for a type
	var res []Tuple
	sqlInfo, metaData, err := client.ExecuteTyped("SELECT id, name, name FROM SQL_TEST WHERE id=?", []interface{}{2}, &res)
	fmt.Println("ExecuteTyped")
	fmt.Println("Error", err)
	fmt.Println("Data", res)
	fmt.Println("MetaData", metaData)
	fmt.Println("SQL Info", sqlInfo)

	// for using different types of parameters (positioned/named), collect all items in []interface{}
	// all "named" items must be passed with tarantool.KeyValueBind{}
	resp, err = client.Execute("SELECT id FROM SQL_TEST WHERE id=:id AND name=?",
		[]interface{}{tarantool.KeyValueBind{"id", 1}, "test"})
	fmt.Println("Execute")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	fmt.Println("MetaData", resp.MetaData)
	fmt.Println("SQL Info", resp.SQLInfo)
}
