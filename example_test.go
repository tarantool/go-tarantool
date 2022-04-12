package tarantool_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"`
	Id       uint
	Msg      string
	Name     string
}

func example_connect() *tarantool.Connection {
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		panic("Connection is not established")
	}
	return conn
}

func ExampleConnection_Select() {
	conn := example_connect()
	defer conn.Close()

	conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	conn.Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"})

	resp, err := conn.Select(512, 0, 0, 100, tarantool.IterEq, []interface{}{uint(1111)})
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
	err := conn.SelectTyped(512, 0, 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)
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

func ExampleConnection_Call17() {
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
