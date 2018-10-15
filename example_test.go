package tarantool_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
)

type Tuple struct {
	/* instruct msgpack to pack this struct as array,
	 * so no custom packer is needed */
	_msgpack struct{} `msgpack:",asArray"`
	Id       uint
	Msg      string
	Name     string
}

func example_connect() (*tarantool.Connection, error) {
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		return nil, err
	}
	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	_, err = conn.Replace(spaceNo, []interface{}{uint(1112), "hallo", "werld"})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func ExampleConnection_Select() {
	var conn *tarantool.Connection
	conn, err := example_connect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()
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
	var conn *tarantool.Connection
	conn, err := example_connect()
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()
	var res []Tuple
	err = conn.SelectTyped(512, 0, 0, 100, tarantool.IterEq, tarantool.IntKey{1111}, &res)
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

func Example() {
	spaceNo := uint32(512)
	indexNo := uint32(0)

	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       50 * time.Millisecond,
		Reconnect:     100 * time.Millisecond,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	client, err := tarantool.Connect(server, opts)
	if err != nil {
		fmt.Errorf("Failed to connect: %s", err.Error())
		return
	}

	resp, err := client.Ping()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)

	// delete tuple for cleaning
	client.Delete(spaceNo, indexNo, []interface{}{uint(10)})
	client.Delete(spaceNo, indexNo, []interface{}{uint(11)})

	// insert new tuple { 10, 1 }
	resp, err = client.Insert(spaceNo, []interface{}{uint(10), "test", "one"})
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Code", resp.Code)
	fmt.Println("Insert Data", resp.Data)

	// insert new tuple { 11, 1 }
	resp, err = client.Insert("test", &Tuple{Id: 10, Msg: "test", Name: "one"})
	fmt.Println("Insert Error", err)
	fmt.Println("Insert Code", resp.Code)
	fmt.Println("Insert Data", resp.Data)

	// delete tuple with primary key { 10 }
	resp, err = client.Delete(spaceNo, indexNo, []interface{}{uint(10)})
	// or
	// resp, err = client.Delete("test", "primary", UintKey{10}})
	fmt.Println("Delete Error", err)
	fmt.Println("Delete Code", resp.Code)
	fmt.Println("Delete Data", resp.Data)

	// replace tuple with primary key 13
	// note, Tuple is defined within tests, and has EncdodeMsgpack and DecodeMsgpack
	// methods
	resp, err = client.Replace(spaceNo, []interface{}{uint(13), 1})
	fmt.Println("Replace Error", err)
	fmt.Println("Replace Code", resp.Code)
	fmt.Println("Replace Data", resp.Data)

	// update tuple with primary key { 13 }, incrementing second field by 3
	resp, err = client.Update("test", "primary", tarantool.UintKey{13}, []tarantool.Op{{"+", 1, 3}})
	// or
	// resp, err = client.Update(spaceNo, indexNo, []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})
	fmt.Println("Update Error", err)
	fmt.Println("Update Code", resp.Code)
	fmt.Println("Update Data", resp.Data)

	// select just one tuple with primay key { 15 }
	resp, err = client.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{uint(15)})
	// or
	// resp, err = client.Select("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{15})
	fmt.Println("Select Error", err)
	fmt.Println("Select Code", resp.Code)
	fmt.Println("Select Data", resp.Data)

	// call function 'func_name' with arguments
	resp, err = client.Call17("simple_incr", []interface{}{1})
	fmt.Println("Call17 Error", err)
	fmt.Println("Call17 Code", resp.Code)
	fmt.Println("Call17 Data", resp.Data)

	// run raw lua code
	resp, err = client.Eval("return 1 + 2", []interface{}{})
	fmt.Println("Eval Error", err)
	fmt.Println("Eval Code", resp.Code)
	fmt.Println("Eval Data", resp.Data)

	resp, err = client.Replace("test", &Tuple{Id: 11, Msg: "test", Name: "eleven"})
	resp, err = client.Replace("test", &Tuple{Id: 12, Msg: "test", Name: "twelve"})

	var futs [3]*tarantool.Future
	futs[0] = client.SelectAsync("test", "primary", 0, 2, tarantool.IterLe, tarantool.UintKey{12})
	futs[1] = client.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{13})
	futs[2] = client.SelectAsync("test", "primary", 0, 1, tarantool.IterEq, tarantool.UintKey{15})
	var t []Tuple
	err = futs[0].GetTyped(&t)
	fmt.Println("Fut", 0, "Error", err)
	fmt.Println("Fut", 0, "Data", t)

	resp, err = futs[1].Get()
	fmt.Println("Fut", 1, "Error", err)
	fmt.Println("Fut", 1, "Data", resp.Data)

	resp, err = futs[2].Get()
	fmt.Println("Fut", 2, "Error", err)
	fmt.Println("Fut", 2, "Data", resp.Data)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
	// Insert Error <nil>
	// Insert Code 0
	// Insert Data [[10 test one]]
	// Insert Error Duplicate key exists in unique index 'primary' in space 'test' (0x3)
	// Insert Code 3
	// Insert Data []
	// Delete Error <nil>
	// Delete Code 0
	// Delete Data [[10 test one]]
	// Replace Error <nil>
	// Replace Code 0
	// Replace Data [[13 1]]
	// Update Error <nil>
	// Update Code 0
	// Update Data [[13 4]]
	// Select Error <nil>
	// Select Code 0
	// Select Data [[15 val 15 bla]]
	// Call17 Error <nil>
	// Call17 Code 0
	// Call17 Data [2]
	// Eval Error <nil>
	// Eval Code 0
	// Eval Data [3]
	// Fut 0 Error <nil>
	// Fut 0 Data [{{} 12 test twelve} {{} 11 test eleven}]
	// Fut 1 Error <nil>
	// Fut 1 Data [[13 4]]
	// Fut 2 Error <nil>
	// Fut 2 Data [[15 val 15 bla]]
}
