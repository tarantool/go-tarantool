<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>
<!--a href="https://travis-ci.org/tarantool/go-tarantool">
	<img src="https://travis-ci.org/tarantool/go-tarantool.png?branch=master" align="right">
</a-->

# Client in Go for Tarantool 1.6+

The `go-tarantool` package has everything necessary for interfacing with
[Tarantool 1.6+](http://tarantool.org/).

The advantage of integrating Go with Tarantool, which is an application server
plus a DBMS, is that Go programmers can handle databases and perform on-the-fly
recompilations of embedded Lua routines, just as in C, with responses that are
faster than other packages according to public benchmarks.

## Table of contents

* [Installation](#installation)
* [Hello World](#hello-world)
* [API reference](#api-reference)
* [Walking\-through example in Go](#walking-through-example-in-go)
* [Help](#help)
* [Usage](#usage)
* [Schema](#schema)
* [Custom (un)packing and typed selects and function calls](#custom-unpacking-and-typed-selects-and-function-calls)
* [Options](#options)
* [Working with queue](#working-with-queue)
* [Alternative connectors](#alternative-connectors)

## Installation

We assume that you have Tarantool version 1.6 and a modern Linux or BSD
operating system.

You will need a current version of `go`, version 1.3 or later (use
`go version` to check the version number). Do not use `gccgo-go`.

**Note:** If your `go` version is younger than 1.3, or if `go` is not installed,
download the latest tarball from [golang.org](https://golang.org/dl/) and say:

```bash
$ sudo tar -C /usr/local -xzf go1.7.5.linux-amd64.tar.gz
$ export PATH=$PATH:/usr/local/go/bin
$ export GOPATH="/usr/local/go/go-tarantool"
$ sudo chmod -R a+rwx /usr/local/go </pre>
```

The `go-tarantool` package is in
[tarantool/go-tarantool](github.com/tarantool/go-tarantool) repository.
To download and install, say:

```
$ go get github.com/tarantool/go-tarantool
```

This should bring source and binary files into subdirectories of `/usr/local/go`,
making it possible to access by adding `github.com/tarantool/go-tarantool` in
the `import {...}` section at the start of any Go program.

<h2>Hello World</h2>

In the "[Connectors](http://tarantool.org/doc/book/connectors/index.html#go)"
chapter of the Tarantool manual, there is an explanation of a very short (18-line)
program written in Go. Follow the instructions at the start of the "Connectors"
chapter carefully. Then cut and paste the example into a file named `example.go`,
and run it. You should see: nothing.

If that is what you see, then you have successfully installed `go-tarantool` and
successfully executed a program that manipulated the contents of a Tarantool
database.

<h2>API reference</h2>

Read the [Tarantool manual](http://tarantool.org/doc.html) to find descriptions
of terms like "connect", "space", "index", and the requests for creating and
manipulating database objects or Lua functions.

The source files for the requests library are:
* [connection.go](https://github.com/tarantool/go-tarantool/blob/master/connection.go)
  for the `Connect()` function plus functions related to connecting, and
* [request.go](https://github.com/tarantool/go-tarantool/blob/master/request.go)
  for data-manipulation functions and Lua invocations.

See comments in those files for syntax details:
```
Ping
closeConnection
Select
Insert
Replace
Delete
Update
Upsert
Call
Call17
Eval
```

The supported requests have parameters and results equivalent to requests in the
Tarantool manual. There are also Typed and Async versions of each data-manipulation
function.

The source file for error-handling tools is
[errors.go](https://github.com/tarantool/go-tarantool/blob/master/errors.go),
which has structure definitions and constants whose names are equivalent to names
of errors that the Tarantool server returns.

## Walking-through example in Go

We can now have a closer look at the `example.go` program and make some observations
about what it does.

```go
package main

import (
     "fmt"
     "github.com/tarantool/go-tarantool"
)

func main() {
   opts := tarantool.Opts{User: "guest"}
   conn, err := tarantool.Connect("127.0.0.1:3301", opts)
   // conn, err := tarantool.Connect("/path/to/tarantool.socket", opts)
   if err != nil {
       fmt.Println("Connection refused:", err)
   }
   resp, err := conn.Insert(999, []interface{}{99999, "BB"})
   if err != nil {
     fmt.Println("Error", err)
     fmt.Println("Code", resp.Code)
   }
}
```

**Observation 1:** the line "`github.com/tarantool/go-tarantool`" in the
`import(...)` section brings in all Tarantool-related functions and structures.

**Observation 2:** the line beginning with "`Opts :=`" sets up the options for
`Connect()`. In this example, there is only one thing in the structure, a user
name. The structure can also contain:

* `Pass` (password),
* `Timeout` (maximum number of milliseconds to wait before giving up),
* `Reconnect` (number of seconds to wait before retrying if a connection fails),
* `MaxReconnect` (maximum number of times to retry).

**Observation 3:** the line containing "`tarantool.Connect`" is essential for
beginning any session. There are two parameters:

* a string with `host:port` format, and
* the option structure that was set up earlier.

**Observation 4:** the `err` structure will be `nil` if there is no error,
otherwise it will have a description which can be retrieved with `err.Error()`.

**Observation 5:** the `Insert` request, like almost all requests, is preceded by
"`conn.`" which is the name of the object that was returned by `Connect()`.
There are two parameters:

* a space number (it could just as easily have been a space name), and
* a tuple.

## Help

To contact `go-tarantool` developers on any problems, create an issue at
[tarantool/go-tarantool](http://github.com/tarantool/go-tarantool/issues).

The developers of the [Tarantool server](http://github.com/tarantool/tarantool)
will also be happy to provide advice or receive feedback.

## Usage

```go
package main

import (
	"github.com/tarantool/go-tarantool"
	"log"
	"time"
)

func main() {
	spaceNo := uint32(512)
	indexNo := uint32(0)

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
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	resp, err := client.Ping()
	log.Println(resp.Code)
	log.Println(resp.Data)
	log.Println(err)

	// insert new tuple { 10, 1 }
	resp, err = client.Insert(spaceNo, []interface{}{uint(10), 1})
    // or
	resp, err = client.Insert("test", []interface{}{uint(10), 1})
	log.Println("Insert")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// delete tuple with primary key { 10 }
	resp, err = client.Delete(spaceNo, indexNo, []interface{}{uint(10)})
    // or
	resp, err = client.Delete("test", "primary", []interface{}{uint(10)})
	log.Println("Delete")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// replace tuple with { 13, 1 }
	resp, err = client.Replace(spaceNo, []interface{}{uint(13), 1})
    // or
	resp, err = client.Replace("test", []interface{}{uint(13), 1})
	log.Println("Replace")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// update tuple with primary key { 13 }, incrementing second field by 3
	resp, err = client.Update(spaceNo, indexNo, []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})
    // or
	resp, err = client.Update("test", "primary", []interface{}{uint(13)}, []interface{}{[]interface{}{"+", 1, 3}})
	log.Println("Update")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// insert tuple {15, 1} or increment second field by 1
	resp, err = client.Upsert(spaceNo, []interface{}{uint(15), 1}, []interface{}{[]interface{}{"+", 1, 1}})
    // or
	resp, err = client.Upsert("test", []interface{}{uint(15), 1}, []interface{}{[]interface{}{"+", 1, 1}})
	log.Println("Upsert")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// select just one tuple with primay key { 15 }
	resp, err = client.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{uint(15)})
    // or
	resp, err = client.Select("test", "primary", 0, 1, tarantool.IterEq, []interface{}{uint(15)})
	log.Println("Select")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// select tuples by condition ( primay key > 15 ) with offset 7 limit 5
	// BTREE index supposed
	resp, err = client.Select(spaceNo, indexNo, 7, 5, tarantool.IterGt, []interface{}{uint(15)})
    // or
	resp, err = client.Select("test", "primary", 7, 5, tarantool.IterGt, []interface{}{uint(15)})
	log.Println("Select")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// call function 'func_name' with arguments
	resp, err = client.Call("func_name", []interface{}{1, 2, 3})
	log.Println("Call")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)

	// run raw lua code
	resp, err = client.Eval("return 1 + 2", []interface{}{})
	log.Println("Eval")
	log.Println("Error", err)
	log.Println("Code", resp.Code)
	log.Println("Data", resp.Data)
}
```

## Schema

```go
    // save Schema to local variable to avoid races
    schema := client.Schema

    // access Space objects by name or id
    space1 := schema.Spaces["some_space"]
    space2 := schema.SpacesById[20] // it's a map
    fmt.Printf("Space %d %s %s\n", space1.Id, space1.Name, space1.Engine)
    fmt.Printf("Space %d %d\n", space1.FieldsCount, space1.Temporary)

    // access index information by name or id
    index1 := space1.Indexes["some_index"]
    index2 := space1.IndexesById[2] // it's a map
    fmt.Printf("Index %d %s\n", index1.Id, index1.Name)

    // access index fields information by index
    indexField1 := index1.Fields[0] // it's a slice
    indexField2 := index1.Fields[1] // it's a slice
    fmt.Printf("IndexFields %s %s\n", indexField1.Name, indexField1.Type)

    // access space fields information by name or id (index)
    spaceField1 := space.Fields["some_field"]
    spaceField2 := space.FieldsById[3]
    fmt.Printf("SpaceField %s %s\n", spaceField1.Name, spaceField1.Type)
```

## Custom (un)packing and typed selects and function calls

You can specify custom pack/unpack functions for your types. This will allow you
to store complex structures inside a tuple and may speed up you requests.

Alternatively, you can just instruct the `msgpack` library to encode your
structure as an array. This is safe "magic". It will be easier to implement than
a custom packer/unpacker, but it will work slower.

```go
import (
	"github.com/tarantool/go-tarantool"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

type Tuple struct {
	Cid     uint
	Orig    string
	Members []Member
}

/* same effect in a "magic" way, but slower */
type Tuple2 struct {
	_msgpack struct{} `msgpack:",asArray"`

	Cid     uint
	Orig    string
	Members []Member
}

func (m *Member) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(2); err != nil {
		return err
	}
	if err := e.EncodeString(m.Name); err != nil {
		return err
	}
	if err := e.EncodeUint(m.Val); err != nil {
		return err
	}
	return nil
}

func (m *Member) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 2 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if m.Name, err = d.DecodeString(); err != nil {
		return err
	}
	if m.Val, err = d.DecodeUint(); err != nil {
		return err
	}
	return nil
}

func (c *Tuple) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint(c.Cid); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	if err := e.EncodeSliceLen(len(c.Members)); err != nil {
		return err
	}
	for _, m := range c.Members {
		e.Encode(m)
	}
	return nil
}

func (c *Tuple) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if c.Cid, err = d.DecodeUint(); err != nil {
		return err
	}
	if c.Orig, err = d.DecodeString(); err != nil {
		return err
	}
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	c.Members = make([]Member, l)
	for i := 0; i < l; i++ {
		d.Decode(&c.Members[i])
	}
	return nil
}

func main() {
	// establish connection ...

	tuple := Tuple{777, "orig", []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Replace(spaceNo, tuple)  // NOTE: insert structure itself
	if err != nil {
		t.Errorf("Failed to insert: %s", err.Error())
		return
	}

	var tuples []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}, &tuples)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
		return
	}

	// same result in a "magic" way
	var tuples2 []Tuple2
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}, &tuples2)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
		return
	}

	// call function 'func_name' returning a table of custom tuples
	var tuples3 []Tuple
	err = client.CallTyped("func_name", []interface{}{1, 2, 3}, &tuples3)
	if err != nil {
		t.Errorf("Failed to CallTyped: %s", err.Error())
		return
	}
}

/*
// Old way to register types
func init() {
	msgpack.Register(reflect.TypeOf(Tuple{}), encodeTuple, decodeTuple)
	msgpack.Register(reflect.TypeOf(Member{}), encodeMember, decodeMember)
}

func encodeMember(e *msgpack.Encoder, v reflect.Value) error {
	m := v.Interface().(Member)
	// same code as in EncodeMsgpack
	return nil
}

func decodeMember(d *msgpack.Decoder, v reflect.Value) error {
	m := v.Addr().Interface().(*Member)
	// same code as in DecodeMsgpack
	return nil
}

func encodeTuple(e *msgpack.Encoder, v reflect.Value) error {
	c := v.Interface().(Tuple)
	// same code as in EncodeMsgpack
	return nil
}

func decodeTuple(d *msgpack.Decoder, v reflect.Value) error {
	c := v.Addr().Interface().(*Tuple)
	// same code as in DecodeMsgpack
	return nil
}
*/

```

## Options

* `Timeout` - timeout for any particular request. If `Timeout` is zero request,
  any request may block infinitely.
* `Reconnect` - timeout between reconnect attempts. If `Reconnect` is zero, no
  reconnects will be performed.
* `MaxReconnects` - maximal number of reconnect failures; after that we give it
  up. If `MaxReconnects` is zero, the client will try to reconnect endlessly.
* `User` - user name to log into Tarantool.
* `Pass` - user password to log into Tarantool.

## Working with queue
```go
package main
import (
	"gopkg.in/vmihailenco/msgpack.v2"
	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/queue"
	"time"
	"fmt"
	"log"
)

type customData struct{
	Dummy bool
}

func (c *customData) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	if c.Dummy, err = d.DecodeBool(); err != nil {
		return err
	}
	return nil
}

func (c *customData) EncodeMsgpack(e *msgpack.Encoder) error {
	return e.EncodeBool(c.Dummy)
}

func main() {
	opts := tarantool.Opts{
		Timeout: time.Second,
		Reconnect: time.Second,
		MaxReconnects: 5,
		User: "user",
		Pass: "pass",
		// ...
	}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)

	if err != nil {
		log.Fatalf("connection: %s", err)
		return
	}

	cfg := queue.Cfg{
		Temporary:  true,
		IfNotExists: true,
		Kind:       queue.FIFO,
		Opts: queue.Opts{
			Ttl:   10 * time.Second,
			Ttr:   5 * time.Second,
			Delay: 3 * time.Second,
			Pri:   1,
		},
	}

	que := queue.New(conn, "test_queue")
	if err = que.Create(cfg); err != nil {
		log.Fatalf("queue create: %s", err)
		return
	}

	// put data
	task, err := que.Put("test_data")
	if err != nil {
		log.Fatalf("put task: %s", err)
	}
	fmt.Println("Task id is", task.Id())

	// take data
	task, err = que.Take() //blocking operation
	if err != nil {
		log.Fatalf("take task: %s", err)
	}
	fmt.Println("Data is", task.Data())
	task.Ack()

	// take typed example
	putData := customData{}
	// put data
	task, err = que.Put(&putData)
	if err != nil {
		log.Fatalf("put typed task: %s", err)
	}
	fmt.Println("Task id is ", task.Id())

	takeData := customData{}
	//take data
	task, err = que.TakeTyped(&takeData) //blocking operation
	if err != nil {
		log.Fatalf("take take typed: %s", err)
	}
	fmt.Println("Data is ", takeData)
	// same data
	fmt.Println("Data is ", task.Data())

	task, err = que.Put([]int{1, 2, 3})
	task.Bury()

	task, err = que.TakeTimeout(2 * time.Second)
	if task == nil {
		fmt.Println("Task is nil")
	}

	que.Drop()
}
```
Features of the implementation:

- If you use connection timeout and call `TakeWithTimeout` with parameter greater than the connection timeout then parameter reduced to it
- If you use connection timeout and call `Take` then we return a error if we can not take task from queue in a time equal to the connection timeout

## Multi connections

You can use multiple connections config with tatantool/multi. 

Main features:

- Check active connection with configurable time interval and on connection fail switch to next in pool.
- Get addresses list from server and reconfigure to use in MultiConnection.

Additional options (configurable via `ConnectWithOpts`):

* `CheckTimeout` - time interval to check for connection timeout and try to switch connection
* `ClusterDiscoveryTime` - time interval to ask server for updated address list (works on with `NodesGetFunctionName` set)
* `NodesGetFunctionName` - server lua function name to call for getting address list

## Alternative connectors

- https://github.com/viciious/go-tarantool
  Has tools to emulate tarantool, and to being replica for tarantool.
