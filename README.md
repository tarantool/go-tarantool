# Tarantool

[Tarantool 1.6+](http://tarantool.org/) client in Go.

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
It's possible to specify custom pack/unpack functions for your types.
It will allow you to store complex structures inside a tuple and may speed up you requests.
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

func init() {
	msgpack.Register(reflect.TypeOf(Tuple{}), encodeTuple, decodeTuple)
	msgpack.Register(reflect.TypeOf(Member{}), encodeMember, decodeMember)
}

func encodeMember(e *msgpack.Encoder, v reflect.Value) error {
	m := v.Interface().(Member)
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

func decodeMember(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	m := v.Addr().Interface().(*Member)
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

func encodeTuple(e *msgpack.Encoder, v reflect.Value) error {
	c := v.Interface().(Tuple)
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

func decodeTuple(d *msgpack.Decoder, v reflect.Value) error {
	var err error
	var l int
	c := v.Addr().Interface().(*Tuple)
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

	// call function 'func_name' returning a table of custom tuples
	var tuples2 []Tuple
	err = client.CallTyped("func_name", []interface{}{1, 2, 3}, &tuples)
	if err != nil {
		t.Errorf("Failed to CallTyped: %s", err.Error())
		return
	}
}

```


## Options
* Timeout - timeout for any particular request. If Timeout is zero request any request may block infinitely
* Reconnect - timeout for between reconnect attempts. If Reconnect is zero, no reconnects will be performed
* MaxReconnects - maximal number of reconnect failures after that we give it up. If MaxReconnects is zero, client will try to reconnect endlessly
* User - user name to login tarantool
* Pass - user password to login tarantool
