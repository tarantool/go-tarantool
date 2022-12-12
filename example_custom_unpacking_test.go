package tarantool_test

import (
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool"
)

type Tuple2 struct {
	Cid     uint
	Orig    string
	Members []Member
}

// Same effect in a "magic" way, but slower.
type Tuple3 struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused

	Cid     uint
	Orig    string
	Members []Member
}

func (c *Tuple2) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := encodeUint(e, uint64(c.Cid)); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	e.Encode(c.Members)
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
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
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	c.Members = make([]Member, l)
	for i := 0; i < l; i++ {
		d.Decode(&c.Members[i])
	}
	return nil
}

// Example demonstrates how to use custom (un)packing with typed selects and
// function calls.
//
// You can specify user-defined packing/unpacking functions for your types.
// This allows you to store complex structures within a tuple and may speed up
// your requests.
//
// Alternatively, you can just instruct the msgpack library to encode your
// structure as an array. This is safe "magic". It is easier to implement than
// a custom packer/unpacker, but it will work slower.
func Example_customUnpacking() {
	// Establish a connection.
	server := "127.0.0.1:3013"
	opts := tarantool.Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     1 * time.Second,
		MaxReconnects: 3,
		User:          "test",
		Pass:          "test",
	}
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	spaceNo := uint32(617)
	indexNo := uint32(0)

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	resp, err := conn.Replace(spaceNo, &tuple) // NOTE: insert a structure itself.
	if err != nil {
		log.Fatalf("Failed to insert: %s", err.Error())
		return
	}
	fmt.Println("Data", resp.Data)
	fmt.Println("Code", resp.Code)

	var tuples1 []Tuple2
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{777}, &tuples1)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples1)", tuples1)

	// Same result in a "magic" way.
	var tuples2 []Tuple3
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{777}, &tuples2)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples2):", tuples2)

	// Call a function "func_name" returning a table of custom tuples.
	var tuples3 [][]Tuple3
	err = conn.Call17Typed("func_name", []interface{}{}, &tuples3)
	if err != nil {
		log.Fatalf("Failed to CallTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples3):", tuples3)

	// Output:
	// Data [[777 orig [[lol 1] [wut 3]]]]
	// Code 0
	// Tuples (tuples1) [{777 orig [{lol  1} {wut  3}]}]
	// Tuples (tuples2): [{{} 777 orig [{lol  1} {wut  3}]}]
	// Tuples (tuples3): [[{{} 221  [{Moscow  34} {Minsk  23} {Kiev  31}]}]]

}
