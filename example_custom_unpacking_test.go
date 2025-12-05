package tarantool_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

type Tuple2 struct {
	Cid     uint
	Orig    string
	Members []Member
}

// Same effect in a "magic" way, but slower.
type Tuple3 struct {
	_msgpack struct{} `msgpack:",asArray"` // nolint: structcheck,unused

	Cid     uint
	Orig    string
	Members []Member
}

func (c *Tuple2) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(3); err != nil {
		return err
	}
	if err := e.EncodeUint(uint64(c.Cid)); err != nil {
		return err
	}
	if err := e.EncodeString(c.Orig); err != nil {
		return err
	}
	if err := e.Encode(c.Members); err != nil {
		return err
	}
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *msgpack.Decoder) error {
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
		if err = d.Decode(&c.Members[i]); err != nil {
			return err
		}
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

	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	opts := tarantool.Opts{}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	conn, err := tarantool.Connect(ctx, dialer, opts)
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	spaceNo := uint32(617)
	indexNo := uint32(0)

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	// Insert a structure itself.
	initReq := tarantool.NewReplaceRequest(spaceNo).Tuple(&tuple)
	data, err := conn.Do(initReq).Get()
	if err != nil {
		log.Fatalf("Failed to insert: %s", err.Error())
		return
	}
	fmt.Println("Data", data)

	var tuples1 []Tuple2
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{777})
	err = conn.Do(selectReq).GetTyped(&tuples1)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples1)", tuples1)

	// Same result in a "magic" way.
	var tuples2 []Tuple3
	err = conn.Do(selectReq).GetTyped(&tuples2)
	if err != nil {
		log.Fatalf("Failed to SelectTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples2):", tuples2)

	// Call a function "func_name" returning a table of custom tuples.
	var tuples3 [][]Tuple3
	callReq := tarantool.NewCallRequest("func_name")
	err = conn.Do(callReq).GetTyped(&tuples3)
	if err != nil {
		log.Fatalf("Failed to CallTyped: %s", err.Error())
		return
	}
	fmt.Println("Tuples (tuples3):", tuples3)

	// Output:
	// Data [[777 orig [[lol 1] [wut 3]]]]
	// Tuples (tuples1) [{777 orig [{lol  1} {wut  3}]}]
	// Tuples (tuples2): [{{} 777 orig [{lol  1} {wut  3}]}]
	// Tuples (tuples3): [[{{} 221  [{Moscow  34} {Minsk  23} {Kiev  31}]}]]

}
