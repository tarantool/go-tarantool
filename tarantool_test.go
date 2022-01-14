package tarantool_test

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

type Tuple2 struct {
	Cid     uint
	Orig    string
	Members []Member
}

func (m *Member) EncodeMsgpack(e *msgpack.Encoder) error {
	e.EncodeSliceLen(2)
	e.EncodeString(m.Name)
	e.EncodeUint(m.Val)
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

func (c *Tuple2) EncodeMsgpack(e *msgpack.Encoder) error {
	e.EncodeSliceLen(3)
	e.EncodeUint(c.Cid)
	e.EncodeString(c.Orig)
	e.Encode(c.Members)
	return nil
}

func (c *Tuple2) DecodeMsgpack(d *msgpack.Decoder) error {
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

var server = "127.0.0.1:3013"
var spaceNo = uint32(512)
var spaceName = "test"
var indexNo = uint32(0)
var indexName = "primary"
var opts = Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
	//Concurrency: 32,
	//RateLimit: 4*1024,
}

const N = 500

func BenchmarkClientSerial(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i++ {
		_, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientSerialTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	var r []Tuple
	for i := 0; i < b.N; i++ {
		err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, IntKey{1111}, &r)
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Error(err)
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
		}
		for j := 0; j < N; j++ {
			_, err = fs[j].Get()
			if err != nil {
				b.Error(err)
			}
		}

	}
}

func BenchmarkClientFutureTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	for i := 0; i < b.N; i += N {
		var fs [N]*Future
		for j := 0; j < N; j++ {
			fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterEq, IntKey{1111})
		}
		var r []Tuple
		for j := 0; j < N; j++ {
			err = fs[j].GetTyped(&r)
			if err != nil {
				b.Error(err)
			}
			if len(r) != 1 || r[0].Id != 1111 {
				b.Errorf("Doesn't match %v", r)
			}
		}
	}
}

func BenchmarkClientFutureParallel(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
			}
			exit = j < N
			for j > 0 {
				j--
				_, err := fs[j].Get()
				if err != nil {
					b.Error(err)
					break
				}
			}
		}
	})
}

func BenchmarkClientFutureParallelTyped(b *testing.B) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		exit := false
		for !exit {
			var fs [N]*Future
			var j int
			for j = 0; j < N && pb.Next(); j++ {
				fs[j] = conn.SelectAsync(spaceNo, indexNo, 0, 1, IterEq, IntKey{1111})
			}
			exit = j < N
			var r []Tuple
			for j > 0 {
				j--
				err := fs[j].GetTyped(&r)
				if err != nil {
					b.Error(err)
					break
				}
				if len(r) != 1 || r[0].Id != 1111 {
					b.Errorf("Doesn't match %v", r)
					break
				}
			}
		}
	})
}

func BenchmarkClientParallel(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
			if err != nil {
				b.Errorf("No connection available")
				break
			}
		}
	})
}

func BenchmarkClientParallelMassive(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	var wg sync.WaitGroup
	limit := make(chan struct{}, 128*1024)
	for i := 0; i < 512; i++ {
		go func() {
			var r []Tuple
			for {
				if _, ok := <-limit; !ok {
					break
				}
				err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, IntKey{1111}, &r)
				wg.Done()
				if err != nil {
					b.Errorf("No connection available")
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

func BenchmarkClientParallelMassiveUntyped(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	var wg sync.WaitGroup
	limit := make(chan struct{}, 128*1024)
	for i := 0; i < 512; i++ {
		go func() {
			for {
				if _, ok := <-limit; !ok {
					break
				}
				_, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
				wg.Done()
				if err != nil {
					b.Errorf("No connection available")
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

///////////////////

func TestClient(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	// Ping
	resp, err = conn.Ping()
	if err != nil {
		t.Errorf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Ping")
	}

	// Insert
	resp, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Insert")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Insert (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 1 {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}
	//resp, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	resp, err = conn.Insert(spaceNo, &Tuple{Id: 1, Msg: "hello", Name: "world"})
	if tntErr, ok := err.(Error); !ok || tntErr.Code != ErrTupleFound {
		t.Errorf("Expected ErrTupleFound but got: %v", err)
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Body len != 0")
	}

	// Delete
	resp, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(1)})
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 1 {
			t.Errorf("Unexpected body of Delete (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Delete (1)")
		}
	}
	resp, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(101)})
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Replace
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hello", "world"})
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hi", "planet"})
	if err != nil {
		t.Errorf("Failed to Replace (duplicate): %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace (duplicate)")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Replace")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Replace (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 2 {
			t.Errorf("Unexpected body of Replace (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hi" {
			t.Errorf("Unexpected body of Replace (1)")
		}
	}

	// Update
	resp, err = conn.Update(spaceNo, indexNo, []interface{}{uint(2)}, []interface{}{[]interface{}{"=", 1, "bye"}, []interface{}{"#", 2, 1}})
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Update (tuple len)")
		}
		if id, ok := tpl[0].(uint64); !ok || id != 2 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
	}

	// Upsert
	if strings.Compare(conn.Greeting.Version, "Tarantool 1.6.7") >= 0 {
		resp, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Errorf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Errorf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select
	for i := 10; i < 20; i++ {
		resp, err = conn.Replace(spaceNo, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Errorf("Failed to Replace: %s", err.Error())
		}
	}
	resp, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, ok := tpl[0].(uint64); !ok || id != 10 {
			t.Errorf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 10" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	// Select empty
	resp, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else {
		if tpl[0].Id != 10 {
			t.Errorf("Bad value loaded from SelectTyped")
		}
	}

	// Get Typed
	var singleTpl = Tuple{}
	err = conn.GetTyped(spaceNo, indexNo, []interface{}{uint(10)}, &singleTpl)
	if err != nil {
		t.Errorf("Failed to GetTyped: %s", err.Error())
	}
	if singleTpl.Id != 10 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed for one tuple
	var tpl1 [1]Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl1)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else {
		if tpl[0].Id != 10 {
			t.Errorf("Bad value loaded from SelectTyped")
		}
	}

	// Get Typed Empty
	var singleTpl2 Tuple
	err = conn.GetTyped(spaceNo, indexNo, []interface{}{uint(30)}, &singleTpl2)
	if err != nil {
		t.Errorf("Failed to GetTyped: %s", err.Error())
	}
	if singleTpl2.Id != 0 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed Empty
	var tpl2 []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}, &tpl2)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl2) != 0 {
		t.Errorf("Result len of SelectTyped != 1")
	}

	// Call
	resp, err = conn.Call("box.info", []interface{}{"box.schema.SPACE_ID"})
	if err != nil {
		t.Errorf("Failed to Call: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Call")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}

	// Call vs Call17
	resp, err = conn.Call("simple_incr", []interface{}{1})
	if resp.Data[0].([]interface{})[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	resp, err = conn.Call17("simple_incr", []interface{}{1})
	if resp.Data[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// Eval
	resp, err = conn.Eval("return 5 + 6", []interface{}{})
	if err != nil {
		t.Errorf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(uint64)
	if val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}
}

func TestSchema(t *testing.T) {
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	// Schema
	schema := conn.Schema
	if schema.SpacesById == nil {
		t.Errorf("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		t.Errorf("schema.Spaces is nil")
	}
	var space, space2 *Space
	var ok bool
	if space, ok = schema.SpacesById[514]; !ok {
		t.Errorf("space with id = 514 was not found in schema.SpacesById")
	}
	if space2, ok = schema.Spaces["schematest"]; !ok {
		t.Errorf("space with name 'schematest' was not found in schema.SpacesById")
	}
	if space != space2 {
		t.Errorf("space with id = 514 and space with name schematest are different")
	}
	if space.Id != 514 {
		t.Errorf("space 514 has incorrect Id")
	}
	if space.Name != "schematest" {
		t.Errorf("space 514 has incorrect Name")
	}
	if !space.Temporary {
		t.Errorf("space 514 should be temporary")
	}
	if space.Engine != "memtx" {
		t.Errorf("space 514 engine should be memtx")
	}
	if space.FieldsCount != 7 {
		t.Errorf("space 514 has incorrect fields count")
	}

	if space.FieldsById == nil {
		t.Errorf("space.FieldsById is nill")
	}
	if space.Fields == nil {
		t.Errorf("space.Fields is nill")
	}
	if len(space.FieldsById) != 6 {
		t.Errorf("space.FieldsById len is incorrect")
	}
	if len(space.Fields) != 6 {
		t.Errorf("space.Fields len is incorrect")
	}

	var field1, field2, field5, field1n, field5n *Field
	if field1, ok = space.FieldsById[1]; !ok {
		t.Errorf("field id = 1 was not found")
	}
	if field2, ok = space.FieldsById[2]; !ok {
		t.Errorf("field id = 2 was not found")
	}
	if field5, ok = space.FieldsById[5]; !ok {
		t.Errorf("field id = 5 was not found")
	}

	if field1n, ok = space.Fields["name1"]; !ok {
		t.Errorf("field name = name1 was not found")
	}
	if field5n, ok = space.Fields["name5"]; !ok {
		t.Errorf("field name = name5 was not found")
	}
	if field1 != field1n || field5 != field5n {
		t.Errorf("field with id = 1 and field with name 'name1' are different")
	}
	if field1.Name != "name1" {
		t.Errorf("field 1 has incorrect Name")
	}
	if field1.Type != "unsigned" {
		t.Errorf("field 1 has incorrect Type")
	}
	if field2.Name != "name2" {
		t.Errorf("field 2 has incorrect Name")
	}
	if field2.Type != "string" {
		t.Errorf("field 2 has incorrect Type")
	}

	if space.IndexesById == nil {
		t.Errorf("space.IndexesById is nill")
	}
	if space.Indexes == nil {
		t.Errorf("space.Indexes is nill")
	}
	if len(space.IndexesById) != 2 {
		t.Errorf("space.IndexesById len is incorrect")
	}
	if len(space.Indexes) != 2 {
		t.Errorf("space.Indexes len is incorrect")
	}

	var index0, index3, index0n, index3n *Index
	if index0, ok = space.IndexesById[0]; !ok {
		t.Errorf("index id = 0 was not found")
	}
	if index3, ok = space.IndexesById[3]; !ok {
		t.Errorf("index id = 3 was not found")
	}
	if index0n, ok = space.Indexes["primary"]; !ok {
		t.Errorf("index name = primary was not found")
	}
	if index3n, ok = space.Indexes["secondary"]; !ok {
		t.Errorf("index name = secondary was not found")
	}
	if index0 != index0n || index3 != index3n {
		t.Errorf("index with id = 3 and index with name 'secondary' are different")
	}
	if index3.Id != 3 {
		t.Errorf("index has incorrect Id")
	}
	if index0.Name != "primary" {
		t.Errorf("index has incorrect Name")
	}
	if index0.Type != "hash" || index3.Type != "tree" {
		t.Errorf("index has incorrect Type")
	}
	if !index0.Unique || index3.Unique {
		t.Errorf("index has incorrect Unique")
	}
	if index3.Fields == nil {
		t.Errorf("index.Fields is nil")
	}
	if len(index3.Fields) != 2 {
		t.Errorf("index.Fields len is incorrect")
	}

	ifield1 := index3.Fields[0]
	ifield2 := index3.Fields[1]
	if ifield1 == nil || ifield2 == nil {
		t.Errorf("index field is nil")
	}
	if ifield1.Id != 1 || ifield2.Id != 2 {
		t.Errorf("index field has incorrect Id")
	}
	if (ifield1.Type != "num" && ifield1.Type != "unsigned") || (ifield2.Type != "STR" && ifield2.Type != "string") {
		t.Errorf("index field has incorrect Type '%s'", ifield2.Type)
	}

	var rSpaceNo, rIndexNo uint32
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex(514, 3)
	if err != nil || rSpaceNo != 514 || rIndexNo != 3 {
		t.Errorf("numeric space and index params not resolved as-is")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex(514, nil)
	if err != nil || rSpaceNo != 514 {
		t.Errorf("numeric space param not resolved as-is")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest", "secondary")
	if err != nil || rSpaceNo != 514 || rIndexNo != 3 {
		t.Errorf("symbolic space and index params not resolved")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest", nil)
	if err != nil || rSpaceNo != 514 {
		t.Errorf("symbolic space param not resolved")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest22", "secondary")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing space name")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest", "secondary22")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing index name")
	}
}

func TestClientNamed(t *testing.T) {
	var resp *Response
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	// Insert
	resp, err = conn.Insert(spaceName, []interface{}{uint(1001), "hello2", "world2"})
	if err != nil {
		t.Errorf("Failed to Insert: %s", err.Error())
	}

	// Delete
	resp, err = conn.Delete(spaceName, indexName, []interface{}{uint(1001)})
	if err != nil {
		t.Errorf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}

	// Replace
	resp, err = conn.Replace(spaceName, []interface{}{uint(1002), "hello", "world"})
	if err != nil {
		t.Errorf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}

	// Update
	resp, err = conn.Update(spaceName, indexName, []interface{}{uint(1002)}, []interface{}{[]interface{}{"=", 1, "bye"}, []interface{}{"#", 2, 1}})
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
	}

	// Upsert
	if strings.Compare(conn.Greeting.Version, "Tarantool 1.6.7") >= 0 {
		resp, err = conn.Upsert(spaceName, []interface{}{uint(1003), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Errorf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Upsert(spaceName, []interface{}{uint(1003), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Errorf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select
	for i := 1010; i < 1020; i++ {
		resp, err = conn.Replace(spaceName, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Errorf("Failed to Replace: %s", err.Error())
		}
	}
	resp, err = conn.Select(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)})
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)}, &tpl)
	if err != nil {
		t.Errorf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
}

func TestComplexStructs(t *testing.T) {
	var err error
	var conn *Connection

	conn, err = Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Replace(spaceNo, &tuple)
	if err != nil {
		t.Errorf("Failed to insert: %s", err.Error())
		return
	}

	var tuples [1]Tuple2
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}, &tuples)
	if err != nil {
		t.Errorf("Failed to selectTyped: %s", err.Error())
		return
	}

	if len(tuples) != 1 {
		t.Errorf("Failed to selectTyped: unexpected array length %d", len(tuples))
		return
	}

	if tuple.Cid != tuples[0].Cid || len(tuple.Members) != len(tuples[0].Members) || tuple.Members[1].Name != tuples[0].Members[1].Name {
		t.Errorf("Failed to selectTyped: incorrect data")
		return
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "config.lua",
		Listen:       server,
		WorkDir:      "work_dir",
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
