package tarantool_test

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	. "github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

func connect(t testing.TB, server string, opts Opts) (conn *Connection) {
	t.Helper()

	conn, err := Connect(server, opts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if conn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	return conn
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

var server = "127.0.0.1:3013"
var spaceNo = uint32(517)
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

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(1111)})
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientSerialTyped(b *testing.B) {
	var err error

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	var r []Tuple
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, IntKey{1111}, &r)
		if err != nil {
			b.Errorf("No connection available")
		}
	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
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

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
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

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
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

	conn := connect(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	b.ResetTimer()
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
	conn := connect(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	b.ResetTimer()
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
	conn := connect(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

func BenchmarkClientParallelMassiveUntyped(b *testing.B) {
	conn := connect(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
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

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		limit <- struct{}{}
	}
	wg.Wait()
	close(limit)
}

func BenchmarkClientReplaceParallel(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()
	spaceNo = 520

	rSpaceNo, _, err := conn.Schema.ResolveSpaceIndex("test_perf", "secondary")
	if err != nil {
		b.Fatalf("Space is not resolved: %s", err.Error())
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Replace(rSpaceNo, []interface{}{uint(1), "hello", []interface{}{}})
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkClientLargeSelectParallel(b *testing.B) {
	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	schema := conn.Schema
	rSpaceNo, rIndexNo, err := schema.ResolveSpaceIndex("test_perf", "secondary")
	if err != nil {
		b.Fatalf("symbolic space and index params not resolved")
	}

	offset, limit := uint32(0), uint32(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Select(rSpaceNo, rIndexNo, offset, limit, IterEq, []interface{}{"test_name"})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkSQLParallel(b *testing.B) {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		b.Fatal("Could not check the Tarantool version")
	}
	if isLess {
		b.Skip()
	}

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("No connection available")
		return
	}
	defer conn.Close()

	spaceNo := 519
	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Execute("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?", []interface{}{uint(1111)})
			if err != nil {
				b.Errorf("Select failed: %s", err.Error())
				break
			}
		}
	})
}

func BenchmarkSQLSerial(b *testing.B) {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		b.Fatal("Could not check the Tarantool version")
	}
	if isLess {
		b.Skip()
	}

	conn, err := Connect(server, opts)
	if err != nil {
		b.Errorf("Failed to connect: %s", err)
		return
	}
	defer conn.Close()

	spaceNo := 519
	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("Failed to replace: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Execute("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?", []interface{}{uint(1111)})
		if err != nil {
			b.Errorf("Select failed: %s", err.Error())
			break
		}
	}
}

///////////////////

func TestClient(t *testing.T) {
	var resp *Response
	var err error

	conn := connect(t, server, opts)
	defer conn.Close()

	// Ping
	resp, err = conn.Ping()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Ping")
	}

	// Insert
	resp, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Insert")
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
		t.Fatalf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Delete")
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
		t.Fatalf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Delete")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Replace
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}
	resp, err = conn.Replace(spaceNo, []interface{}{uint(2), "hi", "planet"})
	if err != nil {
		t.Fatalf("Failed to Replace (duplicate): %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Replace (duplicate)")
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
		t.Fatalf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Update")
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
			t.Fatalf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Fatalf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select
	for i := 10; i < 20; i++ {
		resp, err = conn.Replace(spaceNo, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Fatalf("Failed to Replace: %s", err.Error())
		}
		if resp.Code != 0 {
			t.Errorf("Failed to replace")
		}
	}
	resp, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)})
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
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
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err.Error())
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
		t.Fatalf("Failed to GetTyped: %s", err.Error())
	}
	if singleTpl.Id != 10 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed for one tuple
	var tpl1 [1]Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl1)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err.Error())
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
		t.Fatalf("Failed to GetTyped: %s", err.Error())
	}
	if singleTpl2.Id != 0 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed Empty
	var tpl2 []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}, &tpl2)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl2) != 0 {
		t.Errorf("Result len of SelectTyped != 1")
	}

	// Call16
	resp, err = conn.Call16("box.info", []interface{}{"box.schema.SPACE_ID"})
	if err != nil {
		t.Fatalf("Failed to Call16: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Call16")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}

	// Call16 vs Call17
	resp, err = conn.Call16("simple_incr", []interface{}{1})
	if err != nil {
		t.Errorf("Failed to use Call16")
	}
	if resp.Data[0].([]interface{})[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	resp, err = conn.Call17("simple_incr", []interface{}{1})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if resp.Data[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// Eval
	resp, err = conn.Eval("return 5 + 6", []interface{}{})
	if err != nil {
		t.Fatalf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	val := resp.Data[0].(uint64)
	if val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}
}

func TestClientSessionPush(t *testing.T) {
	conn := connect(t, server, opts)
	defer conn.Close()

	var it ResponseIterator
	const pushMax = 3
	// It will be iterated immediately.
	fut0 := conn.Call17Async("push_func", []interface{}{pushMax})
	respCnt := 0
	for it = fut0.GetIterator(); it.Next(); {
		err := it.Err()
		resp := it.Value()
		if err != nil {
			t.Errorf("Unexpected error after it.Next() == true: %q", err.Error())
			break
		}
		if resp == nil {
			t.Errorf("Response is empty after it.Next() == true")
			break
		}
		respCnt += 1
	}
	if err := it.Err(); err != nil {
		t.Errorf("An unexpected iteration error: %s", err.Error())
	}
	if respCnt > pushMax+1 {
		t.Errorf("Unexpected respCnt = %d, expected 0 <= respCnt <= %d", respCnt, pushMax+1)
	}
	_, _ = fut0.Get()

	// It will wait a response before iteration.
	fut1 := conn.Call17Async("push_func", []interface{}{pushMax})
	// Future.Get ignores push messages.
	resp, err := fut1.Get()
	if err != nil {
		t.Errorf("Failed to Call17: %s", err.Error())
	} else if resp == nil {
		t.Errorf("Response is nil after CallAsync")
	} else if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Call17Async")
	} else if resp.Data[0].(uint64) != pushMax {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// It will will be iterated with a timeout.
	fut2 := conn.Call17Async("push_func", []interface{}{pushMax})

	var its = []ResponseIterator{
		fut1.GetIterator(),
		fut2.GetIterator().WithTimeout(5 * time.Second),
	}

	for i := 0; i < len(its); i++ {
		pushCnt := uint64(0)
		respCnt := uint64(0)

		it = its[i]
		for it.Next() {
			resp = it.Value()
			if resp == nil {
				t.Errorf("Response is empty after it.Next() == true")
				break
			}
			if len(resp.Data) < 1 {
				t.Errorf("Response.Data is empty after CallAsync")
				break
			}
			if resp.Code == PushCode {
				pushCnt += 1
				if resp.Data[0].(uint64) != pushCnt {
					t.Errorf("Unexpected push data = %v", resp.Data)
				}
			} else {
				respCnt += 1
				if resp.Data[0].(uint64) != pushMax {
					t.Errorf("result is not {{1}} : %v", resp.Data)
				}
			}
		}

		if err = it.Err(); err != nil {
			t.Errorf("An unexpected iteration error: %s", err.Error())
		}

		if pushCnt != pushMax {
			t.Errorf("Expect %d pushes but got %d", pushMax, pushCnt)
		}

		if respCnt != 1 {
			t.Errorf("Expect %d responses but got %d", 1, respCnt)
		}
	}
}

const (
	createTableQuery         = "CREATE TABLE SQL_SPACE (id INTEGER PRIMARY KEY AUTOINCREMENT, name STRING COLLATE \"unicode\" DEFAULT NULL);"
	insertQuery              = "INSERT INTO SQL_SPACE VALUES (?, ?);"
	selectNamedQuery         = "SELECT id, name FROM SQL_SPACE WHERE id=:id AND name=:name;"
	selectPosQuery           = "SELECT id, name FROM SQL_SPACE WHERE id=? AND name=?;"
	updateQuery              = "UPDATE SQL_SPACE SET name=? WHERE id=?;"
	enableFullMetaDataQuery  = "SET SESSION \"sql_full_metadata\" = true;"
	selectSpanDifQuery       = "SELECT id*2, name, id FROM SQL_SPACE WHERE name=?;"
	alterTableQuery          = "ALTER TABLE SQL_SPACE RENAME TO SQL_SPACE2;"
	insertIncrQuery          = "INSERT INTO SQL_SPACE2 VALUES (?, ?);"
	deleteQuery              = "DELETE FROM SQL_SPACE2 WHERE name=?;"
	dropQuery                = "DROP TABLE SQL_SPACE2;"
	dropQuery2               = "DROP TABLE SQL_SPACE;"
	disableFullMetaDataQuery = "SET SESSION \"sql_full_metadata\" = false;"

	selectTypedQuery  = "SELECT NAME1, NAME0 FROM SQL_TEST WHERE NAME0=?"
	selectNamedQuery2 = "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;"
	selectPosQuery2   = "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=? AND NAME1=?;"
	mixedQuery        = "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:name0 AND NAME1=?;"
)

func TestSQL(t *testing.T) {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}
	if isLess {
		t.Skip()
	}

	type testCase struct {
		Query string
		Args  interface{}
		Resp  Response
	}

	testCases := []testCase{
		{
			createTableQuery,
			[]interface{}{},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			insertQuery,
			[]interface{}{1, "test"},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			selectNamedQuery,
			map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			Response{
				SQLInfo: SQLInfo{AffectedCount: 0},
				Data:    []interface{}{[]interface{}{uint64(1), "test"}},
				MetaData: []ColumnMetaData{
					{FieldType: "integer", FieldName: "ID"},
					{FieldType: "string", FieldName: "NAME"}},
			},
		},
		{
			selectPosQuery,
			[]interface{}{1, "test"},
			Response{
				SQLInfo: SQLInfo{AffectedCount: 0},
				Data:    []interface{}{[]interface{}{uint64(1), "test"}},
				MetaData: []ColumnMetaData{
					{FieldType: "integer", FieldName: "ID"},
					{FieldType: "string", FieldName: "NAME"}},
			},
		},
		{
			updateQuery,
			[]interface{}{"test_test", 1},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			enableFullMetaDataQuery,
			[]interface{}{},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			selectSpanDifQuery,
			[]interface{}{"test_test"},
			Response{
				SQLInfo: SQLInfo{AffectedCount: 0}, Data: []interface{}{[]interface{}{uint64(2), "test_test", uint64(1)}},
				MetaData: []ColumnMetaData{
					{
						FieldType:            "integer",
						FieldName:            "COLUMN_1",
						FieldIsNullable:      false,
						FieldIsAutoincrement: false,
						FieldSpan:            "id*2",
					},
					{
						FieldType:            "string",
						FieldName:            "NAME",
						FieldIsNullable:      true,
						FieldIsAutoincrement: false,
						FieldSpan:            "name",
						FieldCollation:       "unicode",
					},
					{
						FieldType:            "integer",
						FieldName:            "ID",
						FieldIsNullable:      false,
						FieldIsAutoincrement: true,
						FieldSpan:            "id",
					},
				}},
		},
		{
			alterTableQuery,
			[]interface{}{},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 0},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			insertIncrQuery,
			[]interface{}{2, "test_2"},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1, InfoAutoincrementIds: []uint64{1}},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			deleteQuery,
			[]interface{}{"test_2"},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			dropQuery,
			[]interface{}{},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			disableFullMetaDataQuery,
			[]interface{}{},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
	}

	conn := connect(t, server, opts)
	defer conn.Close()

	for i, test := range testCases {
		resp, err := conn.Execute(test.Query, test.Args)
		assert.NoError(t, err, "Failed to Execute, Query number: %d", i)
		assert.NotNil(t, resp, "Response is nil after Execute\nQuery number: %d", i)
		for j := range resp.Data {
			assert.Equal(t, resp.Data[j], test.Resp.Data[j], "Response data is wrong")
		}
		assert.Equal(t, resp.SQLInfo.AffectedCount, test.Resp.SQLInfo.AffectedCount, "Affected count is wrong")

		errorMsg := "Response Metadata is wrong"
		for j := range resp.MetaData {
			assert.Equal(t, resp.MetaData[j].FieldIsAutoincrement, test.Resp.MetaData[j].FieldIsAutoincrement, errorMsg)
			assert.Equal(t, resp.MetaData[j].FieldIsNullable, test.Resp.MetaData[j].FieldIsNullable, errorMsg)
			assert.Equal(t, resp.MetaData[j].FieldCollation, test.Resp.MetaData[j].FieldCollation, errorMsg)
			assert.Equal(t, resp.MetaData[j].FieldName, test.Resp.MetaData[j].FieldName, errorMsg)
			assert.Equal(t, resp.MetaData[j].FieldSpan, test.Resp.MetaData[j].FieldSpan, errorMsg)
			assert.Equal(t, resp.MetaData[j].FieldType, test.Resp.MetaData[j].FieldType, errorMsg)
		}
	}
}

func TestSQLTyped(t *testing.T) {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatal("Could not check the Tarantool version")
	}
	if isLess {
		t.Skip()
	}

	conn := connect(t, server, opts)
	defer conn.Close()

	mem := []Member{}
	info, meta, err := conn.ExecuteTyped(selectTypedQuery, []interface{}{1}, &mem)
	if info.AffectedCount != 0 {
		t.Errorf("Rows affected count must be 0")
	}
	if len(meta) != 2 {
		t.Errorf("Meta data is not full")
	}
	if len(mem) != 1 {
		t.Errorf("Wrong length of result")
	}
	if err != nil {
		t.Error(err)
	}
}

func TestSQLBindings(t *testing.T) {
	// Data for test table
	testData := map[int]string{
		1: "test",
	}

	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatal("Could not check the Tarantool version")
	}
	if isLess {
		t.Skip()
	}

	var resp *Response

	conn := connect(t, server, opts)
	defer conn.Close()

	// test all types of supported bindings
	// prepare named sql bind
	sqlBind := map[string]interface{}{
		"id":   1,
		"name": "test",
	}

	sqlBind2 := struct {
		Id   int
		Name string
	}{1, "test"}

	sqlBind3 := []KeyValueBind{
		{"id", 1},
		{"name", "test"},
	}

	sqlBind4 := []interface{}{
		KeyValueBind{Key: "id", Value: 1},
		KeyValueBind{Key: "name", Value: "test"},
	}

	namedSQLBinds := []interface{}{
		sqlBind,
		sqlBind2,
		sqlBind3,
		sqlBind4,
	}

	//positioned sql bind
	sqlBind5 := []interface{}{
		1, "test",
	}

	// mixed sql bind
	sqlBind6 := []interface{}{
		KeyValueBind{Key: "name0", Value: 1},
		"test",
	}

	for _, bind := range namedSQLBinds {
		resp, err = conn.Execute(selectNamedQuery2, bind)
		if err != nil {
			t.Fatalf("Failed to Execute: %s", err.Error())
		}
		if resp == nil {
			t.Fatal("Response is nil after Execute")
		}
		if reflect.DeepEqual(resp.Data[0], []interface{}{1, testData[1]}) {
			t.Error("Select with named arguments failed")
		}
		if resp.MetaData[0].FieldType != "unsigned" ||
			resp.MetaData[0].FieldName != "NAME0" ||
			resp.MetaData[1].FieldType != "string" ||
			resp.MetaData[1].FieldName != "NAME1" {
			t.Error("Wrong metadata")
		}
	}

	resp, err = conn.Execute(selectPosQuery2, sqlBind5)
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if reflect.DeepEqual(resp.Data[0], []interface{}{1, testData[1]}) {
		t.Error("Select with positioned arguments failed")
	}
	if resp.MetaData[0].FieldType != "unsigned" ||
		resp.MetaData[0].FieldName != "NAME0" ||
		resp.MetaData[1].FieldType != "string" ||
		resp.MetaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}

	resp, err = conn.Execute(mixedQuery, sqlBind6)
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if reflect.DeepEqual(resp.Data[0], []interface{}{1, testData[1]}) {
		t.Error("Select with positioned arguments failed")
	}
	if resp.MetaData[0].FieldType != "unsigned" ||
		resp.MetaData[0].FieldName != "NAME0" ||
		resp.MetaData[1].FieldType != "string" ||
		resp.MetaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}
}

func TestStressSQL(t *testing.T) {
	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}
	if isLess {
		t.Skip()
	}

	var resp *Response

	conn := connect(t, server, opts)
	defer conn.Close()

	resp, err = conn.Execute(createTableQuery, []interface{}{})
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code != 0 {
		t.Fatalf("Failed to Execute: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}

	// create table with the same name
	resp, err = conn.Execute(createTableQuery, []interface{}{})
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code != ErSpaceExistsCode {
		t.Fatalf("Unexpected response code: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}

	// execute with nil argument
	resp, err = conn.Execute(createTableQuery, nil)
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code == 0 {
		t.Fatalf("Unexpected response code: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}

	// execute with zero string
	resp, err = conn.Execute("", []interface{}{})
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code == 0 {
		t.Fatalf("Unexpected response code: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}

	// drop table query
	resp, err = conn.Execute(dropQuery2, []interface{}{})
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code != 0 {
		t.Fatalf("Failed to Execute: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of dropped spaces: %d", resp.SQLInfo.AffectedCount)
	}

	// drop the same table
	resp, err = conn.Execute(dropQuery2, []interface{}{})
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Code == 0 {
		t.Fatalf("Unexpected response code: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}
}

func TestSchema(t *testing.T) {
	var err error

	conn := connect(t, server, opts)
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
	if space, ok = schema.SpacesById[516]; !ok {
		t.Errorf("space with id = 516 was not found in schema.SpacesById")
	}
	if space2, ok = schema.Spaces["schematest"]; !ok {
		t.Errorf("space with name 'schematest' was not found in schema.SpacesById")
	}
	if space != space2 {
		t.Errorf("space with id = 516 and space with name schematest are different")
	}
	if space.Id != 516 {
		t.Errorf("space 516 has incorrect Id")
	}
	if space.Name != "schematest" {
		t.Errorf("space 516 has incorrect Name")
	}
	if !space.Temporary {
		t.Errorf("space 516 should be temporary")
	}
	if space.Engine != "memtx" {
		t.Errorf("space 516 engine should be memtx")
	}
	if space.FieldsCount != 7 {
		t.Errorf("space 516 has incorrect fields count")
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
		t.Fatalf("index field is nil")
	}
	if ifield1.Id != 1 || ifield2.Id != 2 {
		t.Errorf("index field has incorrect Id")
	}
	if (ifield1.Type != "num" && ifield1.Type != "unsigned") || (ifield2.Type != "STR" && ifield2.Type != "string") {
		t.Errorf("index field has incorrect Type '%s'", ifield2.Type)
	}

	var rSpaceNo, rIndexNo uint32
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex(516, 3)
	if err != nil || rSpaceNo != 516 || rIndexNo != 3 {
		t.Errorf("numeric space and index params not resolved as-is")
	}
	rSpaceNo, _, err = schema.ResolveSpaceIndex(516, nil)
	if err != nil || rSpaceNo != 516 {
		t.Errorf("numeric space param not resolved as-is")
	}
	rSpaceNo, rIndexNo, err = schema.ResolveSpaceIndex("schematest", "secondary")
	if err != nil || rSpaceNo != 516 || rIndexNo != 3 {
		t.Errorf("symbolic space and index params not resolved")
	}
	rSpaceNo, _, err = schema.ResolveSpaceIndex("schematest", nil)
	if err != nil || rSpaceNo != 516 {
		t.Errorf("symbolic space param not resolved")
	}
	_, _, err = schema.ResolveSpaceIndex("schematest22", "secondary")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing space name")
	}
	_, _, err = schema.ResolveSpaceIndex("schematest", "secondary22")
	if err == nil {
		t.Errorf("resolveSpaceIndex didn't returned error with not existing index name")
	}
}

func TestClientNamed(t *testing.T) {
	var resp *Response
	var err error

	conn := connect(t, server, opts)
	defer conn.Close()

	// Insert
	resp, err = conn.Insert(spaceName, []interface{}{uint(1001), "hello2", "world2"})
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp.Code != 0 {
		t.Errorf("Failed to Insert: wrong code returned %d", resp.Code)
	}

	// Delete
	resp, err = conn.Delete(spaceName, indexName, []interface{}{uint(1001)})
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Delete")
	}

	// Replace
	resp, err = conn.Replace(spaceName, []interface{}{uint(1002), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Replace: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Replace")
	}

	// Update
	resp, err = conn.Update(spaceName, indexName, []interface{}{uint(1002)}, []interface{}{[]interface{}{"=", 1, "bye"}, []interface{}{"#", 2, 1}})
	if err != nil {
		t.Fatalf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Update")
	}

	// Upsert
	if strings.Compare(conn.Greeting.Version, "Tarantool 1.6.7") >= 0 {
		resp, err = conn.Upsert(spaceName, []interface{}{uint(1003), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Fatalf("Failed to Upsert (insert): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (insert)")
		}
		resp, err = conn.Upsert(spaceName, []interface{}{uint(1003), 1}, []interface{}{[]interface{}{"+", 1, 1}})
		if err != nil {
			t.Fatalf("Failed to Upsert (update): %s", err.Error())
		}
		if resp == nil {
			t.Errorf("Response is nil after Upsert (update)")
		}
	}

	// Select
	for i := 1010; i < 1020; i++ {
		resp, err = conn.Replace(spaceName, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Fatalf("Failed to Replace: %s", err.Error())
		}
		if resp.Code != 0 {
			t.Errorf("Failed to Replace: wrong code returned %d", resp.Code)
		}
	}
	resp, err = conn.Select(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)})
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)}, &tpl)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err.Error())
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
}

func TestComplexStructs(t *testing.T) {
	var err error

	conn := connect(t, server, opts)
	defer conn.Close()

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Replace(spaceNo, &tuple)
	if err != nil {
		t.Fatalf("Failed to insert: %s", err.Error())
	}

	var tuples [1]Tuple2
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}, &tuples)
	if err != nil {
		t.Fatalf("Failed to selectTyped: %s", err.Error())
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
