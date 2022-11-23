package tarantool_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	. "github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	InitScript:   "config.lua",
	Listen:       server,
	WorkDir:      "work_dir",
	User:         opts.User,
	Pass:         opts.Pass,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 3,
	RetryTimeout: 500 * time.Millisecond,
}

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

func (m *Member) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := e.EncodeString(m.Name); err != nil {
		return err
	}
	if err := encodeUint(e, uint64(m.Val)); err != nil {
		return err
	}
	return nil
}

func (m *Member) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
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

// msgpack.v2 and msgpack.v5 return different uint types in responses. The
// function helps to unify a result.
func convertUint64(v interface{}) (result uint64, err error) {
	switch v := v.(type) {
	case uint:
		result = uint64(v)
	case uint8:
		result = uint64(v)
	case uint16:
		result = uint64(v)
	case uint32:
		result = uint64(v)
	case uint64:
		result = uint64(v)
	case int:
		result = uint64(v)
	case int8:
		result = uint64(v)
	case int16:
		result = uint64(v)
	case int32:
		result = uint64(v)
	case int64:
		result = uint64(v)
	default:
		err = fmt.Errorf("Non-number value %T", v)
	}
	return
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

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

func BenchmarkClientSerialRequestObject(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Error(err)
	}
	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1111)})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := conn.Do(req).Get()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkClientSerialRequestObjectWithContext(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err = conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Error(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		req := NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(IterEq).
			Key([]interface{}{uint(1111)}).
			Context(ctx)
		_, err := conn.Do(req).Get()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkClientSerialTyped(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

func BenchmarkClientSerialSQL(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
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

func BenchmarkClientSerialSQLPrepared(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("Failed to replace: %s", err)
	}

	stmt, err := conn.NewPrepared("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?")
	if err != nil {
		b.Fatalf("failed to prepare a SQL statement")
	}
	executeReq := NewExecutePreparedRequest(stmt)
	unprepareReq := NewUnprepareRequest(stmt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Do(executeReq.Args([]interface{}{uint(1111)})).Get()
		if err != nil {
			b.Errorf("Select failed: %s", err.Error())
			break
		}
	}
	_, err = conn.Do(unprepareReq).Get()
	if err != nil {
		b.Fatalf("failed to unprepare a SQL statement")
	}
}

func BenchmarkClientFuture(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

	conn := test_helpers.ConnectWithValidation(b, server, opts)
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
	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

func benchmarkClientParallelRequestObject(multiplier int, b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1111)})

	b.SetParallelism(multiplier)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = conn.Do(req)
			_, err := conn.Do(req).Get()
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func benchmarkClientParallelRequestObjectWithContext(multiplier int, b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1111)}).
		Context(ctx)

	b.SetParallelism(multiplier)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = conn.Do(req)
			_, err := conn.Do(req).Get()
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func benchmarkClientParallelRequestObjectMixed(multiplier int, b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1111)})

	reqWithCtx := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1111)}).
		Context(ctx)

	b.SetParallelism(multiplier)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = conn.Do(req)
			_, err := conn.Do(reqWithCtx).Get()
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkClientParallelRequestObject(b *testing.B) {
	multipliers := []int{10, 50, 500, 1000}
	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace(spaceNo, []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Fatal("No connection available")
	}

	for _, m := range multipliers {
		goroutinesNum := runtime.GOMAXPROCS(0) * m

		b.Run(fmt.Sprintf("Plain        %d goroutines", goroutinesNum), func(b *testing.B) {
			benchmarkClientParallelRequestObject(m, b)
		})

		b.Run(fmt.Sprintf("With Context %d goroutines", goroutinesNum), func(b *testing.B) {
			benchmarkClientParallelRequestObjectWithContext(m, b)
		})

		b.Run(fmt.Sprintf("Mixed        %d goroutines", goroutinesNum), func(b *testing.B) {
			benchmarkClientParallelRequestObjectMixed(m, b)
		})
	}
}

func BenchmarkClientParallelMassive(b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, server, opts)
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
	conn := test_helpers.ConnectWithValidation(b, server, opts)
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
	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

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
	conn := test_helpers.ConnectWithValidation(b, server, opts)
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

func BenchmarkClientParallelSQL(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
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

func BenchmarkClientParallelSQLPrepared(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	stmt, err := conn.NewPrepared("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?")
	if err != nil {
		b.Fatalf("failed to prepare a SQL statement")
	}
	executeReq := NewExecutePreparedRequest(stmt)
	unprepareReq := NewUnprepareRequest(stmt)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Do(executeReq.Args([]interface{}{uint(1111)})).Get()
			if err != nil {
				b.Errorf("Select failed: %s", err.Error())
				break
			}
		}
	})
	_, err = conn.Do(unprepareReq).Get()
	if err != nil {
		b.Fatalf("failed to unprepare a SQL statement")
	}
}

func BenchmarkSQLSerial(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, server, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
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

func TestFutureMultipleGetGetTyped(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	fut := conn.Call17Async("simple_concat", []interface{}{"1"})

	for i := 0; i < 30; i++ {
		// [0, 10) fut.Get()
		// [10, 20) fut.GetTyped()
		// [20, 30) Mix
		get := false
		if (i < 10) || (i >= 20 && i%2 == 0) {
			get = true
		}

		if get {
			resp, err := fut.Get()
			if err != nil {
				t.Errorf("Failed to call Get(): %s", err)
			}
			if val, ok := resp.Data[0].(string); !ok || val != "11" {
				t.Errorf("Wrong Get() result: %v", resp.Data)
			}
		} else {
			tpl := struct {
				Val string
			}{}
			err := fut.GetTyped(&tpl)
			if err != nil {
				t.Errorf("Failed to call GetTyped(): %s", err)
			}
			if tpl.Val != "11" {
				t.Errorf("Wrong GetTyped() result: %v", tpl)
			}
		}
	}
}

func TestFutureMultipleGetWithError(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	fut := conn.Call17Async("non_exist", []interface{}{"1"})

	for i := 0; i < 2; i++ {
		if _, err := fut.Get(); err == nil {
			t.Fatalf("An error expected")
		}
	}
}

func TestFutureMultipleGetTypedWithError(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	fut := conn.Call17Async("simple_concat", []interface{}{"1"})

	wrongTpl := struct {
		Val int
	}{}
	goodTpl := struct {
		Val string
	}{}

	if err := fut.GetTyped(&wrongTpl); err == nil {
		t.Fatalf("An error expected")
	}
	if err := fut.GetTyped(&goodTpl); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if goodTpl.Val != "11" {
		t.Fatalf("Wrong result: %s", goodTpl.Val)
	}
}

///////////////////

func TestClient(t *testing.T) {
	var resp *Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
		if id, err := convertUint64(tpl[0]); err != nil || id != 1 {
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
		if id, err := convertUint64(tpl[0]); err != nil || id != 1 {
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
		if id, err := convertUint64(tpl[0]); err != nil || id != 2 {
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
		if id, err := convertUint64(tpl[0]); err != nil || id != 2 {
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
		if id, err := convertUint64(tpl[0]); err != nil || id != 10 {
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
	resp, err = conn.Call16("simple_concat", []interface{}{"1"})
	if err != nil {
		t.Errorf("Failed to use Call16")
	}
	if val, ok := resp.Data[0].([]interface{})[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	resp, err = conn.Call17("simple_concat", []interface{}{"1"})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := resp.Data[0].(string); !ok || val != "11" {
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
	if val, err := convertUint64(resp.Data[0]); err != nil || val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}
}

func TestClientSessionPush(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
	} else if val, err := convertUint64(resp.Data[0]); err != nil || val != pushMax {
		t.Errorf("Result is not %d: %v", pushMax, resp.Data)
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
				if val, err := convertUint64(resp.Data[0]); err != nil || val != pushCnt {
					t.Errorf("Unexpected push data = %v", resp.Data)
				}
			} else {
				respCnt += 1
				if val, err := convertUint64(resp.Data[0]); err != nil || val != pushMax {
					t.Errorf("Result is not %d: %v", pushMax, resp.Data)
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

	// We can collect original responses after iterations.
	for _, fut := range []*Future{fut0, fut1, fut2} {
		resp, err := fut.Get()
		if err != nil {
			t.Errorf("Unable to call fut.Get(): %s", err)
		} else if val, err := convertUint64(resp.Data[0]); err != nil || val != pushMax {
			t.Errorf("Result is not %d: %v", pushMax, resp.Data)
		}

		tpl := struct {
			Val int
		}{}
		err = fut.GetTyped(&tpl)
		if err != nil {
			t.Errorf("Unable to call fut.GetTyped(): %s", err)
		} else if tpl.Val != pushMax {
			t.Errorf("Result is not %d: %d", pushMax, tpl.Val)
		}
	}
}

const (
	createTableQuery         = "CREATE TABLE SQL_SPACE (id STRING PRIMARY KEY, name STRING COLLATE \"unicode\" DEFAULT NULL);"
	insertQuery              = "INSERT INTO SQL_SPACE VALUES (?, ?);"
	selectNamedQuery         = "SELECT id, name FROM SQL_SPACE WHERE id=:id AND name=:name;"
	selectPosQuery           = "SELECT id, name FROM SQL_SPACE WHERE id=? AND name=?;"
	updateQuery              = "UPDATE SQL_SPACE SET name=? WHERE id=?;"
	enableFullMetaDataQuery  = "SET SESSION \"sql_full_metadata\" = true;"
	selectSpanDifQuery       = "SELECT id||id, name, id FROM SQL_SPACE WHERE name=?;"
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
	test_helpers.SkipIfSQLUnsupported(t)

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
			[]interface{}{"1", "test"},
			Response{
				SQLInfo:  SQLInfo{AffectedCount: 1},
				Data:     []interface{}{},
				MetaData: nil,
			},
		},
		{
			selectNamedQuery,
			map[string]interface{}{
				"id":   "1",
				"name": "test",
			},
			Response{
				SQLInfo: SQLInfo{AffectedCount: 0},
				Data:    []interface{}{[]interface{}{"1", "test"}},
				MetaData: []ColumnMetaData{
					{FieldType: "string", FieldName: "ID"},
					{FieldType: "string", FieldName: "NAME"}},
			},
		},
		{
			selectPosQuery,
			[]interface{}{"1", "test"},
			Response{
				SQLInfo: SQLInfo{AffectedCount: 0},
				Data:    []interface{}{[]interface{}{"1", "test"}},
				MetaData: []ColumnMetaData{
					{FieldType: "string", FieldName: "ID"},
					{FieldType: "string", FieldName: "NAME"}},
			},
		},
		{
			updateQuery,
			[]interface{}{"test_test", "1"},
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
				SQLInfo: SQLInfo{AffectedCount: 0}, Data: []interface{}{[]interface{}{"11", "test_test", "1"}},
				MetaData: []ColumnMetaData{
					{
						FieldType:            "string",
						FieldName:            "COLUMN_1",
						FieldIsNullable:      false,
						FieldIsAutoincrement: false,
						FieldSpan:            "id||id",
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
						FieldType:            "string",
						FieldName:            "ID",
						FieldIsNullable:      false,
						FieldIsAutoincrement: false,
						FieldSpan:            "id",
						FieldCollation:       "",
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
			[]interface{}{"2", "test_2"},
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

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
	test_helpers.SkipIfSQLUnsupported(t)

	// Data for test table
	testData := map[int]string{
		1: "test",
	}

	var resp *Response

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
		resp, err := conn.Execute(selectNamedQuery2, bind)
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

	resp, err := conn.Execute(selectPosQuery2, sqlBind5)
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
	test_helpers.SkipIfSQLUnsupported(t)

	var resp *Response

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	resp, err := conn.Execute(createTableQuery, []interface{}{})
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

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	stmt, err := conn.NewPrepared(selectNamedQuery2)
	if err != nil {
		t.Errorf("failed to prepare: %v", err)
	}

	executeReq := NewExecutePreparedRequest(stmt)
	unprepareReq := NewUnprepareRequest(stmt)

	resp, err := conn.Do(executeReq.Args([]interface{}{1, "test"})).Get()
	if err != nil {
		t.Errorf("failed to execute prepared: %v", err)
	}
	if resp.Code != OkCode {
		t.Errorf("failed to execute prepared: code %d", resp.Code)
	}
	if reflect.DeepEqual(resp.Data[0], []interface{}{1, "test"}) {
		t.Error("Select with named arguments failed")
	}
	if resp.MetaData[0].FieldType != "unsigned" ||
		resp.MetaData[0].FieldName != "NAME0" ||
		resp.MetaData[1].FieldType != "string" ||
		resp.MetaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}

	resp, err = conn.Do(unprepareReq).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
	}
	if resp.Code != OkCode {
		t.Errorf("failed to unprepare prepared statement: code %d", resp.Code)
	}

	_, err = conn.Do(unprepareReq).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = conn.Do(executeReq).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")

	prepareReq := NewPrepareRequest(selectNamedQuery2)
	resp, err = conn.Do(prepareReq).Get()
	if err != nil {
		t.Errorf("failed to prepare: %v", err)
	}
	if resp.Data == nil {
		t.Errorf("failed to prepare: Data is nil")
	}
	if resp.Code != OkCode {
		t.Errorf("failed to unprepare prepared statement: code %d", resp.Code)
	}

	if len(resp.Data) == 0 {
		t.Errorf("failed to prepare: response Data has no elements")
	}
	stmt, ok := resp.Data[0].(*Prepared)
	if !ok {
		t.Errorf("failed to prepare: failed to cast the response Data to Prepared object")
	}
	if stmt.StatementID == 0 {
		t.Errorf("failed to prepare: statement id is 0")
	}
}

func TestConnection_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool")

	conn1 := &Connection{}
	req := test_helpers.NewStrangerRequest()

	_, err := conn1.Do(req).Get()
	if err == nil {
		t.Fatalf("nil error caught")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("Unexpected error caught")
	}
}

func TestNewPreparedFromResponse(t *testing.T) {
	var (
		ErrNilResponsePassed = fmt.Errorf("passed nil response")
		ErrNilResponseData   = fmt.Errorf("response Data is nil")
		ErrWrongDataFormat   = fmt.Errorf("response Data format is wrong")
	)
	testConn := &Connection{}
	testCases := []struct {
		name          string
		resp          *Response
		expectedError error
	}{
		{"ErrNilResponsePassed", nil, ErrNilResponsePassed},
		{"ErrNilResponseData", &Response{Data: nil}, ErrNilResponseData},
		{"ErrWrongDataFormat", &Response{Data: []interface{}{}}, ErrWrongDataFormat},
		{"ErrWrongDataFormat", &Response{Data: []interface{}{"test"}}, ErrWrongDataFormat},
		{"nil", &Response{Data: []interface{}{&Prepared{}}}, nil},
	}
	for _, testCase := range testCases {
		t.Run("Expecting error "+testCase.name, func(t *testing.T) {
			_, err := NewPreparedFromResponse(testConn, testCase.resp)
			assert.Equal(t, err, testCase.expectedError)
		})
	}
}

func TestSchema(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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
		t.Errorf("ResolveSpaceIndex didn't returned error with not existing space name")
	}
	_, _, err = schema.ResolveSpaceIndex("schematest", "secondary22")
	if err == nil {
		t.Errorf("ResolveSpaceIndex didn't returned error with not existing index name")
	}
}

func TestClientNamed(t *testing.T) {
	var resp *Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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

func TestClientRequestObjects(t *testing.T) {
	var (
		req  Request
		resp *Response
		err  error
	)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Ping
	req = NewPingRequest()
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Ping")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Body len != 0")
	}

	// The code prepares data.
	for i := 1010; i < 1020; i++ {
		conn.Delete(spaceName, nil, []interface{}{uint(i)})
	}

	// Insert
	for i := 1010; i < 1020; i++ {
		req = NewInsertRequest(spaceName).
			Tuple([]interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		resp, err = conn.Do(req).Get()
		if err != nil {
			t.Fatalf("Failed to Insert: %s", err.Error())
		}
		if resp == nil {
			t.Fatalf("Response is nil after Insert")
		}
		if resp.Data == nil {
			t.Fatalf("Response data is nil after Insert")
		}
		if len(resp.Data) != 1 {
			t.Fatalf("Response Body len != 1")
		}
		if tpl, ok := resp.Data[0].([]interface{}); !ok {
			t.Errorf("Unexpected body of Insert")
		} else {
			if len(tpl) != 3 {
				t.Errorf("Unexpected body of Insert (tuple len)")
			}
			if id, err := convertUint64(tpl[0]); err != nil || id != uint64(i) {
				t.Errorf("Unexpected body of Insert (0)")
			}
			if h, ok := tpl[1].(string); !ok || h != fmt.Sprintf("val %d", i) {
				t.Errorf("Unexpected body of Insert (1)")
			}
			if h, ok := tpl[2].(string); !ok || h != "bla" {
				t.Errorf("Unexpected body of Insert (2)")
			}
		}
	}

	// Replace
	for i := 1015; i < 1020; i++ {
		req = NewReplaceRequest(spaceName).
			Tuple([]interface{}{uint(i), fmt.Sprintf("val %d", i), "blar"})
		resp, err = conn.Do(req).Get()
		if err != nil {
			t.Fatalf("Failed to Replace: %s", err.Error())
		}
		if resp == nil {
			t.Fatalf("Response is nil after Replace")
		}
		if resp.Data == nil {
			t.Fatalf("Response data is nil after Replace")
		}
		if len(resp.Data) != 1 {
			t.Fatalf("Response Body len != 1")
		}
		if tpl, ok := resp.Data[0].([]interface{}); !ok {
			t.Errorf("Unexpected body of Replace")
		} else {
			if len(tpl) != 3 {
				t.Errorf("Unexpected body of Replace (tuple len)")
			}
			if id, err := convertUint64(tpl[0]); err != nil || id != uint64(i) {
				t.Errorf("Unexpected body of Replace (0)")
			}
			if h, ok := tpl[1].(string); !ok || h != fmt.Sprintf("val %d", i) {
				t.Errorf("Unexpected body of Replace (1)")
			}
			if h, ok := tpl[2].(string); !ok || h != "blar" {
				t.Errorf("Unexpected body of Replace (2)")
			}
		}
	}

	// Delete
	req = NewDeleteRequest(spaceName).
		Key([]interface{}{uint(1016)})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Delete")
	}
	if resp.Data == nil {
		t.Fatalf("Response data is nil after Delete")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, err := convertUint64(tpl[0]); err != nil || id != uint64(1016) {
			t.Errorf("Unexpected body of Delete (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 1016" {
			t.Errorf("Unexpected body of Delete (1)")
		}
		if h, ok := tpl[2].(string); !ok || h != "blar" {
			t.Errorf("Unexpected body of Delete (2)")
		}
	}

	// Update without operations.
	req = NewUpdateRequest(spaceName).
		Index(indexName).
		Key([]interface{}{uint(1010)})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Update")
	}
	if resp.Data == nil {
		t.Fatalf("Response data is nil after Update")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != uint64(1010) {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 1010" {
			t.Errorf("Unexpected body of Update (1)")
		}
		if h, ok := tpl[2].(string); !ok || h != "bla" {
			t.Errorf("Unexpected body of Update (2)")
		}
	}

	// Update.
	req = NewUpdateRequest(spaceName).
		Index(indexName).
		Key([]interface{}{uint(1010)}).
		Operations(NewOperations().Assign(1, "bye").Insert(2, 1))
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Update: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Update")
	}
	if resp.Data == nil {
		t.Fatalf("Response data is nil after Update")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != 1010 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
		if h, err := convertUint64(tpl[2]); err != nil || h != 1 {
			t.Errorf("Unexpected body of Update (2)")
		}
	}

	// Upsert without operations.
	req = NewUpsertRequest(spaceNo).
		Tuple([]interface{}{uint(1010), "hi", "hi"})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Upsert (update): %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Upsert (update)")
	}
	if resp.Data == nil {
		t.Fatalf("Response data is nil after Upsert")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Upsert.
	req = NewUpsertRequest(spaceNo).
		Tuple([]interface{}{uint(1010), "hi", "hi"}).
		Operations(NewOperations().Assign(2, "bye"))
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Upsert (update): %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Upsert (update)")
	}
	if resp.Data == nil {
		t.Fatalf("Response data is nil after Upsert")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select.
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(20).
		Iterator(IterGe).
		Key([]interface{}{uint(1010)})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Errorf("Response is nil after Select")
		return
	}
	if len(resp.Data) != 9 {
		t.Fatalf("Response Data len %d != 9", len(resp.Data))
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != 1010 {
			t.Errorf("Unexpected body of Select (0) %v, expected %d", tpl[0], 1010)
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Select (1) %q, expected %q", tpl[1].(string), "bye")
		}
		if h, ok := tpl[2].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Select (2) %q, expected %q", tpl[2].(string), "bye")
		}
	}

	// Call16 vs Call17
	req = NewCall16Request("simple_concat").Args([]interface{}{"1"})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := resp.Data[0].([]interface{})[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// Call17
	req = NewCall17Request("simple_concat").Args([]interface{}{"1"})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call17")
	}
	if val, ok := resp.Data[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}

	// Eval
	req = NewEvalRequest("return 5 + 6")
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Eval: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Eval")
	}
	if len(resp.Data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	if val, err := convertUint64(resp.Data[0]); err != nil || val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}

	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version")
	}
	if isLess {
		return
	}

	req = NewExecuteRequest(createTableQuery)
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Body len != 0")
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Execute: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of created spaces: %d", resp.SQLInfo.AffectedCount)
	}

	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err.Error())
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Body len != 0")
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Execute: %d", resp.Code)
	}
	if resp.SQLInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of dropped spaces: %d", resp.SQLInfo.AffectedCount)
	}
}

func TestClientRequestObjectsWithNilContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()
	req := NewPingRequest().Context(nil) //nolint
	resp, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Ping")
	}
	if len(resp.Data) != 0 {
		t.Errorf("Response Body len != 0")
	}
}

func TestClientRequestObjectsWithPassedCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewPingRequest().Context(ctx)
	cancel()
	resp, err := conn.Do(req).Get()
	if err.Error() != "context is done" {
		t.Fatalf("Failed to catch an error from done context")
	}
	if resp != nil {
		t.Fatalf("Response is not nil after the occurred error")
	}
}

func TestClientRequestObjectsWithContext(t *testing.T) {
	var err error
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewPingRequest().Context(ctx)
	fut := conn.Do(req)
	cancel()
	resp, err := fut.Get()
	if resp != nil {
		t.Fatalf("response must be nil")
	}
	if err == nil {
		t.Fatalf("caught nil error")
	}
	if err.Error() != "context is done" {
		t.Fatalf("wrong error caught: %v", err)
	}
}

func TestComplexStructs(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
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

func TestStream_IdValues(t *testing.T) {
	test_helpers.SkipIfStreamsUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	cases := []uint64{
		1,
		128,
		math.MaxUint8,
		math.MaxUint8 + 1,
		math.MaxUint16,
		math.MaxUint16 + 1,
		math.MaxUint32,
		math.MaxUint32 + 1,
		math.MaxUint64,
	}

	stream, _ := conn.NewStream()
	req := NewPingRequest()

	for _, id := range cases {
		t.Run(fmt.Sprintf("%d", id), func(t *testing.T) {
			stream.Id = id
			_, err := stream.Do(req).Get()
			if err != nil {
				t.Fatalf("Failed to Ping: %s", err.Error())
			}
		})
	}
}

func TestStream_Commit(t *testing.T) {
	var req Request
	var resp *Response
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Begin: wrong code returned %d", resp.Code)
	}

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "hello2", "world2"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Errorf("Failed to Insert: wrong code returned %d", resp.Code)
	}
	defer test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{uint(1001)})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1001)})
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != 1001 {
			t.Fatalf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello2" {
			t.Fatalf("Unexpected body of Select (1)")
		}
		if h, ok := tpl[2].(string); !ok || h != "world2" {
			t.Fatalf("Unexpected body of Select (2)")
		}
	}

	// Commit transaction
	req = NewCommitRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Commit: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Commit: wrong code returned %d", resp.Code)
	}

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != 1001 {
			t.Fatalf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello2" {
			t.Fatalf("Unexpected body of Select (1)")
		}
		if h, ok := tpl[2].(string); !ok || h != "world2" {
			t.Fatalf("Unexpected body of Select (2)")
		}
	}
}

func TestStream_Rollback(t *testing.T) {
	var req Request
	var resp *Response
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Begin: wrong code returned %d", resp.Code)
	}

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "hello2", "world2"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Errorf("Failed to Insert: wrong code returned %d", resp.Code)
	}
	defer test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{uint(1001)})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]interface{}{uint(1001)})
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := convertUint64(tpl[0]); err != nil || id != 1001 {
			t.Fatalf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello2" {
			t.Fatalf("Unexpected body of Select (1)")
		}
		if h, ok := tpl[2].(string); !ok || h != "world2" {
			t.Fatalf("Unexpected body of Select (2)")
		}
	}

	// Rollback transaction
	req = NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Rollback: %s", err.Error())
	}
	if resp.Code != OkCode {
		t.Fatalf("Failed to Rollback: wrong code returned %d", resp.Code)
	}

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select inside of stream after rollback
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err.Error())
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	if len(resp.Data) != 0 {
		t.Fatalf("Response Data len != 0")
	}
}

func TestStream_TxnIsolationLevel(t *testing.T) {
	var req Request
	var resp *Response
	var err error
	var conn *Connection

	txnIsolationLevels := []TxnIsolationLevel{
		DefaultIsolationLevel,
		ReadCommittedLevel,
		ReadConfirmedLevel,
		BestEffortLevel,
	}

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Begin")
		require.NotNilf(t, resp, "response is nil after Begin")
		require.Equalf(t, OkCode, resp.Code, "wrong code returned")

		// Insert in stream
		req = NewInsertRequest(spaceName).
			Tuple([]interface{}{uint(1001), "hello2", "world2"})
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Insert")
		require.NotNilf(t, resp, "response is nil after Insert")
		require.Equalf(t, OkCode, resp.Code, "wrong code returned")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(IterEq).
			Key([]interface{}{uint(1001)})
		resp, err = conn.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.NotNilf(t, resp, "response is nil after Select")
		require.Equalf(t, 0, len(resp.Data), "response Data len != 0")

		// Select in stream
		resp, err = stream.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.NotNilf(t, resp, "response is nil after Select")
		require.Equalf(t, 1, len(resp.Data), "response Body len != 1 after Select")

		tpl, ok := resp.Data[0].([]interface{})
		require.Truef(t, ok, "unexpected body of Select")
		require.Equalf(t, 3, len(tpl), "unexpected body of Select")

		key, err := convertUint64(tpl[0])
		require.Nilf(t, err, "unexpected body of Select (0)")
		require.Equalf(t, uint64(1001), key, "unexpected body of Select (0)")

		value1, ok := tpl[1].(string)
		require.Truef(t, ok, "unexpected body of Select (1)")
		require.Equalf(t, "hello2", value1, "unexpected body of Select (1)")

		value2, ok := tpl[2].(string)
		require.Truef(t, ok, "unexpected body of Select (2)")
		require.Equalf(t, "world2", value2, "unexpected body of Select (2)")

		// Rollback transaction
		req = NewRollbackRequest()
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Rollback")
		require.NotNilf(t, resp, "response is nil after Rollback")
		require.Equalf(t, OkCode, resp.Code, "wrong code returned")

		// Select outside of transaction
		resp, err = conn.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.NotNilf(t, resp, "response is nil after Select")
		require.Equalf(t, 0, len(resp.Data), "response Data len != 0")

		// Select inside of stream after rollback
		resp, err = stream.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.NotNilf(t, resp, "response is nil after Select")
		require.Equalf(t, 0, len(resp.Data), "response Data len != 0")

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{uint(1001)})
	}
}

func TestStream_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request " +
		"doesn't belong to the current connection or connection pool")

	conn := &Connection{}
	stream, _ := conn.NewStream()
	req := test_helpers.NewStrangerRequest()

	_, err := stream.Do(req).Get()
	if err == nil {
		t.Fatalf("nil error has been caught")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("Unexpected error has been caught: %s", err.Error())
	}
}

func TestStream_DoWithClosedConn(t *testing.T) {
	expectedErr := fmt.Errorf("using closed connection")

	test_helpers.SkipIfStreamsUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)

	stream, _ := conn.NewStream()
	conn.Close()

	// Begin transaction
	req := NewBeginRequest()
	_, err := stream.Do(req).Get()
	if err == nil {
		t.Fatalf("nil error has been caught")
	}
	if !strings.Contains(err.Error(), expectedErr.Error()) {
		t.Fatalf("Unexpected error has been caught: %s", err.Error())
	}
}

func TestConnectionProtocolInfoSupported(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// First Tarantool protocol version (1, StreamsFeature and TransactionsFeature)
	// was introduced between 2.10.0-beta1 and 2.10.0-beta2.
	// Versions 2 (ErrorExtensionFeature) and 3 (WatchersFeature) were also
	// introduced between 2.10.0-beta1 and 2.10.0-beta2. Version 4
	// (PaginationFeature) was introduced in master 948e5cd (possible 2.10.5 or
	// 2.11.0). So each release Tarantool >= 2.10 (same as each Tarantool with
	// id support) has protocol version >= 3 and first four features.
	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []ProtocolFeature{
			StreamsFeature,
			TransactionsFeature,
			ErrorExtensionFeature,
			WatchersFeature,
		},
	}

	clientProtocolInfo := conn.ClientProtocolInfo()
	require.Equal(t,
		clientProtocolInfo,
		ProtocolInfo{
			Version:  ProtocolVersion(4),
			Features: []ProtocolFeature{StreamsFeature, TransactionsFeature},
		})

	serverProtocolInfo := conn.ServerProtocolInfo()
	require.GreaterOrEqual(t,
		serverProtocolInfo.Version,
		tarantool210ProtocolInfo.Version)
	require.Subset(t,
		serverProtocolInfo.Features,
		tarantool210ProtocolInfo.Features)
}

func TestClientIdRequestObject(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []ProtocolFeature{
			StreamsFeature,
			TransactionsFeature,
			ErrorExtensionFeature,
			WatchersFeature,
		},
	}

	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []ProtocolFeature{StreamsFeature},
	})
	resp, err := conn.Do(req).Get()
	require.Nilf(t, err, "No errors on Id request execution")
	require.NotNilf(t, resp, "Response not empty")
	require.NotNilf(t, resp.Data, "Response data not empty")
	require.Equal(t, len(resp.Data), 1, "Response data contains exactly one object")

	serverProtocolInfo, ok := resp.Data[0].(ProtocolInfo)
	require.Truef(t, ok, "Response Data object is an ProtocolInfo object")
	require.GreaterOrEqual(t,
		serverProtocolInfo.Version,
		tarantool210ProtocolInfo.Version)
	require.Subset(t,
		serverProtocolInfo.Features,
		tarantool210ProtocolInfo.Features)
}

func TestClientIdRequestObjectWithNilContext(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []ProtocolFeature{
			StreamsFeature,
			TransactionsFeature,
			ErrorExtensionFeature,
			WatchersFeature,
		},
	}

	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []ProtocolFeature{StreamsFeature},
	}).Context(nil) //nolint
	resp, err := conn.Do(req).Get()
	require.Nilf(t, err, "No errors on Id request execution")
	require.NotNilf(t, resp, "Response not empty")
	require.NotNilf(t, resp.Data, "Response data not empty")
	require.Equal(t, len(resp.Data), 1, "Response data contains exactly one object")

	serverProtocolInfo, ok := resp.Data[0].(ProtocolInfo)
	require.Truef(t, ok, "Response Data object is an ProtocolInfo object")
	require.GreaterOrEqual(t,
		serverProtocolInfo.Version,
		tarantool210ProtocolInfo.Version)
	require.Subset(t,
		serverProtocolInfo.Features,
		tarantool210ProtocolInfo.Features)
}

func TestClientIdRequestObjectWithPassedCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []ProtocolFeature{StreamsFeature},
	}).Context(ctx) //nolint
	cancel()
	resp, err := conn.Do(req).Get()
	require.Nilf(t, resp, "Response is empty")
	require.NotNilf(t, err, "Error is not empty")
	require.Equal(t, err.Error(), "context is done")
}

func TestClientIdRequestObjectWithContext(t *testing.T) {
	var err error
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []ProtocolFeature{StreamsFeature},
	}).Context(ctx) //nolint
	fut := conn.Do(req)
	cancel()
	resp, err := fut.Get()
	require.Nilf(t, resp, "Response is empty")
	require.NotNilf(t, err, "Error is not empty")
	require.Equal(t, err.Error(), "context is done")
}

func TestConnectionProtocolInfoUnsupported(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	clientProtocolInfo := conn.ClientProtocolInfo()
	require.Equal(t,
		clientProtocolInfo,
		ProtocolInfo{
			Version:  ProtocolVersion(4),
			Features: []ProtocolFeature{StreamsFeature, TransactionsFeature},
		})

	serverProtocolInfo := conn.ServerProtocolInfo()
	require.Equal(t, serverProtocolInfo, ProtocolInfo{})
}

func TestConnectionClientFeaturesUmmutable(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	info := conn.ClientProtocolInfo()
	infoOrig := info.Clone()
	info.Features[0] = ProtocolFeature(15532)

	require.Equal(t, conn.ClientProtocolInfo(), infoOrig)
	require.NotEqual(t, conn.ClientProtocolInfo(), info)
}

func TestConnectionServerFeaturesUmmutable(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	info := conn.ServerProtocolInfo()
	infoOrig := info.Clone()
	info.Features[0] = ProtocolFeature(15532)

	require.Equal(t, conn.ServerProtocolInfo(), infoOrig)
	require.NotEqual(t, conn.ServerProtocolInfo(), info)
}

func TestConnectionProtocolVersionRequirementSuccess(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	connOpts := opts.Clone()
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Version: ProtocolVersion(3),
	}

	conn, err := Connect(server, connOpts)

	require.Nilf(t, err, "No errors on connect")
	require.NotNilf(t, conn, "Connect success")

	conn.Close()
}

func TestConnectionProtocolVersionRequirementFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	connOpts := opts.Clone()
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Version: ProtocolVersion(3),
	}

	conn, err := Connect(server, connOpts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t, err.Error(), "identify: protocol version 3 is not supported")
}

func TestConnectionProtocolFeatureRequirementSuccess(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	connOpts := opts.Clone()
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Features: []ProtocolFeature{TransactionsFeature},
	}

	conn, err := Connect(server, connOpts)

	require.NotNilf(t, conn, "Connect success")
	require.Nilf(t, err, "No errors on connect")

	conn.Close()
}

func TestConnectionProtocolFeatureRequirementFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	connOpts := opts.Clone()
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Features: []ProtocolFeature{TransactionsFeature},
	}

	conn, err := Connect(server, connOpts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t, err.Error(), "identify: protocol feature TransactionsFeature is not supported")
}

func TestConnectionProtocolFeatureRequirementManyFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	connOpts := opts.Clone()
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Features: []ProtocolFeature{TransactionsFeature, ProtocolFeature(15532)},
	}

	conn, err := Connect(server, connOpts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t,
		err.Error(),
		"identify: protocol features TransactionsFeature, Unknown feature (code 15532) are not supported")
}

func TestConnectionFeatureOptsImmutable(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	restartOpts := startOpts
	restartOpts.Listen = "127.0.0.1:3014"
	inst, err := test_helpers.StartTarantool(restartOpts)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	retries := uint(10)
	timeout := 100 * time.Millisecond

	connOpts := opts.Clone()
	connOpts.Reconnect = timeout
	connOpts.MaxReconnects = retries
	connOpts.RequiredProtocolInfo = ProtocolInfo{
		Features: []ProtocolFeature{TransactionsFeature},
	}

	// Connect with valid opts
	conn := test_helpers.ConnectWithValidation(t, server, connOpts)
	defer conn.Close()

	// Change opts outside
	connOpts.RequiredProtocolInfo.Features[0] = ProtocolFeature(15532)

	// Trigger reconnect with opts re-check
	test_helpers.StopTarantool(inst)
	err = test_helpers.RestartTarantool(&inst)
	require.Nilf(t, err, "Failed to restart tarantool")

	connected := test_helpers.WaitUntilReconnected(conn, retries, timeout)
	require.True(t, connected, "Reconnect success")
}

func TestErrorExtendedInfoBasic(t *testing.T) {
	test_helpers.SkipIfErrorExtendedInfoUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	_, err := conn.Eval("not a Lua code", []interface{}{})
	require.NotNilf(t, err, "expected error on invalid Lua code")

	ttErr, ok := err.(Error)
	require.Equalf(t, ok, true, "error is built from a Tarantool error")

	expected := BoxError{
		Type:  "LuajitError",
		File:  "eval",
		Line:  uint64(1),
		Msg:   "eval:1: unexpected symbol near 'not'",
		Errno: uint64(0),
		Code:  uint64(32),
	}

	// In fact, CheckEqualBoxErrors does not check than File and Line
	// of connector BoxError are equal to the Tarantool ones
	// since they may differ between different Tarantool versions
	// and editions.
	test_helpers.CheckEqualBoxErrors(t, expected, *ttErr.ExtendedInfo)
}

func TestErrorExtendedInfoStack(t *testing.T) {
	test_helpers.SkipIfErrorExtendedInfoUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	_, err := conn.Eval("error(chained_error)", []interface{}{})
	require.NotNilf(t, err, "expected error on explicit error raise")

	ttErr, ok := err.(Error)
	require.Equalf(t, ok, true, "error is built from a Tarantool error")

	expected := BoxError{
		Type:  "ClientError",
		File:  "config.lua",
		Line:  uint64(214),
		Msg:   "Timeout exceeded",
		Errno: uint64(0),
		Code:  uint64(78),
		Prev: &BoxError{
			Type:  "ClientError",
			File:  "config.lua",
			Line:  uint64(213),
			Msg:   "Unknown error",
			Errno: uint64(0),
			Code:  uint64(0),
		},
	}

	// In fact, CheckEqualBoxErrors does not check than File and Line
	// of connector BoxError are equal to the Tarantool ones
	// since they may differ between different Tarantool versions
	// and editions.
	test_helpers.CheckEqualBoxErrors(t, expected, *ttErr.ExtendedInfo)
}

func TestErrorExtendedInfoFields(t *testing.T) {
	test_helpers.SkipIfErrorExtendedInfoUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	_, err := conn.Eval("error(access_denied_error)", []interface{}{})
	require.NotNilf(t, err, "expected error on forbidden action")

	ttErr, ok := err.(Error)
	require.Equalf(t, ok, true, "error is built from a Tarantool error")

	expected := BoxError{
		Type:  "AccessDeniedError",
		File:  "/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c",
		Line:  uint64(535),
		Msg:   "Execute access to function 'forbidden_function' is denied for user 'no_grants'",
		Errno: uint64(0),
		Code:  uint64(42),
		Fields: map[string]interface{}{
			"object_type": "function",
			"object_name": "forbidden_function",
			"access_type": "Execute",
		},
	}

	// In fact, CheckEqualBoxErrors does not check than File and Line
	// of connector BoxError are equal to the Tarantool ones
	// since they may differ between different Tarantool versions
	// and editions.
	test_helpers.CheckEqualBoxErrors(t, expected, *ttErr.ExtendedInfo)
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	// Tarantool supports streams and interactive transactions since version 2.10.0
	isStreamUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Could not check the Tarantool version")
	}

	startOpts.MemtxUseMvccEngine = !isStreamUnsupported

	inst, err := test_helpers.StartTarantool(startOpts)
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
