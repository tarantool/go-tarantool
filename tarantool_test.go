package tarantool_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var startOpts test_helpers.StartOpts = test_helpers.StartOpts{
	Dialer:       dialer,
	InitScript:   "config.lua",
	Listen:       server,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 10,
	RetryTimeout: 500 * time.Millisecond,
}

var dialer = NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

type Member struct {
	Name  string
	Nonce string
	Val   uint
}

var contextDoneErrRegexp = regexp.MustCompile(`^context is done \(request ID [0-9]+\)$`)

func (m *Member) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(2); err != nil {
		return err
	}
	if err := e.EncodeString(m.Name); err != nil {
		return err
	}
	if err := e.EncodeUint(uint64(m.Val)); err != nil {
		return err
	}
	return nil
}

func (m *Member) DecodeMsgpack(d *msgpack.Decoder) error {
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

var server = "127.0.0.1:3013"
var fdDialerTestServer = "127.0.0.1:3014"
var spaceNo = uint32(617)
var spaceName = "test"
var indexNo = uint32(0)
var indexName = "primary"
var opts = Opts{
	Timeout: 5 * time.Second,
	//Concurrency: 32,
	//RateLimit: 4*1024,
}

const N = 500

func BenchmarkClientSerial(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("Failed to replace: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Execute("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?",
			[]interface{}{uint(1111)})
		if err != nil {
			b.Errorf("Select failed: %s", err.Error())
			break
		}
	}
}

func BenchmarkClientSerialSQLPrepared(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer conn.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Replace("test_perf", []interface{}{uint(1), "hello", []interface{}{}})
			if err != nil {
				b.Error(err)
			}
		}
	})
}

func BenchmarkClientLargeSelectParallel(b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer conn.Close()

	offset, limit := uint32(0), uint32(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Select("test_perf", "secondary", offset, limit, IterEq,
				[]interface{}{"test_name"})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClientParallelSQL(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("No connection available")
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := conn.Execute("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?",
				[]interface{}{uint(1111)})
			if err != nil {
				b.Errorf("Select failed: %s", err.Error())
				break
			}
		}
	})
}

func BenchmarkClientParallelSQLPrepared(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer conn.Close()

	_, err := conn.Replace("SQL_TEST", []interface{}{uint(1111), "hello", "world"})
	if err != nil {
		b.Errorf("Failed to replace: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := conn.Execute("SELECT NAME0,NAME1,NAME2 FROM SQL_TEST WHERE NAME0=?",
			[]interface{}{uint(1111)})
		if err != nil {
			b.Errorf("Select failed: %s", err.Error())
			break
		}
	}
}

type mockRequest struct {
	conn *Connection
}

func (req *mockRequest) Type() iproto.Type {
	return iproto.Type(0)
}

func (req *mockRequest) Async() bool {
	return false
}

func (req *mockRequest) Body(resolver SchemaResolver, enc *msgpack.Encoder) error {
	return nil
}

func (req *mockRequest) Conn() *Connection {
	return req.conn
}

func (req *mockRequest) Ctx() context.Context {
	return nil
}

func (req *mockRequest) Response(header Header,
	body io.Reader) (Response, error) {
	return nil, fmt.Errorf("some error")
}

func TestNetDialer(t *testing.T) {
	assert := assert.New(t)
	require := require.New(t)

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, DialOpts{})
	require.Nil(err)
	require.NotNil(conn)
	defer conn.Close()

	assert.Equal(server, conn.Addr().String())
	assert.NotEqual("", conn.Greeting().Version)

	// Write IPROTO_PING.
	ping := []byte{
		0xce, 0x00, 0x00, 0x00, 0xa, // Length.
		0x82, // Header map.
		0x00, 0x40,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x02,
		0x80, // Empty map.
	}
	ret, err := conn.Write(ping)
	require.Equal(len(ping), ret)
	require.Nil(err)
	require.Nil(conn.Flush())

	// Read IPROTO_PING response length.
	lenbuf := make([]byte, 5)
	ret, err = io.ReadFull(conn, lenbuf)
	require.Nil(err)
	require.Equal(len(lenbuf), ret)
	length := int(binary.BigEndian.Uint32(lenbuf[1:]))
	require.Greater(length, 0)

	// Read IPROTO_PING response.
	buf := make([]byte, length)
	ret, err = io.ReadFull(conn, buf)
	require.Nil(err)
	require.Equal(len(buf), ret)
	// Check that it is IPROTO_OK.
	assert.Equal([]byte{0x83, 0x00, 0xce, 0x00, 0x00, 0x00, 0x00}, buf[:7])
}

func TestNetDialer_BadUser(t *testing.T) {
	badDialer := NetDialer{
		Address:  server,
		User:     "Cpt Smollett",
		Password: "none",
	}
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := Connect(ctx, badDialer, opts)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "failed to authenticate")
	if conn != nil {
		conn.Close()
		t.Errorf("connection is not nil")
	}
}

// NetDialer does not work with PapSha256Auth, no matter the Tarantool version
// and edition.
func TestNetDialer_PapSha256Auth(t *testing.T) {
	authDialer := AuthDialer{
		Dialer:   dialer,
		Username: "test",
		Password: "test",
		Auth:     PapSha256Auth,
	}
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := authDialer.Dial(ctx, DialOpts{})
	if conn != nil {
		conn.Close()
		t.Fatalf("Connection created successfully")
	}

	assert.ErrorContains(t, err, "failed to authenticate")
}

func TestFutureMultipleGetGetTyped(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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
			data, err := fut.Get()
			if err != nil {
				t.Errorf("Failed to call Get(): %s", err)
			}
			if val, ok := data[0].(string); !ok || val != "11" {
				t.Errorf("Wrong Get() result: %v", data)
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
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	fut := conn.Call17Async("non_exist", []interface{}{"1"})

	for i := 0; i < 2; i++ {
		if _, err := fut.Get(); err == nil {
			t.Fatalf("An error expected")
		}
	}
}

func TestFutureMultipleGetTypedWithError(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Ping
	data, err := conn.Ping()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err)
	}
	if data != nil {
		t.Fatalf("Response data is not nil after Ping")
	}

	// Insert
	data, err = conn.Insert(spaceNo, []interface{}{uint(1), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err)
	}
	if len(data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Insert")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Insert (tuple len)")
		}
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1 {
			t.Errorf("Unexpected body of Insert (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Insert (1)")
		}
	}
	data, err = conn.Insert(spaceNo, &Tuple{Id: 1, Msg: "hello", Name: "world"})
	if tntErr, ok := err.(Error); !ok || tntErr.Code != iproto.ER_TUPLE_FOUND {
		t.Errorf("Expected %s but got: %v", iproto.ER_TUPLE_FOUND, err)
	}
	if len(data) != 0 {
		t.Errorf("Response Body len != 0")
	}

	// Delete
	data, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(1)})
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err)
	}
	if len(data) != 1 {
		t.Errorf("Response Body len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1 {
			t.Errorf("Unexpected body of Delete (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hello" {
			t.Errorf("Unexpected body of Delete (1)")
		}
	}
	data, err = conn.Delete(spaceNo, indexNo, []interface{}{uint(101)})
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err)
	}
	if len(data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Replace
	data, err = conn.Replace(spaceNo, []interface{}{uint(2), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Replace: %s", err)
	}
	if data == nil {
		t.Fatalf("Response is nil after Replace")
	}
	data, err = conn.Replace(spaceNo, []interface{}{uint(2), "hi", "planet"})
	if err != nil {
		t.Fatalf("Failed to Replace (duplicate): %s", err)
	}
	if len(data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Replace")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Replace (tuple len)")
		}
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 2 {
			t.Errorf("Unexpected body of Replace (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "hi" {
			t.Errorf("Unexpected body of Replace (1)")
		}
	}

	// Update
	data, err = conn.Update(spaceNo, indexNo, []interface{}{uint(2)},
		NewOperations().Assign(1, "bye").Delete(2, 1))
	if err != nil {
		t.Fatalf("Failed to Update: %s", err)
	}
	if len(data) != 1 {
		t.Errorf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if len(tpl) != 2 {
			t.Errorf("Unexpected body of Update (tuple len)")
		}
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 2 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
	}

	// Upsert
	data, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1},
		NewOperations().Add(1, 1))
	if err != nil {
		t.Fatalf("Failed to Upsert (insert): %s", err)
	}
	if data == nil {
		t.Fatalf("Response is nil after Upsert (insert)")
	}
	data, err = conn.Upsert(spaceNo, []interface{}{uint(3), 1},
		NewOperations().Add(1, 1))
	if err != nil {
		t.Fatalf("Failed to Upsert (update): %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Upsert (update)")
	}

	// Select
	for i := 10; i < 20; i++ {
		data, err = conn.Replace(spaceNo, []interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Fatalf("Failed to Replace: %s", err)
		}
		if data == nil {
			t.Errorf("Response is nil after Replace")
		}
	}
	data, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)})
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 10 {
			t.Errorf("Unexpected body of Select (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "val 10" {
			t.Errorf("Unexpected body of Select (1)")
		}
	}

	// Select empty
	data, err = conn.Select(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)})
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 0 {
		t.Errorf("Response Data len != 0")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err)
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else if tpl[0].Id != 10 {
		t.Errorf("Bad value loaded from SelectTyped")
	}

	// Get Typed
	var singleTpl = Tuple{}
	err = conn.GetTyped(spaceNo, indexNo, []interface{}{uint(10)}, &singleTpl)
	if err != nil {
		t.Fatalf("Failed to GetTyped: %s", err)
	}
	if singleTpl.Id != 10 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed for one tuple
	var tpl1 [1]Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(10)}, &tpl1)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err)
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	} else if tpl[0].Id != 10 {
		t.Errorf("Bad value loaded from SelectTyped")
	}

	// Get Typed Empty
	var singleTpl2 Tuple
	err = conn.GetTyped(spaceNo, indexNo, []interface{}{uint(30)}, &singleTpl2)
	if err != nil {
		t.Fatalf("Failed to GetTyped: %s", err)
	}
	if singleTpl2.Id != 0 {
		t.Errorf("Bad value loaded from GetTyped")
	}

	// Select Typed Empty
	var tpl2 []Tuple
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{uint(30)}, &tpl2)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err)
	}
	if len(tpl2) != 0 {
		t.Errorf("Result len of SelectTyped != 1")
	}

	// Call16
	data, err = conn.Call16("box.info", []interface{}{"box.schema.SPACE_ID"})
	if err != nil {
		t.Fatalf("Failed to Call16: %s", err)
	}
	if len(data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}

	// Call16 vs Call17
	data, err = conn.Call16("simple_concat", []interface{}{"1"})
	if err != nil {
		t.Errorf("Failed to use Call16")
	}
	if val, ok := data[0].([]interface{})[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}

	data, err = conn.Call17("simple_concat", []interface{}{"1"})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := data[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}

	// Eval
	data, err = conn.Eval("return 5 + 6", []interface{}{})
	if err != nil {
		t.Fatalf("Failed to Eval: %s", err)
	}
	if len(data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	if val, err := test_helpers.ConvertUint64(data[0]); err != nil || val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}
}

func TestClientSessionPush(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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
	data, err := fut1.Get()
	if err != nil {
		t.Errorf("Failed to Call17: %s", err)
	} else if len(data) < 1 {
		t.Errorf("Response.Data is empty after Call17Async")
	} else if val, err := test_helpers.ConvertUint64(data[0]); err != nil || val != pushMax {
		t.Errorf("Result is not %d: %v", pushMax, data)
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
			resp := it.Value()
			if resp == nil {
				t.Errorf("Response is empty after it.Next() == true")
				break
			}
			data, err := resp.Decode()
			if err != nil {
				t.Errorf("Failed to Decode: %s", err)
				break
			}
			if len(data) < 1 {
				t.Errorf("Response.Data is empty after CallAsync")
				break
			}
			if it.IsPush() {
				pushCnt += 1
				val, err := test_helpers.ConvertUint64(data[0])
				if err != nil || val != pushCnt {
					t.Errorf("Unexpected push data = %v", data)
				}
			} else {
				respCnt += 1
				val, err := test_helpers.ConvertUint64(data[0])
				if err != nil || val != pushMax {
					t.Errorf("Result is not %d: %v", pushMax, data)
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
		data, err := fut.Get()
		if err != nil {
			t.Errorf("Unable to call fut.Get(): %s", err)
		}
		val, err := test_helpers.ConvertUint64(data[0])
		if err != nil || val != pushMax {
			t.Errorf("Result is not %d: %v", pushMax, data)
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
	createTableQuery = "CREATE TABLE SQL_SPACE (ID STRING PRIMARY KEY, NAME " +
		"STRING COLLATE \"unicode\" DEFAULT NULL);"
	insertQuery              = "INSERT INTO SQL_SPACE VALUES (?, ?);"
	selectNamedQuery         = "SELECT ID, NAME FROM SQL_SPACE WHERE ID=:ID AND NAME=:NAME;"
	selectPosQuery           = "SELECT ID, NAME FROM SQL_SPACE WHERE ID=? AND NAME=?;"
	updateQuery              = "UPDATE SQL_SPACE SET NAME=? WHERE ID=?;"
	enableFullMetaDataQuery  = "SET SESSION \"sql_full_metadata\" = true;"
	selectSpanDifQueryNew    = "SELECT ID||ID, NAME, ID FROM seqscan SQL_SPACE WHERE NAME=?;"
	selectSpanDifQueryOld    = "SELECT ID||ID, NAME, ID FROM SQL_SPACE WHERE NAME=?;"
	alterTableQuery          = "ALTER TABLE SQL_SPACE RENAME TO SQL_SPACE2;"
	insertIncrQuery          = "INSERT INTO SQL_SPACE2 VALUES (?, ?);"
	deleteQuery              = "DELETE FROM SQL_SPACE2 WHERE NAME=?;"
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
		Query    string
		Args     interface{}
		sqlInfo  SQLInfo
		data     []interface{}
		metaData []ColumnMetaData
	}

	selectSpanDifQuery := selectSpanDifQueryNew
	if isSeqScanOld, err := test_helpers.IsTarantoolVersionLess(3, 0, 0); err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	} else if isSeqScanOld {
		selectSpanDifQuery = selectSpanDifQueryOld
	}

	testCases := []testCase{
		{
			createTableQuery,
			[]interface{}{},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			insertQuery,
			[]interface{}{"1", "test"},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			selectNamedQuery,
			map[string]interface{}{
				"ID":   "1",
				"NAME": "test",
			},
			SQLInfo{AffectedCount: 0},
			[]interface{}{[]interface{}{"1", "test"}},
			[]ColumnMetaData{
				{FieldType: "string", FieldName: "ID"},
				{FieldType: "string", FieldName: "NAME"}},
		},
		{
			selectPosQuery,
			[]interface{}{"1", "test"},
			SQLInfo{AffectedCount: 0},
			[]interface{}{[]interface{}{"1", "test"}},
			[]ColumnMetaData{
				{FieldType: "string", FieldName: "ID"},
				{FieldType: "string", FieldName: "NAME"}},
		},
		{
			updateQuery,
			[]interface{}{"test_test", "1"},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			enableFullMetaDataQuery,
			[]interface{}{},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			selectSpanDifQuery,
			[]interface{}{"test_test"},
			SQLInfo{AffectedCount: 0},
			[]interface{}{[]interface{}{"11", "test_test", "1"}},
			[]ColumnMetaData{
				{
					FieldType:            "string",
					FieldName:            "COLUMN_1",
					FieldIsNullable:      false,
					FieldIsAutoincrement: false,
					FieldSpan:            "ID||ID",
				},
				{
					FieldType:            "string",
					FieldName:            "NAME",
					FieldIsNullable:      true,
					FieldIsAutoincrement: false,
					FieldSpan:            "NAME",
					FieldCollation:       "unicode",
				},
				{
					FieldType:            "string",
					FieldName:            "ID",
					FieldIsNullable:      false,
					FieldIsAutoincrement: false,
					FieldSpan:            "ID",
					FieldCollation:       "",
				},
			},
		},
		{
			alterTableQuery,
			[]interface{}{},
			SQLInfo{AffectedCount: 0},
			[]interface{}{},
			nil,
		},
		{
			insertIncrQuery,
			[]interface{}{"2", "test_2"},
			SQLInfo{AffectedCount: 1, InfoAutoincrementIds: []uint64{1}},
			[]interface{}{},
			nil,
		},
		{
			deleteQuery,
			[]interface{}{"test_2"},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			dropQuery,
			[]interface{}{},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
		{
			disableFullMetaDataQuery,
			[]interface{}{},
			SQLInfo{AffectedCount: 1},
			[]interface{}{},
			nil,
		},
	}

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	for i, test := range testCases {
		req := NewExecuteRequest(test.Query).Args(test.Args)
		resp, err := conn.Do(req).GetResponse()
		assert.NoError(t, err, "Failed to Execute, query: %s", test.Query)
		assert.NotNil(t, resp, "Response is nil after Execute\nQuery number: %d", i)
		data, err := resp.Decode()
		assert.NoError(t, err, "Failed to Decode")
		for j := range data {
			assert.Equal(t, data[j], test.data[j], "Response data is wrong")
		}
		exResp, ok := resp.(*ExecuteResponse)
		assert.True(t, ok, "Got wrong response type")
		sqlInfo, err := exResp.SQLInfo()
		assert.NoError(t, err, "Error while getting SQLInfo")
		assert.Equal(t, sqlInfo.AffectedCount, test.sqlInfo.AffectedCount,
			"Affected count is wrong")

		errorMsg := "Response Metadata is wrong"
		metaData, err := exResp.MetaData()
		assert.NoError(t, err, "Error while getting MetaData")
		for j := range metaData {
			assert.Equal(t, metaData[j], test.metaData[j], errorMsg)
		}
	}
}

func TestSQLTyped(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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

	var resp Response

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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

	// positioned sql bind
	sqlBind5 := []interface{}{
		1, "test",
	}

	// mixed sql bind
	sqlBind6 := []interface{}{
		KeyValueBind{Key: "name0", Value: 1},
		"test",
	}

	for _, bind := range namedSQLBinds {
		req := NewExecuteRequest(selectNamedQuery2).Args(bind)
		resp, err := conn.Do(req).GetResponse()
		if err != nil {
			t.Fatalf("Failed to Execute: %s", err)
		}
		if resp == nil {
			t.Fatal("Response is nil after Execute")
		}
		data, err := resp.Decode()
		if err != nil {
			t.Errorf("Failed to Decode: %s", err)
		}
		if reflect.DeepEqual(data[0], []interface{}{1, testData[1]}) {
			t.Error("Select with named arguments failed")
		}
		exResp, ok := resp.(*ExecuteResponse)
		assert.True(t, ok, "Got wrong response type")
		metaData, err := exResp.MetaData()
		assert.NoError(t, err, "Error while getting MetaData")
		if metaData[0].FieldType != "unsigned" ||
			metaData[0].FieldName != "NAME0" ||
			metaData[1].FieldType != "string" ||
			metaData[1].FieldName != "NAME1" {
			t.Error("Wrong metadata")
		}
	}

	req := NewExecuteRequest(selectPosQuery2).Args(sqlBind5)
	resp, err := conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	data, err := resp.Decode()
	if err != nil {
		t.Errorf("Failed to Decode: %s", err)
	}
	if reflect.DeepEqual(data[0], []interface{}{1, testData[1]}) {
		t.Error("Select with positioned arguments failed")
	}
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err := exResp.MetaData()
	assert.NoError(t, err, "Error while getting MetaData")
	if metaData[0].FieldType != "unsigned" ||
		metaData[0].FieldName != "NAME0" ||
		metaData[1].FieldType != "string" ||
		metaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}

	req = NewExecuteRequest(mixedQuery).Args(sqlBind6)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	data, err = resp.Decode()
	if err != nil {
		t.Errorf("Failed to Decode: %s", err)
	}
	if reflect.DeepEqual(data[0], []interface{}{1, testData[1]}) {
		t.Error("Select with positioned arguments failed")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err = exResp.MetaData()
	assert.NoError(t, err, "Error while getting MetaData")
	if metaData[0].FieldType != "unsigned" ||
		metaData[0].FieldName != "NAME0" ||
		metaData[1].FieldType != "string" ||
		metaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}
}

func TestStressSQL(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	req := NewExecuteRequest(createTableQuery)
	resp, err := conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to create an Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	assert.NoError(t, err, "Error while getting SQLInfo")
	if sqlInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}

	// create table with the same name
	req = NewExecuteRequest(createTableQuery)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to create an Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	_, err = resp.Decode()
	assert.NotNil(t, err, "Expected error while decoding")

	tntErr, ok := err.(Error)
	assert.True(t, ok)
	assert.Equal(t, iproto.ER_SPACE_EXISTS, tntErr.Code)
	if resp.Header().Error != iproto.ER_SPACE_EXISTS {
		t.Fatalf("Unexpected response error: %d", resp.Header().Error)
	}
	prevErr := err

	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.Equal(t, prevErr, err)
	if sqlInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}

	// execute with nil argument
	req = NewExecuteRequest(createTableQuery).Args(nil)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to create an Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Header().Error == ErrorNo {
		t.Fatal("Unexpected successful Execute")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.NotNil(t, err, "Expected an error")
	if sqlInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}

	// execute with zero string
	req = NewExecuteRequest("")
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to create an Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Header().Error == ErrorNo {
		t.Fatal("Unexpected successful Execute")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.NotNil(t, err, "Expected an error")
	if sqlInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}

	// drop table query
	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.NoError(t, err, "Error while getting SQLInfo")
	if sqlInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of dropped spaces: %d", sqlInfo.AffectedCount)
	}

	// drop the same table
	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to create an Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	if resp.Header().Error == ErrorNo {
		t.Fatal("Unexpected successful Execute")
	}
	_, err = resp.Decode()
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	if err == nil {
		t.Fatal("Unexpected lack of error")
	}
	if sqlInfo.AffectedCount != 0 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	stmt, err := conn.NewPrepared(selectNamedQuery2)
	if err != nil {
		t.Errorf("failed to prepare: %v", err)
	}

	executeReq := NewExecutePreparedRequest(stmt)
	unprepareReq := NewUnprepareRequest(stmt)

	resp, err := conn.Do(executeReq.Args([]interface{}{1, "test"})).GetResponse()
	if err != nil {
		t.Errorf("failed to execute prepared: %v", err)
	}
	data, err := resp.Decode()
	if err != nil {
		t.Errorf("Failed to Decode: %s", err)
	}
	if reflect.DeepEqual(data[0], []interface{}{1, "test"}) {
		t.Error("Select with named arguments failed")
	}
	prepResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err := prepResp.MetaData()
	assert.NoError(t, err, "Error while getting MetaData")
	if metaData[0].FieldType != "unsigned" ||
		metaData[0].FieldName != "NAME0" ||
		metaData[1].FieldType != "string" ||
		metaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}

	_, err = conn.Do(unprepareReq).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
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
	data, err = conn.Do(prepareReq).Get()
	if err != nil {
		t.Errorf("failed to prepare: %v", err)
	}
	if data == nil {
		t.Errorf("failed to prepare: Data is nil")
	}

	if len(data) == 0 {
		t.Errorf("failed to prepare: response Data has no elements")
	}
	stmt, ok = data[0].(*Prepared)
	if !ok {
		t.Errorf("failed to prepare: failed to cast the response Data to Prepared object")
	}
	if stmt.StatementID == 0 {
		t.Errorf("failed to prepare: statement id is 0")
	}
}

func TestConnection_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to the current" +
		" connection or connection pool")

	conn1 := &Connection{}
	req := test_helpers.NewMockRequest()

	_, err := conn1.Do(req).Get()
	if err == nil {
		t.Fatalf("nil error caught")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("Unexpected error caught")
	}
}

func TestConnection_SetResponse_failed(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	req := mockRequest{conn}
	fut := conn.Do(&req)

	data, err := fut.Get()
	assert.EqualError(t, err, "failed to set response: some error")
	assert.Nil(t, data)
}

func TestGetSchema(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	s, err := GetSchema(conn)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if s.Version != 0 || s.Spaces[spaceName].Id != spaceNo {
		t.Errorf("GetSchema() returns incorrect schema")
	}
}

func TestConnection_SetSchema_Changes(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	req := NewInsertRequest(spaceName)
	req.Tuple([]interface{}{uint(1010), "Tarantool"})
	_, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err)
	}

	s, err := GetSchema(conn)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	conn.SetSchema(s)

	// Check if changes of the SetSchema result will do nothing to the
	// connection schema.
	s.Spaces[spaceName] = Space{}

	reqS := NewSelectRequest(spaceName)
	reqS.Key([]interface{}{uint(1010)})
	data, err := conn.Do(reqS).Get()
	if err != nil {
		t.Fatalf("failed to Select: %s", err)
	}
	if data[0].([]interface{})[1] != "Tarantool" {
		t.Errorf("wrong Select body: %v", data)
	}
}

func TestSchema(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Schema
	schema, err := GetSchema(conn)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if schema.SpacesById == nil {
		t.Errorf("schema.SpacesById is nil")
	}
	if schema.Spaces == nil {
		t.Errorf("schema.Spaces is nil")
	}
	var space, space2 Space
	var ok bool
	if space, ok = schema.SpacesById[616]; !ok {
		t.Errorf("space with id = 616 was not found in schema.SpacesById")
	}
	if space2, ok = schema.Spaces["schematest"]; !ok {
		t.Errorf("space with name 'schematest' was not found in schema.SpacesById")
	}
	assert.Equal(t, space, space2,
		"space with id = 616 and space with name schematest are different")
	if space.Id != 616 {
		t.Errorf("space 616 has incorrect Id")
	}
	if space.Name != "schematest" {
		t.Errorf("space 616 has incorrect Name")
	}
	if !space.Temporary {
		t.Errorf("space 616 should be temporary")
	}
	if space.Engine != "memtx" {
		t.Errorf("space 616 engine should be memtx")
	}
	if space.FieldsCount != 8 {
		t.Errorf("space 616 has incorrect fields count")
	}

	if space.FieldsById == nil {
		t.Errorf("space.FieldsById is nill")
	}
	if space.Fields == nil {
		t.Errorf("space.Fields is nill")
	}
	if len(space.FieldsById) != 7 {
		t.Errorf("space.FieldsById len is incorrect")
	}
	if len(space.Fields) != 7 {
		t.Errorf("space.Fields len is incorrect")
	}

	var field1, field2, field5, field1n, field5n Field
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

	var index0, index3, index0n, index3n Index
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
	assert.Equal(t, index0, index0n,
		"index with id = 0 and index with name 'primary' are different")
	assert.Equal(t, index3, index3n,
		"index with id = 3 and index with name 'secondary' are different")
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
	if (ifield1 == IndexField{}) || (ifield2 == IndexField{}) {
		t.Fatalf("index field is nil")
	}
	if ifield1.Id != 1 || ifield2.Id != 2 {
		t.Errorf("index field has incorrect Id")
	}
	if (ifield1.Type != "num" && ifield1.Type != "unsigned") ||
		(ifield2.Type != "STR" && ifield2.Type != "string") {
		t.Errorf("index field has incorrect Type '%s'", ifield2.Type)
	}
}

func TestSchema_IsNullable(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	schema, err := GetSchema(conn)
	if err != nil {
		t.Errorf("unexpected error: %s", err.Error())
	}
	if schema.Spaces == nil {
		t.Errorf("schema.Spaces is nil")
	}

	var space Space
	var ok bool
	if space, ok = schema.SpacesById[616]; !ok {
		t.Errorf("space with id = 616 was not found in schema.SpacesById")
	}

	var field, field_nullable Field
	for i := 0; i <= 5; i++ {
		name := fmt.Sprintf("name%d", i)
		if field, ok = space.Fields[name]; !ok {
			t.Errorf("field name = %s was not found", name)
		}
		if field.IsNullable {
			t.Errorf("field %s has incorrect IsNullable", name)
		}
	}
	if field_nullable, ok = space.Fields["nullable"]; !ok {
		t.Errorf("field name = nullable was not found")
	}
	if !field_nullable.IsNullable {
		t.Errorf("field nullable has incorrect IsNullable")
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
		resp          Response
		expectedError error
	}{
		{"ErrNilResponsePassed", nil, ErrNilResponsePassed},
		{"ErrNilResponseData", test_helpers.NewMockResponse(t, nil),
			ErrNilResponseData},
		{"ErrWrongDataFormat", test_helpers.NewMockResponse(t, []interface{}{}),
			ErrWrongDataFormat},
		{"ErrWrongDataFormat", test_helpers.NewMockResponse(t, []interface{}{"test"}),
			ErrWrongDataFormat},
	}
	for _, testCase := range testCases {
		t.Run("Expecting error "+testCase.name, func(t *testing.T) {
			_, err := NewPreparedFromResponse(testConn, testCase.resp)
			assert.Equal(t, testCase.expectedError, err)
		})
	}
}

func TestClientNamed(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Insert
	data, err := conn.Insert(spaceName, []interface{}{uint(1001), "hello2", "world2"})
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Insert")
	}

	// Delete
	data, err = conn.Delete(spaceName, indexName, []interface{}{uint(1001)})
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Delete")
	}

	// Replace
	data, err = conn.Replace(spaceName, []interface{}{uint(1002), "hello", "world"})
	if err != nil {
		t.Fatalf("Failed to Replace: %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Replace")
	}

	// Update
	data, err = conn.Update(spaceName, indexName,
		[]interface{}{
			uint(1002)},
		NewOperations().Assign(1, "buy").Delete(2, 1))
	if err != nil {
		t.Fatalf("Failed to Update: %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Update")
	}

	// Upsert
	data, err = conn.Upsert(spaceName,
		[]interface{}{uint(1003), 1}, NewOperations().Add(1, 1))
	if err != nil {
		t.Fatalf("Failed to Upsert (insert): %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Upsert (insert)")
	}
	data, err = conn.Upsert(spaceName,
		[]interface{}{uint(1003), 1}, NewOperations().Add(1, 1))
	if err != nil {
		t.Fatalf("Failed to Upsert (update): %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Upsert (update)")
	}

	// Select
	for i := 1010; i < 1020; i++ {
		data, err = conn.Replace(spaceName,
			[]interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if err != nil {
			t.Fatalf("Failed to Replace: %s", err)
		}
		if data == nil {
			t.Errorf("Response is nil after Replace")
		}
	}
	data, err = conn.Select(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)})
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if data == nil {
		t.Errorf("Response is nil after Select")
	}

	// Select Typed
	var tpl []Tuple
	err = conn.SelectTyped(spaceName, indexName, 0, 1, IterEq, []interface{}{uint(1010)}, &tpl)
	if err != nil {
		t.Fatalf("Failed to SelectTyped: %s", err)
	}
	if len(tpl) != 1 {
		t.Errorf("Result len of SelectTyped != 1")
	}
}

func TestClientRequestObjects(t *testing.T) {
	var (
		req Request
		err error
	)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Ping
	req = NewPingRequest()
	data, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err)
	}
	if len(data) != 0 {
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
		data, err = conn.Do(req).Get()
		if err != nil {
			t.Fatalf("Failed to Insert: %s", err)
		}
		if len(data) != 1 {
			t.Fatalf("Response Body len != 1")
		}
		if tpl, ok := data[0].([]interface{}); !ok {
			t.Errorf("Unexpected body of Insert")
		} else {
			if len(tpl) != 3 {
				t.Errorf("Unexpected body of Insert (tuple len)")
			}
			if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != uint64(i) {
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
		data, err = conn.Do(req).Get()
		if err != nil {
			t.Fatalf("Failed to Decode: %s", err)
		}
		if len(data) != 1 {
			t.Fatalf("Response Body len != 1")
		}
		if tpl, ok := data[0].([]interface{}); !ok {
			t.Errorf("Unexpected body of Replace")
		} else {
			if len(tpl) != 3 {
				t.Errorf("Unexpected body of Replace (tuple len)")
			}
			if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != uint64(i) {
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
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Delete: %s", err)
	}
	if data == nil {
		t.Fatalf("Response data is nil after Delete")
	}
	if len(data) != 1 {
		t.Fatalf("Response Body len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Delete")
	} else {
		if len(tpl) != 3 {
			t.Errorf("Unexpected body of Delete (tuple len)")
		}
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != uint64(1016) {
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
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Update: %s", err)
	}
	if data == nil {
		t.Fatalf("Response data is nil after Update")
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Update")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != uint64(1010) {
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
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Update: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Errorf("Unexpected body of Select")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1010 {
			t.Errorf("Unexpected body of Update (0)")
		}
		if h, ok := tpl[1].(string); !ok || h != "bye" {
			t.Errorf("Unexpected body of Update (1)")
		}
		if h, err := test_helpers.ConvertUint64(tpl[2]); err != nil || h != 1 {
			t.Errorf("Unexpected body of Update (2)")
		}
	}

	// Upsert without operations.
	req = NewUpsertRequest(spaceNo).
		Tuple([]interface{}{uint(1010), "hi", "hi"})
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Upsert (update): %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Upsert.
	req = NewUpsertRequest(spaceNo).
		Tuple([]interface{}{uint(1010), "hi", "hi"}).
		Operations(NewOperations().Assign(2, "bye"))
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to Upsert (update): %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Call16 vs Call17
	req = NewCall16Request("simple_concat").Args([]interface{}{"1"})
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := data[0].([]interface{})[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}

	// Call17
	req = NewCall17Request("simple_concat").Args([]interface{}{"1"})
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call17")
	}
	if val, ok := data[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}

	// Eval
	req = NewEvalRequest("return 5 + 6")
	data, err = conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Eval: %s", err)
	}
	if len(data) < 1 {
		t.Errorf("Response.Data is empty after Eval")
	}
	if val, err := test_helpers.ConvertUint64(data[0]); err != nil || val != 11 {
		t.Errorf("5 + 6 == 11, but got %v", val)
	}

	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	if err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	}
	if isLess {
		return
	}

	req = NewExecuteRequest(createTableQuery)
	resp, err := conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	data, err = resp.Decode()
	if err != nil {
		t.Fatalf("Failed to Decode: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Body len != 0")
	}
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	assert.NoError(t, err, "Error while getting SQLInfo")
	if sqlInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of created spaces: %d", sqlInfo.AffectedCount)
	}

	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		t.Fatalf("Failed to Execute: %s", err)
	}
	if resp == nil {
		t.Fatal("Response is nil after Execute")
	}
	data, err = resp.Decode()
	if err != nil {
		t.Fatalf("Failed to Decode: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Body len != 0")
	}
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.NoError(t, err, "Error while getting SQLInfo")
	if sqlInfo.AffectedCount != 1 {
		t.Errorf("Incorrect count of dropped spaces: %d", sqlInfo.AffectedCount)
	}
}

func testConnectionDoSelectRequestPrepare(t *testing.T, conn Connector) {
	t.Helper()

	for i := 1010; i < 1020; i++ {
		req := NewReplaceRequest(spaceName).Tuple(
			[]interface{}{uint(i), fmt.Sprintf("val %d", i), "bla"})
		if _, err := conn.Do(req).Get(); err != nil {
			t.Fatalf("Unable to prepare tuples: %s", err)
		}
	}
}

func testConnectionDoSelectRequestCheck(t *testing.T,
	resp *SelectResponse, err error, pos bool, dataLen int, firstKey uint64) {
	t.Helper()

	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if resp == nil {
		t.Fatalf("Response is nil after Select")
	}
	respPos, err := resp.Pos()
	if err != nil {
		t.Errorf("Error while getting Pos: %s", err)
	}
	if !pos && respPos != nil {
		t.Errorf("Response should not have a position descriptor")
	}
	if pos && respPos == nil {
		t.Fatalf("A response must have a position descriptor")
	}
	data, err := resp.Decode()
	if err != nil {
		t.Fatalf("Failed to Decode: %s", err)
	}
	if len(data) != dataLen {
		t.Fatalf("Response Data len %d != %d", len(data), dataLen)
	}
	for i := 0; i < dataLen; i++ {
		key := firstKey + uint64(i)
		if tpl, ok := data[i].([]interface{}); !ok {
			t.Errorf("Unexpected body of Select")
		} else {
			if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != key {
				t.Errorf("Unexpected body of Select (0) %v, expected %d",
					tpl[0], key)
			}
			expectedSecond := fmt.Sprintf("val %d", key)
			if h, ok := tpl[1].(string); !ok || h != expectedSecond {
				t.Errorf("Unexpected body of Select (1) %q, expected %q",
					tpl[1].(string), expectedSecond)
			}
			if h, ok := tpl[2].(string); !ok || h != "bla" {
				t.Errorf("Unexpected body of Select (2) %q, expected %q",
					tpl[2].(string), "bla")
			}
		}
	}
}

func TestConnectionDoSelectRequest(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(20).
		Iterator(IterGe).
		Key([]interface{}{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, false, 10, 1010)
}

func TestConnectionDoWatchOnceRequest(t *testing.T) {
	test_helpers.SkipIfWatchOnceUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	_, err := conn.Do(NewBroadcastRequest("hello").Value("world")).Get()
	if err != nil {
		t.Fatalf("Failed to create a broadcast : %s", err.Error())
	}

	data, err := conn.Do(NewWatchOnceRequest("hello")).Get()
	if err != nil {
		t.Fatalf("Failed to WatchOnce: %s", err.Error())
	}
	if len(data) < 1 || data[0] != "world" {
		t.Errorf("Failed to WatchOnce: wrong value returned %v", data)
	}
}

func TestConnectionDoWatchOnceOnEmptyKey(t *testing.T) {
	test_helpers.SkipIfWatchOnceUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	data, err := conn.Do(NewWatchOnceRequest("notexists!")).Get()
	if err != nil {
		t.Fatalf("Failed to WatchOnce: %s", err.Error())
	}
	if len(data) > 0 {
		t.Errorf("Failed to WatchOnce: wrong value returned %v", data)
	}
}

func TestConnectionDoSelectRequest_fetch_pos(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]interface{}{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1010)
}

func TestConnectDoSelectRequest_after_tuple(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]interface{}{uint(1010)}).
		After([]interface{}{uint(1012)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1013)
}

func TestConnectionDoSelectRequest_pagination_pos(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]interface{}{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1010)

	selPos, err := selResp.Pos()
	assert.NoError(t, err, "Error while getting Pos")

	resp, err = conn.Do(req.After(selPos)).GetResponse()
	selResp, ok = resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1012)
}

func TestConnection_Call(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	data, err := conn.Call("simple_concat", []interface{}{"1"})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := data[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}
}

func TestCallRequest(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	req := NewCallRequest("simple_concat").Args([]interface{}{"1"})
	data, err := conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if val, ok := data[0].(string); !ok || val != "11" {
		t.Errorf("result is not {{1}} : %v", data)
	}
}

func TestClientRequestObjectsWithNilContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()
	req := NewPingRequest().Context(nil) //nolint
	data, err := conn.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Ping: %s", err)
	}
	if len(data) != 0 {
		t.Errorf("Response Body len != 0")
	}
}

func TestClientRequestObjectsWithPassedCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewPingRequest().Context(ctx)
	cancel()
	resp, err := conn.Do(req).Get()
	if !contextDoneErrRegexp.Match([]byte(err.Error())) {
		t.Fatalf("Failed to catch an error from done context")
	}
	if resp != nil {
		t.Fatalf("Response is not nil after the occurred error")
	}
}

// waitCtxRequest waits for the WaitGroup in Body() call and returns
// the context from Ctx() call. The request helps us to make sure that
// the context's cancel() call is called before a response received.
type waitCtxRequest struct {
	ctx context.Context
	wg  sync.WaitGroup
}

func (req *waitCtxRequest) Type() iproto.Type {
	return NewPingRequest().Type()
}

func (req *waitCtxRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	req.wg.Wait()
	return NewPingRequest().Body(res, enc)
}

func (req *waitCtxRequest) Ctx() context.Context {
	return req.ctx
}

func (req *waitCtxRequest) Async() bool {
	return NewPingRequest().Async()
}

func (req *waitCtxRequest) Response(header Header, body io.Reader) (Response, error) {
	resp, err := test_helpers.CreateMockResponse(header, body)
	return resp, err
}

func TestClientRequestObjectsWithContext(t *testing.T) {
	var err error
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := &waitCtxRequest{ctx: ctx}
	req.wg.Add(1)

	var futWg sync.WaitGroup
	var fut *Future

	futWg.Add(1)
	go func() {
		defer futWg.Done()
		fut = conn.Do(req)
	}()

	cancel()
	req.wg.Done()

	futWg.Wait()
	if fut == nil {
		t.Fatalf("fut must be not nil")
	}

	resp, err := fut.Get()
	if resp != nil {
		t.Fatalf("response must be nil")
	}
	if err == nil {
		t.Fatalf("caught nil error")
	}
	if !contextDoneErrRegexp.Match([]byte(err.Error())) {
		t.Fatalf("wrong error caught: %v", err)
	}
}

func TestComplexStructs(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Replace(spaceNo, &tuple)
	if err != nil {
		t.Fatalf("Failed to insert: %s", err)
	}

	var tuples [1]Tuple2
	err = conn.SelectTyped(spaceNo, indexNo, 0, 1, IterEq, []interface{}{777}, &tuples)
	if err != nil {
		t.Fatalf("Failed to selectTyped: %s", err)
	}

	if len(tuples) != 1 {
		t.Errorf("Failed to selectTyped: unexpected array length %d", len(tuples))
		return
	}

	if tuple.Cid != tuples[0].Cid ||
		len(tuple.Members) != len(tuples[0].Members) ||
		tuple.Members[1].Name != tuples[0].Members[1].Name {
		t.Errorf("Failed to selectTyped: incorrect data")
		return
	}
}

func TestStream_IdValues(t *testing.T) {
	test_helpers.SkipIfStreamsUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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
				t.Fatalf("Failed to Ping: %s", err)
			}
		})
	}
}

func TestStream_Commit(t *testing.T) {
	var req Request
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err)
	}

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "hello2", "world2"})
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err)
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
	data, err := conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1001 {
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
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Commit: %s", err)
	}

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1001 {
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
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err)
	}

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]interface{}{uint(1001), "hello2", "world2"})
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err)
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
	data, err := conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 1 {
		t.Fatalf("Response Data len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		t.Fatalf("Unexpected body of Select")
	} else {
		if id, err := test_helpers.ConvertUint64(tpl[0]); err != nil || id != 1001 {
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
	_, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Rollback: %s", err)
	}

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}

	// Select inside of stream after rollback
	_, err = stream.Do(selectReq).Get()
	if err != nil {
		t.Fatalf("Failed to Select: %s", err)
	}
	if len(data) != 0 {
		t.Fatalf("Response Data len != 0")
	}
}

func TestStream_TxnIsolationLevel(t *testing.T) {
	var req Request
	var err error
	var conn *Connection

	txnIsolationLevels := []TxnIsolationLevel{
		DefaultIsolationLevel,
		ReadCommittedLevel,
		ReadConfirmedLevel,
		BestEffortLevel,
	}

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	stream, _ := conn.NewStream()

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		_, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Begin")

		// Insert in stream
		req = NewInsertRequest(spaceName).
			Tuple([]interface{}{uint(1001), "hello2", "world2"})
		_, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Insert")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(IterEq).
			Key([]interface{}{uint(1001)})
		data, err := conn.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 0, len(data), "response Data len != 0")

		// Select in stream
		data, err = stream.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 1, len(data), "response Body len != 1 after Select")

		tpl, ok := data[0].([]interface{})
		require.Truef(t, ok, "unexpected body of Select")
		require.Equalf(t, 3, len(tpl), "unexpected body of Select")

		key, err := test_helpers.ConvertUint64(tpl[0])
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
		_, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Rollback")

		// Select outside of transaction
		data, err = conn.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 0, len(data), "response Data len != 0")

		// Select inside of stream after rollback
		data, err = stream.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 0, len(data), "response Data len != 0")

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{uint(1001)})
	}
}

func TestStream_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request " +
		"doesn't belong to the current connection or connection pool")

	conn := &Connection{}
	stream, _ := conn.NewStream()
	req := test_helpers.NewMockRequest()

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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)

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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// First Tarantool protocol version (1, IPROTO_FEATURE_STREAMS and
	// IPROTO_FEATURE_TRANSACTIONS) was introduced between 2.10.0-beta1 and
	// 2.10.0-beta2. Versions 2 (IPROTO_FEATURE_ERROR_EXTENSION) and
	// 3 (IPROTO_FEATURE_WATCHERS) were also introduced between 2.10.0-beta1 and
	// 2.10.0-beta2. Version 4 (IPROTO_FEATURE_PAGINATION) was introduced in
	// master 948e5cd (possible 2.10.5 or 2.11.0). So each release
	// Tarantool >= 2.10 (same as each Tarantool with id support) has protocol
	// version >= 3 and first four features.
	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_STREAMS,
			iproto.IPROTO_FEATURE_TRANSACTIONS,
			iproto.IPROTO_FEATURE_ERROR_EXTENSION,
			iproto.IPROTO_FEATURE_WATCHERS,
		},
	}

	serverProtocolInfo := conn.ProtocolInfo()
	require.GreaterOrEqual(t,
		serverProtocolInfo.Version,
		tarantool210ProtocolInfo.Version)
	require.Subset(t,
		serverProtocolInfo.Features,
		tarantool210ProtocolInfo.Features)
}

func TestClientIdRequestObject(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_STREAMS,
			iproto.IPROTO_FEATURE_TRANSACTIONS,
			iproto.IPROTO_FEATURE_ERROR_EXTENSION,
			iproto.IPROTO_FEATURE_WATCHERS,
		},
	}

	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_STREAMS},
	})
	data, err := conn.Do(req).Get()
	require.Nilf(t, err, "No errors on Id request execution")
	require.NotNilf(t, data, "Response data not empty")
	require.Equal(t, len(data), 1, "Response data contains exactly one object")

	serverProtocolInfo, ok := data[0].(ProtocolInfo)
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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	tarantool210ProtocolInfo := ProtocolInfo{
		Version: ProtocolVersion(3),
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_STREAMS,
			iproto.IPROTO_FEATURE_TRANSACTIONS,
			iproto.IPROTO_FEATURE_ERROR_EXTENSION,
			iproto.IPROTO_FEATURE_WATCHERS,
		},
	}

	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_STREAMS},
	}).Context(nil) //nolint
	data, err := conn.Do(req).Get()
	require.Nilf(t, err, "No errors on Id request execution")
	require.NotNilf(t, data, "Response data not empty")
	require.Equal(t, len(data), 1, "Response data contains exactly one object")

	serverProtocolInfo, ok := data[0].(ProtocolInfo)
	require.Truef(t, ok, "Response Data object is an ProtocolInfo object")
	require.GreaterOrEqual(t,
		serverProtocolInfo.Version,
		tarantool210ProtocolInfo.Version)
	require.Subset(t,
		serverProtocolInfo.Features,
		tarantool210ProtocolInfo.Features)
}

func TestClientIdRequestObjectWithPassedCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_STREAMS},
	}).Context(ctx) //nolint
	cancel()
	resp, err := conn.Do(req).Get()
	require.Nilf(t, resp, "Response is empty")
	require.NotNilf(t, err, "Error is not empty")
	require.Regexp(t, contextDoneErrRegexp, err.Error())
}

func TestConnectionProtocolInfoUnsupported(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	serverProtocolInfo := conn.ProtocolInfo()
	expected := ProtocolInfo{}
	require.Equal(t, expected, serverProtocolInfo)
}

func TestConnectionServerFeaturesImmutable(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	info := conn.ProtocolInfo()
	infoOrig := info.Clone()
	info.Features[0] = iproto.Feature(15532)

	require.Equal(t, conn.ProtocolInfo(), infoOrig)
	require.NotEqual(t, conn.ProtocolInfo(), info)
}

func TestConnectionProtocolVersionRequirementSuccess(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{
		Version: ProtocolVersion(3),
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, testDialer, opts)

	require.Nilf(t, err, "No errors on connect")
	require.NotNilf(t, conn, "Connect success")

	conn.Close()
}

func TestConnectionProtocolVersionRequirementFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{
		Version: ProtocolVersion(3),
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, testDialer, opts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t, err.Error(), "invalid server protocol: protocol version 3 is not supported")
}

func TestConnectionProtocolFeatureRequirementSuccess(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_TRANSACTIONS},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, testDialer, opts)

	require.NotNilf(t, conn, "Connect success")
	require.Nilf(t, err, "No errors on connect")

	conn.Close()
}

func TestConnectionProtocolFeatureRequirementFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_TRANSACTIONS},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, testDialer, opts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t, err.Error(),
		"invalid server protocol: protocol feature "+
			"IPROTO_FEATURE_TRANSACTIONS is not supported")
}

func TestConnectionProtocolFeatureRequirementManyFail(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_TRANSACTIONS,
			iproto.Feature(15532)},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, testDialer, opts)

	require.Nilf(t, conn, "Connect fail")
	require.NotNilf(t, err, "Got error on connect")
	require.Contains(t,
		err.Error(),
		"invalid server protocol: protocol features IPROTO_FEATURE_TRANSACTIONS, "+
			"Feature(15532) are not supported")
}

func TestErrorExtendedInfoBasic(t *testing.T) {
	test_helpers.SkipIfErrorExtendedInfoUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
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

func TestConnection_NewWatcher(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnection_NewWatcher"
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}
	defer watcher.Unregister()

	select {
	case event := <-events:
		if event.Conn != conn {
			t.Errorf("Unexpected event connection: %v", event.Conn)
		}
		if event.Key != key {
			t.Errorf("Unexpected event key: %s", event.Key)
		}
		if event.Value != nil {
			t.Errorf("Unexpected event value: %v", event.Value)
		}
	case <-time.After(time.Second):
		t.Fatalf("Failed to get watch event.")
	}
}

func TestConnection_NewWatcher_noWatchersFeature(t *testing.T) {
	test_helpers.SkipIfWatchersSupported(t)

	const key = "TestConnection_NewWatcher_noWatchersFeature"
	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{Features: []iproto.Feature{}}
	conn := test_helpers.ConnectWithValidation(t, testDialer, opts)
	defer conn.Close()

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {})
	require.Nilf(t, watcher, "watcher must not be created")
	require.NotNilf(t, err, "an error is expected")
	expected := "the feature IPROTO_FEATURE_WATCHERS must be supported by " +
		"connection to create a watcher"
	require.Equal(t, expected, err.Error())
}

func TestConnection_NewWatcher_reconnect(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnection_NewWatcher_reconnect"
	const server = "127.0.0.1:3014"

	testDialer := dialer
	testDialer.Address = server

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       testDialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)
	if err != nil {
		t.Fatalf("Unable to start Tarantool: %s", err)
	}

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 10

	conn := test_helpers.ConnectWithValidation(t, testDialer, reconnectOpts)
	defer conn.Close()

	events := make(chan WatchEvent)
	defer close(events)
	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}
	defer watcher.Unregister()

	<-events

	test_helpers.StopTarantool(inst)
	if err := test_helpers.RestartTarantool(inst); err != nil {
		t.Fatalf("Unable to restart Tarantool: %s", err)
	}

	maxTime := reconnectOpts.Reconnect * time.Duration(reconnectOpts.MaxReconnects)
	select {
	case <-events:
	case <-time.After(maxTime):
		t.Fatalf("Failed to get watch event.")
	}
}

func TestBroadcastRequest(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestBroadcastRequest"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	data, err := conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	if err != nil {
		t.Fatalf("Got broadcast error: %s", err)
	}
	if !reflect.DeepEqual(data, []interface{}{}) {
		t.Errorf("Got unexpected broadcast response data: %v", data)
	}

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}
	defer watcher.Unregister()

	select {
	case event := <-events:
		if event.Conn != conn {
			t.Errorf("Unexpected event connection: %v", event.Conn)
		}
		if event.Key != key {
			t.Errorf("Unexpected event key: %s", event.Key)
		}
		if event.Value != value {
			t.Errorf("Unexpected event value: %v", event.Value)
		}
	case <-time.After(time.Second):
		t.Fatalf("Failed to get watch event.")
	}
}

func TestBroadcastRequest_multi(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestBroadcastRequest_multi"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}
	defer watcher.Unregister()

	<-events // Skip an initial event.
	for i := 0; i < 10; i++ {
		val := fmt.Sprintf("%d", i)
		_, err := conn.Do(NewBroadcastRequest(key).Value(val)).Get()
		if err != nil {
			t.Fatalf("Failed to send a broadcast request: %s", err)
		}
		select {
		case event := <-events:
			if event.Conn != conn {
				t.Errorf("Unexpected event connection: %v", event.Conn)
			}
			if event.Key != key {
				t.Errorf("Unexpected event key: %s", event.Key)
			}
			if event.Value.(string) != val {
				t.Errorf("Unexpected event value: %v", event.Value)
			}
		case <-time.After(time.Second):
			t.Fatalf("Failed to get watch event %d", i)
		}
	}
}

func TestConnection_NewWatcher_multiOnKey(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnection_NewWatcher_multiOnKey"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	events := []chan WatchEvent{
		make(chan WatchEvent),
		make(chan WatchEvent),
	}
	for _, ch := range events {
		defer close(ch)
	}

	for _, ch := range events {
		channel := ch
		watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
			channel <- event
		})
		if err != nil {
			t.Fatalf("Failed to create a watch: %s", err)
		}
		defer watcher.Unregister()
	}

	for i, ch := range events {
		select {
		case <-ch: // Skip an initial event.
		case <-time.After(2 * time.Second):
			t.Fatalf("Failed to skip watch event for %d callback", i)
		}
	}

	_, err := conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	if err != nil {
		t.Fatalf("Failed to send a broadcast request: %s", err)
	}

	for i, ch := range events {
		select {
		case event := <-ch:
			if event.Conn != conn {
				t.Errorf("Unexpected event connection: %v", event.Conn)
			}
			if event.Key != key {
				t.Errorf("Unexpected event key: %s", event.Key)
			}
			if event.Value.(string) != value {
				t.Errorf("Unexpected event value: %v", event.Value)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Failed to get watch event from callback %d", i)
		}
	}
}

func TestWatcher_Unregister(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestWatcher_Unregister"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	events := make(chan WatchEvent)
	defer close(events)
	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}

	<-events
	watcher.Unregister()

	_, err = conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	if err != nil {
		t.Fatalf("Got broadcast error: %s", err)
	}

	select {
	case event := <-events:
		t.Fatalf("Get unexpected events: %v", event)
	case <-time.After(time.Second):
	}
}

func TestConnection_NewWatcher_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestConnection_NewWatcher_concurrent"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	errors := make(chan error, testConcurrency)
	for i := 0; i < testConcurrency; i++ {
		go func(i int) {
			defer wg.Done()

			events := make(chan struct{})

			watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
				close(events)
			})
			if err != nil {
				errors <- err
			} else {
				select {
				case <-events:
				case <-time.After(time.Second):
					errors <- fmt.Errorf("Unable to get an event %d", i)
				}
				watcher.Unregister()
			}
		}(i)
	}
	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("An error found: %s", err)
	}
}

func TestWatcher_Unregister_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestWatcher_Unregister_concurrent"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {})
	if err != nil {
		t.Fatalf("Failed to create a watch: %s", err)
	}

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	for i := 0; i < testConcurrency; i++ {
		go func() {
			defer wg.Done()
			watcher.Unregister()
		}()
	}
	wg.Wait()
}

func TestConnection_named_index_after_reconnect(t *testing.T) {
	const server = "127.0.0.1:3015"

	testDialer := dialer
	testDialer.Address = server

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       testDialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)
	if err != nil {
		t.Fatalf("Unable to start Tarantool: %s", err)
	}

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 10

	conn := test_helpers.ConnectWithValidation(t, testDialer, reconnectOpts)
	defer conn.Close()

	test_helpers.StopTarantool(inst)

	request := NewSelectRequest("test").Index("primary").Limit(1)
	_, err = conn.Do(request).Get()
	if err == nil {
		t.Fatalf("An error expected.")
	}

	if err := test_helpers.RestartTarantool(inst); err != nil {
		t.Fatalf("Unable to restart Tarantool: %s", err)
	}

	maxTime := reconnectOpts.Reconnect * time.Duration(reconnectOpts.MaxReconnects)
	timeout := time.After(maxTime)

	for {
		select {
		case <-timeout:
			t.Fatalf("Failed to execute request without an error, last error: %s", err)
		default:
		}

		_, err = conn.Do(request).Get()
		if err == nil {
			return
		}
	}
}

func TestConnect_schema_update(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	for i := 0; i < 100; i++ {
		fut := conn.Do(NewCallRequest("create_spaces"))

		ctx, cancel := test_helpers.GetConnectContext()
		defer cancel()
		if conn, err := Connect(ctx, dialer, opts); err != nil {
			if err.Error() != "concurrent schema update" {
				t.Errorf("unexpected error: %s", err)
			}
		} else if conn == nil {
			t.Errorf("conn is nil")
		} else {
			conn.Close()
		}

		if _, err := fut.Get(); err != nil {
			t.Errorf("Failed to call create_spaces: %s", err)
		}
	}
}

func TestConnect_context_cancel(t *testing.T) {
	var connLongReconnectOpts = Opts{
		Timeout:       5 * time.Second,
		Reconnect:     time.Second,
		MaxReconnects: 100,
	}

	ctx, cancel := context.WithCancel(context.Background())

	var conn *Connection
	var err error

	cancel()
	conn, err = Connect(ctx, dialer, connLongReconnectOpts)

	if conn != nil || err == nil {
		t.Fatalf("Connection was created after cancel")
	}
	if !strings.Contains(err.Error(), "operation was canceled") {
		t.Fatalf("Unexpected error, expected to contain %s, got %v",
			"operation was canceled", err)
	}
}

// A dialer that rejects the first few connection requests.
type mockSlowDialer struct {
	counter  *int
	original NetDialer
}

func (m mockSlowDialer) Dial(ctx context.Context, opts DialOpts) (Conn, error) {
	*m.counter++
	if *m.counter < 10 {
		return nil, fmt.Errorf("Too early: %v", *m.counter)
	}
	return m.original.Dial(ctx, opts)
}

func TestConnectIsBlocked(t *testing.T) {
	const server = "127.0.0.1:3015"
	testDialer := dialer
	testDialer.Address = server

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       testDialer,
		InitScript:   "config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)
	if err != nil {
		t.Fatalf("Unable to start Tarantool: %s", err)
	}

	var counter int
	mockDialer := mockSlowDialer{original: testDialer, counter: &counter}
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 100
	conn, err := Connect(ctx, mockDialer, reconnectOpts)
	assert.Nil(t, err)
	conn.Close()
	assert.GreaterOrEqual(t, counter, 10)
}

func TestConnectIsBlockedUntilContextExpires(t *testing.T) {
	const server = "127.0.0.1:3015"

	testDialer := dialer
	testDialer.Address = server

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 100
	_, err := Connect(ctx, testDialer, reconnectOpts)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "failed to dial: dial tcp 127.0.0.1:3015: i/o timeout")
}

func TestConnectIsUnblockedAfterMaxAttempts(t *testing.T) {
	const server = "127.0.0.1:3015"

	testDialer := dialer
	testDialer.Address = server

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 1
	_, err := Connect(ctx, testDialer, reconnectOpts)
	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "last reconnect failed")
}

func buildSidecar(dir string) error {
	goPath, err := exec.LookPath("go")
	if err != nil {
		return err
	}
	cmd := exec.Command(goPath, "build", "main.go")
	cmd.Dir = filepath.Join(dir, "testdata", "sidecar")
	return cmd.Run()
}

func TestFdDialer(t *testing.T) {
	isLess, err := test_helpers.IsTarantoolVersionLess(3, 0, 0)
	if err != nil || isLess {
		t.Skip("box.session.new present in Tarantool since version 3.0")
	}

	wd, err := os.Getwd()
	require.NoError(t, err)

	err = buildSidecar(wd)
	require.NoErrorf(t, err, "failed to build sidecar: %v", err)

	instOpts := startOpts
	instOpts.Listen = fdDialerTestServer
	instOpts.Dialer = NetDialer{
		Address:  fdDialerTestServer,
		User:     "test",
		Password: "test",
	}

	inst, err := test_helpers.StartTarantool(instOpts)
	require.NoError(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	sidecarExe := filepath.Join(wd, "testdata", "sidecar", "main")

	evalBody := fmt.Sprintf(`
		local socket = require('socket')
		local popen = require('popen')
		local os = require('os')
		local s1, s2 = socket.socketpair('AF_UNIX', 'SOCK_STREAM', 0)

		--[[ Tell sidecar which fd use to connect. --]]
		os.setenv('SOCKET_FD', tostring(s2:fd()))

		box.session.new({
			type = 'binary',
			fd = s1:fd(),
			user = 'test',
		})
		s1:detach()

		local ph, err = popen.new({'%s'}, {
			stdout = popen.opts.PIPE,
			stderr = popen.opts.PIPE,
			inherit_fds = {s2:fd()},
		})

		if err ~= nil then
			return 1, err
		end

		ph:wait()

		local status_code = ph:info().status.exit_code
		local stderr = ph:read({stderr=true}):rstrip()
		local stdout = ph:read({stdout=true}):rstrip()
		return status_code, stderr, stdout
	`, sidecarExe)

	var resp []interface{}
	err = conn.EvalTyped(evalBody, []interface{}{}, &resp)
	require.NoError(t, err)
	require.Equal(t, "", resp[1], resp[1])
	require.Equal(t, "", resp[2], resp[2])
	require.Equal(t, int8(0), resp[0])
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
		log.Fatalf("Could not check the Tarantool version: %s", err)
	}

	startOpts.MemtxUseMvccEngine = !isStreamUnsupported

	inst, err := test_helpers.StartTarantool(startOpts)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
