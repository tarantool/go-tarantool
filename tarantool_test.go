package tarantool_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
	. "github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
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

var contextDoneErrRegexp = regexp.MustCompile(
	`^context is done \(request ID [0-9]+\): context canceled$`)

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
	// Concurrency: 32,
	// RateLimit: 4*1024,
}

const N = 500

func BenchmarkSync_naive(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err = conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	b.ResetTimer()

	for b.Loop() {
		req := NewSelectRequest(spaceNo).
			Index(indexNo).
			Iterator(IterEq).
			Key([]any{uint(1111)})
		data, err := conn.Do(req).Get()
		if err != nil {
			b.Errorf("request error: %s", err)
		}

		tuple := data[0].([]any)
		if tuple[0].(uint16) != uint16(1111) {
			b.Errorf("invalid result")
		}
	}
}

func BenchmarkSync_naive_with_single_request(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err = conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	b.ResetTimer()

	for b.Loop() {
		data, err := conn.Do(req).Get()
		if err != nil {
			b.Errorf("request error: %s", err)
		}

		tuple := data[0].([]any)
		if tuple[0].(uint16) != uint16(1111) {
			b.Errorf("invalid result")
		}
	}
}

type benchTuple struct {
	id uint
}

func (t *benchTuple) DecodeMsgpack(dec *msgpack.Decoder) error {
	l, err := dec.DecodeArrayLen()
	if err != nil {
		return fmt.Errorf("failed to decode tuples array: %w", err)
	}

	if l != 1 {
		return fmt.Errorf("unexpected tuples array with len %d", l)
	}

	l, err = dec.DecodeArrayLen()
	if err != nil {
		return fmt.Errorf("failed to decode tuple array: %w", err)
	}

	if l < 1 {
		return fmt.Errorf("too small tuple have 0 fields")
	}

	t.id, err = dec.DecodeUint()
	if err != nil {
		return fmt.Errorf("failed to decode id: %w", err)
	}

	return nil
}

func BenchmarkSync_naive_with_custom_type_without_Release(b *testing.B) {
	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	var tuple benchTuple

	b.ResetTimer()

	for b.Loop() {
		err := conn.Do(req).GetTyped(&tuple)
		if err != nil {
			b.Errorf("request error: %s", err)
		}

		if tuple.id != 1111 {
			b.Errorf("invalid result")
		}
	}
}

func BenchmarkSync_naive_with_custom_type_with_Release(b *testing.B) {
	releasedOpts := opts
	allocator, err := tarantool.NewPoolAllocator([]int{8, 12, 16})
	if err != nil {
		b.Fatalf("failed to create pool allocator: %s", err)
	}

	releasedOpts.Allocator = allocator
	conn := test_helpers.ConnectWithValidation(b, dialer, releasedOpts)
	defer func() { _ = conn.Close() }()

	_, err = conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	var tuple benchTuple

	b.ResetTimer()

	for b.Loop() {
		fut := conn.Do(req)
		if err := fut.GetTyped(&tuple); err != nil {
			b.Errorf("request error: %s", err)
		}

		if tuple.id != 1111 {
			b.Errorf("invalid result")
		}
		fut.Release()
	}
}

func BenchmarkSync_execute_with_Release(b *testing.B) {
	test_helpers.SkipIfSQLUnsupported(b)

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	var mem []Member

	req := NewExecuteRequest(selectTypedQuery).Args(
		[]any{1},
	)

	b.ResetTimer()

	for b.Loop() {
		fut := conn.Do(req)
		if err := fut.GetTyped(&mem); err != nil {
			b.Errorf("request error: %s", err)
		}

		if len(mem) != 1 || mem[0].Name != "test" {
			b.Errorf("invalid result, got: %v", mem)
		}
		fut.Release()
	}
}

func BenchmarkSync_multithread(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err = conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		var tuple benchTuple

		for pb.Next() {
			err := conn.Do(req).GetTyped(&tuple)
			if err != nil {
				b.Errorf("request error: %s", err)
			}

			if tuple.id != 1111 {
				b.Errorf("invalid result")
			}
		}
	})
}

func BenchmarkAsync_multithread_parallelism(b *testing.B) {
	var err error

	conn := test_helpers.ConnectWithValidation(b, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err = conn.Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		b.Fatalf("failed to initialize database: %s", err)
	}

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	b.ResetTimer()

	for p := 1; p <= 1024; p *= 2 {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			b.SetParallelism(p)
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				var tuple benchTuple

				for pb.Next() {
					err := conn.Do(req).GetTyped(&tuple)
					if err != nil {
						b.Errorf("request error: %s", err)
					}

					if tuple.id != 1111 {
						b.Errorf("invalid result")
					}
				}
			})
		})
	}
}

// TestBenchmarkAsync is a benchmark for the async API that is unable to
// implement with a Go-benchmark. It can be used to test performance with
// different numbers of connections and processing goroutines.
func TestBenchmarkAsync(t *testing.T) {
	t.Skip()

	requests := int64(10_000_000)
	connections := 16

	ops := opts
	allocator, err := tarantool.NewPoolAllocator([]int{8, 12, 16})
	require.NoError(t, err)

	ops.Allocator = allocator
	// ops.Concurrency = 2 // 4 max. // 2 max.

	conns := make([]*Connection, 0, connections)
	for range connections {
		conn := test_helpers.ConnectWithValidation(t, dialer, ops)
		defer func() { _ = conn.Close() }()

		conns = append(conns, conn)
	}

	_, err = conns[0].Do(
		NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	require.NoError(t, err, "failed to initialize database")

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Iterator(IterEq).
		Key(UintKey{I: 1111})

	maxRps := float64(0)
	maxConnections := 0
	maxConcurrency := 0

	for cn := 1; cn <= connections; cn *= 2 {
		for cc := 1; cc <= 512; cc *= 2 {
			var wg sync.WaitGroup

			curRequests := requests

			start := time.Now()

			for i := range cc {
				wg.Add(1)

				ch := make(chan Future, 1024)

				go func(i int) {
					defer close(ch)

					for atomic.AddInt64(&curRequests, -1) >= 0 {
						ch <- conns[i%cn].Do(req)
					}
				}(i)

				go func() {
					defer wg.Done()

					var tuple benchTuple

					for fut := range ch {
						err := fut.GetTyped(&tuple)
						assert.NoError(t, err, "request error")

						assert.Equal(t, uint(1111), tuple.id, "invalid result")
						fut.Release()
					}
				}()
			}

			wg.Wait()

			duration := time.Since(start)

			rps := float64(requests) / duration.Seconds()
			t.Log("requests   :", requests)
			t.Log("concurrency:", cc)
			t.Log("connections:", cn)
			t.Logf("duration   : %.2f\n", duration.Seconds())
			t.Logf("requests/s : %.2f\n", rps)
			t.Log("============")

			if maxRps < rps {
				maxRps = rps
				maxConnections = cn
				maxConcurrency = cc
			}
		}
	}

	t.Log("max connections:", maxConnections)
	t.Log("max concurrency:", maxConcurrency)
	t.Logf("max requests/s : %.2f\n", maxRps)
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
	require.NoError(err)
	require.NotNil(conn)
	defer func() { _ = conn.Close() }()

	assert.Equal(server, conn.Addr().String())
	assert.NotEmpty(conn.Greeting().Version)

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
	require.NoError(err)
	require.NoError(conn.Flush())

	// Read IPROTO_PING response length.
	lenbuf := make([]byte, 5)
	ret, err = io.ReadFull(conn, lenbuf)
	require.NoError(err)
	require.Equal(len(lenbuf), ret)
	length := int(binary.BigEndian.Uint32(lenbuf[1:]))
	require.Positive(length)

	// Read IPROTO_PING response.
	buf := make([]byte, length)
	ret, err = io.ReadFull(conn, buf)
	require.NoError(err)
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
	require.Error(t, err)
	require.ErrorContains(t, err, "failed to authenticate")
	if conn != nil {
		_ = conn.Close()
		assert.Fail(t, "connection is not nil")
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
		_ = conn.Close()
		require.Fail(t, "Connection created successfully")
	}

	assert.ErrorContains(t, err, "failed to authenticate")
}

func TestFutureMultipleGetGetTyped(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	fut := conn.Do(NewCallRequest("simple_concat").Args([]any{"1"}))

	for i := range 30 {
		// [0, 10) fut.Get()
		// [10, 20) fut.GetTyped()
		// [20, 30) Mix
		get := false
		if (i < 10) || (i >= 20 && i%2 == 0) {
			get = true
		}

		if get {
			data, err := fut.Get()
			require.NoError(t, err, "Failed to call Get()")
			assert.Equal(t, "11", data[0], "Wrong Get() result")
		} else {
			tpl := struct {
				Val string
			}{}
			err := fut.GetTyped(&tpl)
			require.NoError(t, err, "Failed to call GetTyped()")
			assert.Equal(t, "11", tpl.Val, "Wrong GetTyped() result")
		}
	}
}

func TestFutureMultipleGetWithError(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	fut := conn.Do(NewCallRequest("non_exist").Args([]any{"1"}))

	for range 2 {
		_, err := fut.Get()
		require.Error(t, err, "An error expected")
	}
}

func TestFutureMultipleGetTypedWithError(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	fut := conn.Do(NewCallRequest("simple_concat").Args([]any{"1"}))

	wrongTpl := struct {
		Val int
	}{}
	goodTpl := struct {
		Val string
	}{}

	require.Error(t, fut.GetTyped(&wrongTpl), "An error expected")
	require.NoError(t, fut.GetTyped(&goodTpl), "Unexpected error")
	require.Equal(t, "11", goodTpl.Val, "Wrong result")
}

// /////////////////

func TestClient(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	var (
		req Request
		err error
	)

	// Ping
	req = NewPingRequest()
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to Ping")
	require.Nil(t, data, "Response data is not nil after Ping")

	// Insert
	req = NewInsertRequest(spaceNo).Tuple([]any{uint(1), "hello", "world"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Insert")
	assert.Len(t, data, 1, "Response Body len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Insert")
	} else {
		assert.Len(t, tpl, 3, "Unexpected body of Insert (tuple len)")
		assert.EqualValues(t, 1, tpl[0], "Unexpected body of Insert (0)")
		assert.Equal(t, "hello", tpl[1], "Unexpected body of Insert (1)")
	}
	req = NewInsertRequest(spaceNo).Tuple(&Tuple{Id: 1, Msg: "hello", Name: "world"})
	data, err = conn.Do(req).Get()
	tntErr, ok := err.(Error)
	require.True(t, ok, "Expected Error type")
	assert.Equal(t, iproto.ER_TUPLE_FOUND, tntErr.Code,
		"Expected %s but got: %v", iproto.ER_TUPLE_FOUND, err)
	assert.Empty(t, data, "Response Body len != 0")

	// Delete
	req = NewDeleteRequest(spaceNo).Index(indexNo).Key([]any{uint(1)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Delete")
	assert.Len(t, data, 1, "Response Body len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Delete")
	} else {
		assert.Len(t, tpl, 3, "Unexpected body of Delete (tuple len)")
		assert.EqualValues(t, 1, tpl[0], "Unexpected body of Delete (0)")
		assert.Equal(t, "hello", tpl[1], "Unexpected body of Delete (1)")
	}
	req = NewDeleteRequest(spaceNo).Index(indexNo).Key([]any{uint(101)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Delete")
	assert.Empty(t, data, "Response Data len != 0")

	// Replace
	req = NewReplaceRequest(spaceNo).Tuple([]any{uint(2), "hello", "world"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Replace")
	require.NotNil(t, data, "Response is nil after Replace")
	req = NewReplaceRequest(spaceNo).Tuple([]any{uint(2), "hi", "planet"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Replace (duplicate)")
	assert.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Replace")
	} else {
		assert.Len(t, tpl, 3, "Unexpected body of Replace (tuple len)")
		assert.EqualValues(t, 2, tpl[0], "Unexpected body of Replace (0)")
		assert.Equal(t, "hi", tpl[1], "Unexpected body of Replace (1)")
	}

	// Update
	req = NewUpdateRequest(spaceNo).
		Index(indexNo).
		Key([]any{uint(2)}).
		Operations(NewOperations().Assign(1, "bye").Delete(2, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Update")
	assert.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Update")
	} else {
		assert.Len(t, tpl, 2, "Unexpected body of Update (tuple len)")
		assert.EqualValues(t, 2, tpl[0], "Unexpected body of Update (0)")
		assert.Equal(t, "bye", tpl[1], "Unexpected body of Update (1)")
	}

	// Upsert
	req = NewUpsertRequest(spaceNo).
		Tuple([]any{uint(3), 1}).
		Operations(NewOperations().Add(1, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (insert)")
	require.NotNil(t, data, "Response is nil after Upsert (insert)")
	req = NewUpsertRequest(spaceNo).
		Tuple([]any{uint(3), 1}).
		Operations(NewOperations().Add(1, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (update)")
	assert.NotNil(t, data, "Response is nil after Upsert (update)")

	// Select
	for i := 10; i < 20; i++ {
		req = NewReplaceRequest(spaceNo).
			Tuple([]any{uint(i), fmt.Sprintf("val %d", i), "bla"})
		data, err = conn.Do(req).Get()
		require.NoError(t, err, "Failed to Replace")
		assert.NotNil(t, data, "Response is nil after Replace")
	}
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(10)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Select")
	require.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Select")
	} else {
		assert.EqualValues(t, 10, tpl[0], "Unexpected body of Select (0)")
		assert.Equal(t, "val 10", tpl[1], "Unexpected body of Select (1)")
	}

	// Select empty
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(30)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Select")
	assert.Empty(t, data, "Response Data len != 0")

	// Select Typed
	var tpl []Tuple
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(10)})
	err = conn.Do(req).GetTyped(&tpl)
	require.NoError(t, err, "Failed to SelectTyped")
	assert.Len(t, tpl, 1, "Result len of SelectTyped != 1")
	assert.Equal(t, uint(10), tpl[0].Id, "Bad value loaded from SelectTyped")

	// Select Typed for one tuple
	var tpl1 [1]Tuple
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(10)})
	err = conn.Do(req).GetTyped(&tpl1)
	require.NoError(t, err, "Failed to SelectTyped")
	assert.Len(t, tpl1, 1, "Result len of SelectTyped != 1")
	assert.Equal(t, uint(10), tpl1[0].Id, "Bad value loaded from SelectTyped")

	// Get Typed Empty
	var singleTpl2 Tuple
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(30)})
	err = conn.Do(req).GetTyped(&singleTpl2)
	require.NoError(t, err, "Failed to GetTyped")
	assert.Equal(t, uint(0), singleTpl2.Id, "Bad value loaded from GetTyped")

	// Select Typed Empty
	var tpl2 []Tuple
	req = NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(30)})
	err = conn.Do(req).GetTyped(&tpl2)
	require.NoError(t, err, "Failed to SelectTyped")
	assert.Empty(t, tpl2, "Result len of SelectTyped != 1")

	// Call
	req = NewCallRequest("box.info").Args([]any{"box.schema.SPACE_ID"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Call")
	assert.GreaterOrEqual(t, len(data), 1, "Response.Data is empty after Eval")

	req = NewCallRequest("simple_concat").Args([]any{"1"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to use Call")
	assert.Equal(t, "11", data[0], "result is not {{1}}")

	// Eval
	req = NewEvalRequest("return 5 + 6").Args([]any{})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Eval")
	assert.GreaterOrEqual(t, len(data), 1, "Response.Data is empty after Eval")
	assert.EqualValues(t, 11, data[0], "5 + 6 == 11, but got")
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
		Args     any
		sqlInfo  SQLInfo
		data     []any
		metaData []ColumnMetaData
	}

	selectSpanDifQuery := selectSpanDifQueryNew
	isSeqScanOld, err := test_helpers.IsTarantoolVersionLess(3, 0, 0)
	require.NoError(t, err, "Could not check the Tarantool version")
	if isSeqScanOld {
		selectSpanDifQuery = selectSpanDifQueryOld
	}

	testCases := []testCase{
		{
			createTableQuery,
			[]any{},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			insertQuery,
			[]any{"1", "test"},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			selectNamedQuery,
			map[string]any{
				"ID":   "1",
				"NAME": "test",
			},
			SQLInfo{AffectedCount: 0},
			[]any{[]any{"1", "test"}},
			[]ColumnMetaData{
				{FieldType: "string", FieldName: "ID"},
				{FieldType: "string", FieldName: "NAME"}},
		},
		{
			selectPosQuery,
			[]any{"1", "test"},
			SQLInfo{AffectedCount: 0},
			[]any{[]any{"1", "test"}},
			[]ColumnMetaData{
				{FieldType: "string", FieldName: "ID"},
				{FieldType: "string", FieldName: "NAME"}},
		},
		{
			updateQuery,
			[]any{"test_test", "1"},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			enableFullMetaDataQuery,
			[]any{},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			selectSpanDifQuery,
			[]any{"test_test"},
			SQLInfo{AffectedCount: 0},
			[]any{[]any{"11", "test_test", "1"}},
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
			[]any{},
			SQLInfo{AffectedCount: 0},
			[]any{},
			nil,
		},
		{
			insertIncrQuery,
			[]any{"2", "test_2"},
			SQLInfo{AffectedCount: 1, InfoAutoincrementIds: []uint64{1}},
			[]any{},
			nil,
		},
		{
			deleteQuery,
			[]any{"test_2"},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			dropQuery,
			[]any{},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
		{
			disableFullMetaDataQuery,
			[]any{},
			SQLInfo{AffectedCount: 1},
			[]any{},
			nil,
		},
	}

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	for i, test := range testCases {
		req := NewExecuteRequest(test.Query).Args(test.Args)
		resp, err := conn.Do(req).GetResponse()
		require.NoError(t, err, "Failed to Execute, query: %s", test.Query)
		assert.NotNil(t, resp, "Response is nil after Execute\nQuery number: %d", i)
		data, err := resp.Decode()
		require.NoError(t, err, "Failed to Decode")
		for j := range data {
			assert.Equal(t, data[j], test.data[j], "Response data is wrong")
		}
		exResp, ok := resp.(*ExecuteResponse)
		assert.True(t, ok, "Got wrong response type")
		sqlInfo, err := exResp.SQLInfo()
		require.NoError(t, err, "Error while getting SQLInfo")
		assert.Equal(t, sqlInfo.AffectedCount, test.sqlInfo.AffectedCount,
			"Affected count is wrong")

		errorMsg := "Response Metadata is wrong"
		metaData, err := exResp.MetaData()
		require.NoError(t, err, "Error while getting MetaData")
		for j := range metaData {
			assert.Equal(t, metaData[j], test.metaData[j], errorMsg)
		}
	}
}

func TestSQLTyped(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	mem := []Member{}
	fut := conn.Do(NewExecuteRequest(selectTypedQuery).
		Args([]any{1}),
	)
	err := fut.GetTyped(&mem)
	require.NoError(t, err, "Error while GetTyped")
	resp, err := fut.GetResponse()
	require.NoError(t, err, "Error while getting Response")
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")

	info, err := exResp.SQLInfo()
	require.NoError(t, err, "Error while getting SQLInfo")
	meta, err := exResp.MetaData()
	require.NoError(t, err, "Error while getting MetaData")
	assert.Equal(t, uint64(0), info.AffectedCount, "Rows affected count must be 0")
	assert.Len(t, meta, 2, "Meta data is not full")
	assert.Len(t, mem, 1, "Wrong length of result")
	assert.NoError(t, err)
}

func TestSQLBindings(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	// Data for test table
	testData := map[int]string{
		1: "test",
	}

	var resp Response

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	// test all types of supported bindings
	// prepare named sql bind
	sqlBind := map[string]any{
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

	sqlBind4 := []any{
		KeyValueBind{Key: "id", Value: 1},
		KeyValueBind{Key: "name", Value: "test"},
	}

	namedSQLBinds := []any{
		sqlBind,
		sqlBind2,
		sqlBind3,
		sqlBind4,
	}

	// positioned sql bind
	sqlBind5 := []any{
		1, "test",
	}

	// mixed sql bind
	sqlBind6 := []any{
		KeyValueBind{Key: "name0", Value: 1},
		"test",
	}

	for _, bind := range namedSQLBinds {
		req := NewExecuteRequest(selectNamedQuery2).Args(bind)
		resp, err := conn.Do(req).GetResponse()
		require.NoError(t, err, "Failed to Execute")
		require.NotNil(t, resp, "Response is nil after Execute")
		data, err := resp.Decode()
		require.NoError(t, err, "Failed to Decode")
		assert.NotEqual(t, []any{1, testData[1]}, data[0],
			"Select with named arguments failed")
		exResp, ok := resp.(*ExecuteResponse)
		assert.True(t, ok, "Got wrong response type")
		metaData, err := exResp.MetaData()
		require.NoError(t, err, "Error while getting MetaData")
		assert.Equal(t, "unsigned", metaData[0].FieldType, "Wrong metadata")
		assert.Equal(t, "NAME0", metaData[0].FieldName, "Wrong metadata")
		assert.Equal(t, "string", metaData[1].FieldType, "Wrong metadata")
		assert.Equal(t, "NAME1", metaData[1].FieldName, "Wrong metadata")
	}

	req := NewExecuteRequest(selectPosQuery2).Args(sqlBind5)
	resp, err := conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	data, err := resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	assert.NotEqual(t, []any{1, testData[1]}, data[0],
		"Select with positioned arguments failed")
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err := exResp.MetaData()
	require.NoError(t, err, "Error while getting MetaData")
	assert.Equal(t, "unsigned", metaData[0].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME0", metaData[0].FieldName, "Wrong metadata")
	assert.Equal(t, "string", metaData[1].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME1", metaData[1].FieldName, "Wrong metadata")

	req = NewExecuteRequest(mixedQuery).Args(sqlBind6)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	data, err = resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	assert.NotEqual(t, []any{1, testData[1]}, data[0], "Select with mixed arguments failed")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err = exResp.MetaData()
	require.NoError(t, err, "Error while getting MetaData")
	assert.Equal(t, "unsigned", metaData[0].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME0", metaData[0].FieldName, "Wrong metadata")
	assert.Equal(t, "string", metaData[1].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME1", metaData[1].FieldName, "Wrong metadata")
}

func TestStressSQL(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	req := NewExecuteRequest(createTableQuery)
	resp, err := conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to create an Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.NoError(t, err, "Error while getting SQLInfo")
	assert.Equal(t, uint64(1), sqlInfo.AffectedCount, "Incorrect count of created spaces")

	// create table with the same name
	req = NewExecuteRequest(createTableQuery)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to create an Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	_, err = resp.Decode()
	require.Error(t, err, "Expected error while decoding")

	tntErr, ok := err.(Error)
	assert.True(t, ok)
	assert.Equal(t, iproto.ER_SPACE_EXISTS, tntErr.Code)
	require.Equal(t, iproto.ER_SPACE_EXISTS, resp.Header().Error, "Unexpected response error")
	prevErr := err

	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	assert.Equal(t, prevErr, err)
	assert.Equal(t, uint64(0), sqlInfo.AffectedCount, "Incorrect count of created spaces")

	// execute with nil argument
	req = NewExecuteRequest(createTableQuery).Args(nil)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to create an Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	require.NotEqual(t, ErrorNo, resp.Header().Error, "Unexpected successful Execute")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Error(t, err, "Expected an error")
	assert.Equal(t, uint64(0), sqlInfo.AffectedCount, "Incorrect count of created spaces")

	// execute with zero string
	req = NewExecuteRequest("")
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to create an Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	require.NotEqual(t, ErrorNo, resp.Header().Error, "Unexpected successful Execute")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Error(t, err, "Expected an error")
	assert.Equal(t, uint64(0), sqlInfo.AffectedCount, "Incorrect count of created spaces")

	// drop table query
	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.NoError(t, err, "Error while getting SQLInfo")
	assert.Equal(t, uint64(1), sqlInfo.AffectedCount, "Incorrect count of dropped spaces")

	// drop the same table
	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to create an Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	require.NotEqual(t, ErrorNo, resp.Header().Error, "Unexpected successful Execute")
	_, err = resp.Decode()
	require.Error(t, err, "Unexpected lack of error")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Error(t, err, "Unexpected lack of error")
	assert.Equal(t, uint64(0), sqlInfo.AffectedCount, "Incorrect count of created spaces")
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stmt, err := conn.NewPrepared(selectNamedQuery2)
	require.NoError(t, err, "failed to prepare")

	executeReq := NewExecutePreparedRequest(stmt)
	unprepareReq := NewUnprepareRequest(stmt)

	resp, err := conn.Do(executeReq.Args([]any{1, "test"})).GetResponse()
	require.NoError(t, err, "failed to execute prepared")
	data, err := resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	assert.NotEqual(t, []any{1, "test"}, data[0], "Select with named arguments failed")
	prepResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	metaData, err := prepResp.MetaData()
	require.NoError(t, err, "Error while getting MetaData")
	assert.Equal(t, "unsigned", metaData[0].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME0", metaData[0].FieldName, "Wrong metadata")
	assert.Equal(t, "string", metaData[1].FieldType, "Wrong metadata")
	assert.Equal(t, "NAME1", metaData[1].FieldName, "Wrong metadata")

	_, err = conn.Do(unprepareReq).Get()
	require.NoError(t, err, "failed to unprepare prepared statement")

	_, err = conn.Do(unprepareReq).Get()
	require.Error(t, err, "the statement must be already unprepared")
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = conn.Do(executeReq).Get()
	require.Error(t, err, "the statement must be already unprepared")
	require.Contains(t, err.Error(), "Prepared statement with id")

	prepareReq := NewPrepareRequest(selectNamedQuery2)
	data, err = conn.Do(prepareReq).Get()
	require.NoError(t, err, "failed to prepare")
	require.NotNil(t, data, "failed to prepare: Data is nil")
	require.NotEmpty(t, data, "failed to prepare: response Data has no elements")
	stmt, ok = data[0].(*Prepared)
	require.True(t, ok, "failed to prepare: failed to cast the response Data to Prepared object")
	require.NotZero(t, stmt.StatementID, "failed to prepare: statement id is 0")
}

func TestExecutePrepareResponseRelease(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)

	stmt, err := conn.NewPrepared(selectNamedQuery2)
	require.NoError(t, err, "failed to prepare")

	req := NewExecutePreparedRequest(stmt)

	resp, err := conn.Do(req.Args([]any{1, "test"})).GetResponse()
	require.NoError(t, err, "failed to execute prepared")

	resp.Release()

	exPrepResp, ok := resp.(*ExecuteResponse)
	require.True(t, ok, "got wrong response type")
	require.Equal(t, ExecuteResponse{}, *exPrepResp)
}

func TestConnection_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to the current" +
		" connection or connection pool")

	conn1 := &Connection{}
	req := test_helpers.NewMockRequest()

	_, err := conn1.Do(req).Get()
	require.Error(t, err, "nil error caught")
	require.Equal(t, expectedErr.Error(), err.Error(), "Unexpected error caught")
}

func TestConnection_SetResponse_failed(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	req := mockRequest{conn}
	fut := conn.Do(&req)

	data, err := fut.Get()
	require.EqualError(t, err, "failed to set response: some error")
	assert.Nil(t, data)
}

func TestGetSchema(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	s, err := GetSchema(conn)
	require.NoError(t, err, "unexpected error")
	assert.Equal(t, spaceNo, s.Spaces[spaceName].Id, "GetSchema() returns incorrect schema")
}

func TestConnection_SetSchema_Changes(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	req := NewInsertRequest(spaceName).Tuple([]any{uint(1010), "Tarantool"})
	_, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to Insert")

	s, err := GetSchema(conn)
	require.NoError(t, err, "unexpected error")
	conn.SetSchema(s)

	// Check if changes of the SetSchema result will do nothing to the
	// connection schema.
	s.Spaces[spaceName] = Space{}

	reqS := NewSelectRequest(spaceName).Key([]any{uint(1010)})
	data, err := conn.Do(reqS).Get()
	require.NoError(t, err, "failed to Select")
	assert.Equal(t, "Tarantool", data[0].([]any)[1], "wrong Select body")
}

func TestSchema(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	// Schema
	schema, err := GetSchema(conn)
	require.NoError(t, err, "unexpected error")
	assert.NotNil(t, schema.SpacesById, "schema.SpacesById is nil")
	assert.NotNil(t, schema.Spaces, "schema.Spaces is nil")
	var space, space2 Space
	var ok bool
	space, ok = schema.SpacesById[616]
	require.True(t, ok, "space with id = 616 was not found in schema.SpacesById")
	space2, ok = schema.Spaces["schematest"]
	require.True(t, ok, "space with name 'schematest' was not found in schema.SpacesById")
	assert.Equal(t, space, space2,
		"space with id = 616 and space with name schematest are different")
	assert.Equal(t, uint32(616), space.Id, "space 616 has incorrect Id")
	assert.Equal(t, "schematest", space.Name, "space 616 has incorrect Name")
	assert.True(t, space.Temporary, "space 616 should be temporary")
	assert.Equal(t, "memtx", space.Engine, "space 616 engine should be memtx")
	assert.Equal(t, uint32(8), space.FieldsCount, "space 616 has incorrect fields count")

	assert.NotNil(t, space.FieldsById, "space.FieldsById is nill")
	assert.NotNil(t, space.Fields, "space.Fields is nill")
	assert.Len(t, space.FieldsById, 7, "space.FieldsById len is incorrect")
	assert.Len(t, space.Fields, 7, "space.Fields len is incorrect")

	var field1, field2, field5, field1n, field5n Field
	field1, ok = space.FieldsById[1]
	require.True(t, ok, "field id = 1 was not found")
	field2, ok = space.FieldsById[2]
	require.True(t, ok, "field id = 2 was not found")
	field5, ok = space.FieldsById[5]
	require.True(t, ok, "field id = 5 was not found")

	field1n, ok = space.Fields["name1"]
	require.True(t, ok, "field name = name1 was not found")
	field5n, ok = space.Fields["name5"]
	require.True(t, ok, "field name = name5 was not found")
	assert.Equal(t, field1n, field1, "field with id = 1 and field with name 'name1' are different")
	assert.Equal(t, field5n, field5, "field with id = 5 and field with name 'name5' are different")
	assert.Equal(t, "name1", field1.Name, "field 1 has incorrect Name")
	assert.Equal(t, "unsigned", field1.Type, "field 1 has incorrect Type")
	assert.Equal(t, "name2", field2.Name, "field 2 has incorrect Name")
	assert.Equal(t, "string", field2.Type, "field 2 has incorrect Type")

	assert.NotNil(t, space.IndexesById, "space.IndexesById is nill")
	assert.NotNil(t, space.Indexes, "space.Indexes is nill")
	assert.Len(t, space.IndexesById, 2, "space.IndexesById len is incorrect")
	assert.Len(t, space.Indexes, 2, "space.Indexes len is incorrect")

	var index0, index3, index0n, index3n Index
	index0, ok = space.IndexesById[0]
	require.True(t, ok, "index id = 0 was not found")
	index3, ok = space.IndexesById[3]
	require.True(t, ok, "index id = 3 was not found")
	index0n, ok = space.Indexes["primary"]
	require.True(t, ok, "index name = primary was not found")
	index3n, ok = space.Indexes["secondary"]
	require.True(t, ok, "index name = secondary was not found")
	assert.Equal(t, index0, index0n,
		"index with id = 0 and index with name 'primary' are different")
	assert.Equal(t, index3, index3n,
		"index with id = 3 and index with name 'secondary' are different")
	assert.Equal(t, uint32(3), index3.Id, "index has incorrect Id")
	assert.Equal(t, "primary", index0.Name, "index has incorrect Name")
	assert.Equal(t, "hash", index0.Type, "index has incorrect Type")
	assert.Equal(t, "tree", index3.Type, "index has incorrect Type")
	assert.True(t, index0.Unique, "index has incorrect Unique")
	assert.False(t, index3.Unique, "index has incorrect Unique")
	assert.NotNil(t, index3.Fields, "index.Fields is nil")
	assert.Len(t, index3.Fields, 2, "index.Fields len is incorrect")

	ifield1 := index3.Fields[0]
	ifield2 := index3.Fields[1]
	require.NotEmpty(t, ifield1, "index field is nil")
	require.NotEmpty(t, ifield2, "index field is nil")
	assert.Equal(t, uint32(1), ifield1.Id, "index field has incorrect Id")
	assert.Equal(t, uint32(2), ifield2.Id, "index field has incorrect Id")
	assert.Contains(t, []string{"num", "unsigned"}, ifield1.Type, "index field has incorrect Type")
	assert.Contains(t, []string{"STR", "string"}, ifield2.Type, "index field has incorrect Type")
}

func TestSchema_IsNullable(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	schema, err := GetSchema(conn)
	require.NoError(t, err, "unexpected error")
	assert.NotNil(t, schema.Spaces, "schema.Spaces is nil")

	var space Space
	var ok bool
	space, ok = schema.SpacesById[616]
	require.True(t, ok, "space with id = 616 was not found in schema.SpacesById")

	var field, field_nullable Field
	for i := 0; i <= 5; i++ {
		name := fmt.Sprintf("name%d", i)
		field, ok = space.Fields[name]
		require.Truef(t, ok, "field name = %s was not found", name)
		assert.Falsef(t, field.IsNullable, "field %s has incorrect IsNullable", name)
	}
	field_nullable, ok = space.Fields["nullable"]
	require.True(t, ok, "field name = nullable was not found")
	assert.True(t, field_nullable.IsNullable, "field nullable has incorrect IsNullable")
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
		{"ErrWrongDataFormat", test_helpers.NewMockResponse(t, []any{}),
			ErrWrongDataFormat},
		{"ErrWrongDataFormat", test_helpers.NewMockResponse(t, []any{"test"}),
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
	defer func() { _ = conn.Close() }()

	var (
		req  Request
		data []any
		err  error
	)

	// Insert
	req = NewInsertRequest(spaceName).Tuple([]any{uint(1001), "hello2", "world2"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Insert")
	require.NotNil(t, data, "Response is nil after Insert")

	// Delete
	req = NewDeleteRequest(spaceName).Index(indexName).Key([]any{uint(1001)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Delete")
	require.NotNil(t, data, "Response is nil after Delete")

	// Replace
	req = NewReplaceRequest(spaceName).Tuple([]any{uint(1002), "hello", "world"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Replace")
	require.NotNil(t, data, "Response is nil after Replace")

	// Update
	req = NewUpdateRequest(spaceName).
		Index(indexName).
		Key([]any{uint(1002)}).
		Operations(NewOperations().Assign(1, "buy").Delete(2, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Update")
	require.NotNil(t, data, "Response is nil after Update")

	// Upsert
	req = NewUpsertRequest(spaceName).
		Tuple([]any{uint(1003), 1}).
		Operations(NewOperations().Add(1, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (insert)")
	require.NotNil(t, data, "Response is nil after Upsert (insert)")

	req = NewUpsertRequest(spaceName).
		Tuple([]any{uint(1003), 1}).
		Operations(NewOperations().Add(1, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (update)")
	require.NotNil(t, data, "Response is nil after Upsert (update)")

	// Select
	for i := 1010; i < 1020; i++ {
		req = NewReplaceRequest(spaceName).
			Tuple([]any{uint(i), fmt.Sprintf("val %d", i), "bla"})
		data, err = conn.Do(req).Get()
		require.NoError(t, err, "Failed to Replace")
		require.NotNil(t, data, "Response is nil after Replace")
	}
	req = NewSelectRequest(spaceName).
		Index(indexName).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(1010)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Select")
	require.NotNil(t, data, "Response is nil after Select")

	// Select Typed
	var tpl []Tuple
	req = NewSelectRequest(spaceName).
		Index(indexName).
		Offset(0).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(1010)})
	err = conn.Do(req).GetTyped(&tpl)
	require.NoError(t, err, "Failed to SelectTyped")
	assert.Len(t, tpl, 1, "Result len of SelectTyped != 1")
}

func TestClientRequestObjects(t *testing.T) {
	var (
		req Request
		err error
	)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	// Ping
	req = NewPingRequest()
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to Ping")
	assert.Empty(t, data, "Response Body len != 0")

	// The code prepares data.
	for i := 1010; i < 1020; i++ {
		conn.Do(NewDeleteRequest(spaceName).Index(nil).Key([]any{uint(i)}))
	}

	// Insert
	for i := 1010; i < 1020; i++ {
		req = NewInsertRequest(spaceName).
			Tuple([]any{uint(i), fmt.Sprintf("val %d", i), "bla"})
		data, err = conn.Do(req).Get()
		require.NoError(t, err, "Failed to Insert")
		require.Len(t, data, 1, "Response Body len != 1")
		if tpl, ok := data[0].([]any); !ok {
			assert.Fail(t, "Unexpected body of Insert")
		} else {
			assert.Len(t, tpl, 3, "Unexpected body of Insert (tuple len)")
			assert.EqualValues(t, i, tpl[0], "Unexpected body of Insert (0)")
			assert.Equal(t, fmt.Sprintf("val %d", i), tpl[1], "Unexpected body of Insert (1)")
			assert.Equal(t, "bla", tpl[2], "Unexpected body of Insert (2)")
		}
	}

	// Replace
	for i := 1015; i < 1020; i++ {
		req = NewReplaceRequest(spaceName).
			Tuple([]any{uint(i), fmt.Sprintf("val %d", i), "blar"})
		data, err = conn.Do(req).Get()
		require.NoError(t, err, "Failed to Decode")
		require.Len(t, data, 1, "Response Body len != 1")
		if tpl, ok := data[0].([]any); !ok {
			assert.Fail(t, "Unexpected body of Replace")
		} else {
			assert.Len(t, tpl, 3, "Unexpected body of Replace (tuple len)")
			assert.EqualValues(t, i, tpl[0], "Unexpected body of Replace (0)")
			assert.Equal(t, fmt.Sprintf("val %d", i), tpl[1], "Unexpected body of Replace (1)")
			assert.Equal(t, "blar", tpl[2], "Unexpected body of Replace (2)")
		}
	}

	// Delete
	req = NewDeleteRequest(spaceName).
		Key([]any{uint(1016)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Delete")
	require.NotNil(t, data, "Response data is nil after Delete")
	require.Len(t, data, 1, "Response Body len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Delete")
	} else {
		assert.Len(t, tpl, 3, "Unexpected body of Delete (tuple len)")
		assert.EqualValues(t, 1016, tpl[0], "Unexpected body of Delete (0)")
		assert.Equal(t, "val 1016", tpl[1], "Unexpected body of Delete (1)")
		assert.Equal(t, "blar", tpl[2], "Unexpected body of Delete (2)")
	}

	// Update without operations.
	req = NewUpdateRequest(spaceName).
		Index(indexName).
		Key([]any{uint(1010)})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Update")
	require.NotNil(t, data, "Response data is nil after Update")
	require.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Update")
	} else {
		assert.EqualValues(t, 1010, tpl[0], "Unexpected body of Update (0)")
		assert.Equal(t, "val 1010", tpl[1], "Unexpected body of Update (1)")
		assert.Equal(t, "bla", tpl[2], "Unexpected body of Update (2)")
	}

	// Update.
	req = NewUpdateRequest(spaceName).
		Index(indexName).
		Key([]any{uint(1010)}).
		Operations(NewOperations().Assign(1, "bye").Insert(2, 1))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Update")
	require.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		assert.Fail(t, "Unexpected body of Select")
	} else {
		assert.EqualValues(t, 1010, tpl[0], "Unexpected body of Update (0)")
		assert.Equal(t, "bye", tpl[1], "Unexpected body of Update (1)")
		assert.EqualValues(t, 1, tpl[2], "Unexpected body of Update (2)")
	}

	// Upsert without operations.
	req = NewUpsertRequest(spaceNo).
		Tuple([]any{uint(1010), "hi", "hi"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (update)")
	require.Empty(t, data, "Response Data len != 0")

	// Upsert.
	req = NewUpsertRequest(spaceNo).
		Tuple([]any{uint(1010), "hi", "hi"}).
		Operations(NewOperations().Assign(2, "bye"))
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Upsert (update)")
	require.Empty(t, data, "Response Data len != 0")

	// Call
	req = NewCallRequest("simple_concat").Args([]any{"1"})
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to use Call")
	assert.Equal(t, "11", data[0], "result is not {{1}}")

	// Eval
	req = NewEvalRequest("return 5 + 6")
	data, err = conn.Do(req).Get()
	require.NoError(t, err, "Failed to Eval")
	assert.GreaterOrEqual(t, len(data), 1, "Response.Data is empty after Eval")
	assert.EqualValues(t, 11, data[0], "5 + 6 == 11, but got")

	// Tarantool supports SQL since version 2.0.0
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 0, 0)
	require.NoError(t, err, "Could not check the Tarantool version")
	if isLess {
		return
	}

	req = NewExecuteRequest(createTableQuery)
	resp, err := conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	data, err = resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	require.Empty(t, data, "Response Body len != 0")
	exResp, ok := resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.NoError(t, err, "Error while getting SQLInfo")
	assert.Equal(t, uint64(1), sqlInfo.AffectedCount, "Incorrect count of created spaces")

	req = NewExecuteRequest(dropQuery2)
	resp, err = conn.Do(req).GetResponse()
	require.NoError(t, err, "Failed to Execute")
	require.NotNil(t, resp, "Response is nil after Execute")
	data, err = resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	require.Empty(t, data, "Response Body len != 0")
	exResp, ok = resp.(*ExecuteResponse)
	assert.True(t, ok, "Got wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.NoError(t, err, "Error while getting SQLInfo")
	assert.Equal(t, uint64(1), sqlInfo.AffectedCount, "Incorrect count of dropped spaces")
}

func testConnectionDoSelectRequestPrepare(t *testing.T, conn Connector) {
	t.Helper()

	for i := 1010; i < 1020; i++ {
		req := NewReplaceRequest(spaceName).Tuple(
			[]any{uint(i), fmt.Sprintf("val %d", i), "bla"})
		_, err := conn.Do(req).Get()
		require.NoError(t, err, "Unable to prepare tuples")
	}
}

func testConnectionDoSelectRequestCheck(t *testing.T,
	resp *SelectResponse, err error, pos bool, dataLen int, firstKey uint64) {
	t.Helper()

	require.NoError(t, err, "Failed to Select")
	require.NotNil(t, resp, "Response is nil after Select")
	respPos, err := resp.Pos()
	require.NoError(t, err, "Error while getting Pos")
	if !pos {
		assert.Nil(t, respPos, "Response should not have a position descriptor")
	}
	if pos {
		require.NotNil(t, respPos, "A response must have a position descriptor")
	}
	data, err := resp.Decode()
	require.NoError(t, err, "Failed to Decode")
	require.Len(t, data, dataLen, "Response Data len")
	for i := range dataLen {
		key := firstKey + uint64(i)
		if tpl, ok := data[i].([]any); !ok {
			assert.Fail(t, "Unexpected body of Select")
		} else {
			assert.EqualValues(t, key, tpl[0], "Unexpected body of Select (0)")
			expectedSecond := fmt.Sprintf("val %d", key)
			assert.Equal(t, expectedSecond, tpl[1], "Unexpected body of Select (1)")
			assert.Equal(t, "bla", tpl[2], "Unexpected body of Select (2)")
		}
	}
}

func TestConnectionDoSelectRequest(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(10).
		Iterator(IterGe).
		Key([]any{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, false, 10, 1010)
}

func TestConnectionDoWatchOnceRequest(t *testing.T) {
	test_helpers.SkipIfWatchOnceUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewBroadcastRequest("hello").Value("world")).Get()
	require.NoError(t, err, "Failed to create a broadcast")

	data, err := conn.Do(NewWatchOnceRequest("hello")).Get()
	require.NoError(t, err, "Failed to WatchOnce")
	assert.Equal(t, "world", data[0], "Failed to WatchOnce: wrong value returned")
}

func TestConnectionDoWatchOnceOnEmptyKey(t *testing.T) {
	test_helpers.SkipIfWatchOnceUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	data, err := conn.Do(NewWatchOnceRequest("notexists!")).Get()
	require.NoError(t, err, "Failed to WatchOnce")
	assert.Empty(t, data, "Failed to WatchOnce: wrong value returned")
}

func TestConnectionDoSelectRequest_fetch_pos(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]any{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1010)
}

func TestConnectDoSelectRequest_after_tuple(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]any{uint(1010)}).
		After([]any{uint(1012)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1013)
}

func TestConnectionDoSelectRequest_pagination_pos(t *testing.T) {
	test_helpers.SkipIfPaginationUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	testConnectionDoSelectRequestPrepare(t, conn)

	req := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(2).
		Iterator(IterGe).
		FetchPos(true).
		Key([]any{uint(1010)})
	resp, err := conn.Do(req).GetResponse()

	selResp, ok := resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1010)

	selPos, err := selResp.Pos()
	require.NoError(t, err, "Error while getting Pos")

	resp, err = conn.Do(req.After(selPos)).GetResponse()
	selResp, ok = resp.(*SelectResponse)
	assert.True(t, ok, "Got wrong response type")

	testConnectionDoSelectRequestCheck(t, selResp, err, true, 2, 1012)
}

func TestCallRequest(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	req := NewCallRequest("simple_concat").Args([]any{"1"})
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to use Call")
	assert.Equal(t, "11", data[0], "result is not {{1}}")
}

func TestClientRequestObjectsWithNilContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()
	req := NewPingRequest().Context(nil)
	data, err := conn.Do(req).Get()
	require.NoError(t, err, "Failed to Ping")
	assert.Empty(t, data, "Response Body len != 0")
}

func TestClientRequestObjectsWithPassedCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewPingRequest().Context(ctx)
	cancel()
	resp, err := conn.Do(req).Get()
	require.True(t,
		contextDoneErrRegexp.MatchString(err.Error()),
		"Failed to catch an error from done context")
	require.Nil(t, resp, "Response is not nil after the occurred error")
}

// Checking comparable with simple context.WithCancel.
func TestComparableErrorsCanceledContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewPingRequest().Context(ctx)
	cancel()
	_, err := conn.Do(req).Get()
	require.ErrorIs(t, err, context.Canceled)
}

// Checking comparable with simple context.WithTimeout.
func TestComparableErrorsTimeoutContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	timeout := time.Nanosecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	req := NewPingRequest().Context(ctx)
	defer cancel()
	_, err := conn.Do(req).Get()
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// Checking comparable with context.WithCancelCause.
// Shows ability to compare with custom errors (also with ClientError).
func TestComparableErrorsCancelCauseContext(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	ctxCause, cancelCause := context.WithCancelCause(context.Background())
	req := NewPingRequest().Context(ctxCause)
	cancelCause(ClientError{ErrConnectionClosed, "something went wrong"})
	_, err := conn.Do(req).Get()
	var tmpErr ClientError
	require.ErrorAs(t, err, &tmpErr)
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
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	req := &waitCtxRequest{ctx: ctx}
	req.wg.Add(1)

	var futWg sync.WaitGroup
	var fut Future

	futWg.Add(1)
	go func() {
		defer futWg.Done()
		fut = conn.Do(req)
	}()

	cancel()
	req.wg.Done()

	futWg.Wait()
	require.NotNil(t, fut, "fut must be not nil")

	resp, err := fut.Get()
	require.Nil(t, resp, "response must be nil")
	require.Error(t, err, "caught nil error")
	require.True(t, contextDoneErrRegexp.MatchString(err.Error()), "wrong error caught")
}

func TestComplexStructs(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	tuple := Tuple2{Cid: 777, Orig: "orig", Members: []Member{{"lol", "", 1}, {"wut", "", 3}}}
	_, err = conn.Do(NewReplaceRequest(spaceNo).Tuple(&tuple)).Get()
	require.NoError(t, err, "Failed to insert")

	var tuples [1]Tuple2
	err = conn.Do(NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]any{777}),
	).GetTyped(&tuples)
	require.NoError(t, err, "Failed to selectTyped")

	assert.Len(t, tuples, 1, "Failed to selectTyped: unexpected array length")
	assert.Equal(t, tuple.Cid, tuples[0].Cid)
	assert.Len(t, tuple.Members, len(tuples[0].Members))
	assert.Equal(t, tuple.Members[1].Name, tuples[0].Members[1].Name)
}

func TestStream_IdValues(t *testing.T) {
	test_helpers.SkipIfStreamsUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
			require.NoError(t, err, "Failed to Ping")
		})
	}
}

func TestStream_Commit(t *testing.T) {
	var req Request
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.NoError(t, err, "Failed to Begin")

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]any{uint(1001), "hello2", "world2"})
	_, err = stream.Do(req).Get()
	require.NoError(t, err, "Failed to Insert")
	defer test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []any{uint(1001)})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(1001)})
	data, err := conn.Do(selectReq).Get()
	require.NoError(t, err, "Failed to Select")
	require.Empty(t, data, "Response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.NoError(t, err, "Failed to Select")
	require.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		require.Fail(t, "Unexpected body of Select")
	} else {
		require.EqualValues(t, 1001, tpl[0], "Unexpected body of Select (0)")
		require.Equal(t, "hello2", tpl[1], "Unexpected body of Select (1)")
		require.Equal(t, "world2", tpl[2], "Unexpected body of Select (2)")
	}

	// Commit transaction
	req = NewCommitRequest()
	_, err = stream.Do(req).Get()
	require.NoError(t, err, "Failed to Commit")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.NoError(t, err, "Failed to Select")
	require.Len(t, data, 1, "Response Data len != 1")
	if tpl, ok := data[0].([]any); !ok {
		require.Fail(t, "Unexpected body of Select")
	} else {
		require.EqualValues(t, 1001, tpl[0], "Unexpected body of Select (0)")
		require.Equal(t, "hello2", tpl[1], "Unexpected body of Select (1)")
		require.Equal(t, "world2", tpl[2], "Unexpected body of Select (2)")
	}
}

func TestStream_Rollback(t *testing.T) {
	var req Request
	var err error
	var conn *Connection

	test_helpers.SkipIfStreamsUnsupported(t)

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stream, _ := conn.NewStream()

	// Begin transaction
	req = NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "Failed to Begin")

	// Insert in stream
	req = NewInsertRequest(spaceName).
		Tuple([]any{uint(1001), "hello2", "world2"})
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "Failed to Insert")
	defer test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []any{uint(1001)})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(IterEq).
		Key([]any{uint(1001)})
	data, err := conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "Failed to Select")
	require.Emptyf(t, data, "Response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.NoErrorf(t, err, "Failed to Select")
	require.Lenf(t, data, 1, "Response Data len != 1")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "Unexpected body of Select")

	id, err := test_helpers.ConvertUint64(tpl[0])
	require.NoErrorf(t, err, "Unexpected body of Select (0)")
	require.Equalf(t, uint64(1001), id, "Unexpected body of Select (0)")

	h, ok := tpl[1].(string)
	require.Truef(t, ok, "Unexpected body of Select (1)")
	require.Equalf(t, "hello2", h, "Unexpected body of Select (1)")

	h2, ok := tpl[2].(string)
	require.Truef(t, ok, "Unexpected body of Select (2)")
	require.Equalf(t, "world2", h2, "Unexpected body of Select (2)")

	// Rollback transaction
	req = NewRollbackRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "Failed to Rollback")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "Failed to Select")
	require.Emptyf(t, data, "Response Data len != 0")

	// Select inside of stream after rollback
	_, err = stream.Do(selectReq).Get()
	require.NoErrorf(t, err, "Failed to Select")
	require.Emptyf(t, data, "Response Data len != 0")
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
	defer func() { _ = conn.Close() }()

	stream, _ := conn.NewStream()

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		_, err = stream.Do(req).Get()
		require.NoErrorf(t, err, "failed to Begin")

		// Insert in stream
		req = NewInsertRequest(spaceName).
			Tuple([]any{uint(1001), "hello2", "world2"})
		_, err = stream.Do(req).Get()
		require.NoErrorf(t, err, "failed to Insert")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(IterEq).
			Key([]any{uint(1001)})
		data, err := conn.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Emptyf(t, data, "response Data len != 0")

		// Select in stream
		data, err = stream.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Lenf(t, data, 1, "response Body len != 1 after Select")

		tpl, ok := data[0].([]any)
		require.Truef(t, ok, "unexpected body of Select")
		require.Lenf(t, tpl, 3, "unexpected body of Select")

		key, err := test_helpers.ConvertUint64(tpl[0])
		require.NoErrorf(t, err, "unexpected body of Select (0)")
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
		require.NoErrorf(t, err, "failed to Rollback")

		// Select outside of transaction
		data, err = conn.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Emptyf(t, data, "response Data len != 0")

		// Select inside of stream after rollback
		data, err = stream.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Emptyf(t, data, "response Data len != 0")

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []any{uint(1001)})
	}
}

func TestStream_DoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request " +
		"doesn't belong to the current connection or connection pool")

	conn := &Connection{}
	stream, _ := conn.NewStream()
	req := test_helpers.NewMockRequest()

	_, err := stream.Do(req).Get()
	require.Errorf(t, err, "nil error has been caught")
	require.EqualError(t, err, expectedErr.Error())
}

func TestStream_DoWithClosedConn(t *testing.T) {
	expectedErr := fmt.Errorf("using closed connection")

	test_helpers.SkipIfStreamsUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)

	stream, _ := conn.NewStream()
	_ = conn.Close()

	// Begin transaction
	req := NewBeginRequest()
	_, err := stream.Do(req).Get()
	require.Errorf(t, err, "nil error has been caught")
	require.Contains(t, err.Error(), expectedErr.Error())
}

func TestConnectionBoxSessionPushUnsupported(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	testOpts := opts
	testOpts.Logger = logger

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewCallRequest("push_func").Args([]any{1})).Get()
	require.NoError(t, err)

	actualLog := buf.String()
	require.Contains(t, actualLog, LogMsgPushUnsupported)
}

func TestConnectionProtocolInfoSupported(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
	defer func() { _ = conn.Close() }()

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
	require.NoErrorf(t, err, "No errors on Id request execution")
	require.NotNilf(t, data, "Response data not empty")
	require.Len(t, data, 1, "Response data contains exactly one object")

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
	defer func() { _ = conn.Close() }()

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
	}).Context(nil)
	data, err := conn.Do(req).Get()
	require.NoErrorf(t, err, "No errors on Id request execution")
	require.NotNilf(t, data, "Response data not empty")
	require.Len(t, data, 1, "Response data contains exactly one object")

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
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	req := NewIdRequest(ProtocolInfo{
		Version:  ProtocolVersion(1),
		Features: []iproto.Feature{iproto.IPROTO_FEATURE_STREAMS},
	}).Context(ctx)
	cancel()
	resp, err := conn.Do(req).Get()
	require.Nilf(t, resp, "Response is empty")
	require.Errorf(t, err, "Error is not empty")
	require.Regexp(t, contextDoneErrRegexp, err.Error())
}

func TestConnectionProtocolInfoUnsupported(t *testing.T) {
	test_helpers.SkipIfIdSupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	serverProtocolInfo := conn.ProtocolInfo()
	expected := ProtocolInfo{}
	require.Equal(t, expected, serverProtocolInfo)
}

func TestConnectionServerFeaturesImmutable(t *testing.T) {
	test_helpers.SkipIfIdUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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

	require.NoErrorf(t, err, "No errors on connect")
	require.NotNilf(t, conn, "Connect success")

	_ = conn.Close()
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
	require.Errorf(t, err, "Got error on connect")
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
	require.NoErrorf(t, err, "No errors on connect")

	_ = conn.Close()
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
	require.Errorf(t, err, "Got error on connect")
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
	require.Errorf(t, err, "Got error on connect")
	require.Contains(t,
		err.Error(),
		"invalid server protocol: protocol features IPROTO_FEATURE_TRANSACTIONS, "+
			"Feature(15532) are not supported")
}

func TestErrorExtendedInfoBasic(t *testing.T) {
	test_helpers.SkipIfErrorExtendedInfoUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewEvalRequest("not a Lua code").Args([]any{})).Get()
	require.Errorf(t, err, "expected error on invalid Lua code")

	ttErr, ok := err.(Error)
	require.Truef(t, ok, "error is built from a Tarantool error")

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
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewEvalRequest("error(chained_error)").Args([]any{})).Get()
	require.Errorf(t, err, "expected error on explicit error raise")

	ttErr, ok := err.(Error)
	require.Truef(t, ok, "error is built from a Tarantool error")

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
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewEvalRequest("error(access_denied_error)").Args([]any{})).Get()
	require.Errorf(t, err, "expected error on forbidden action")

	ttErr, ok := err.(Error)
	require.Truef(t, ok, "error is built from a Tarantool error")

	expected := BoxError{
		Type:  "AccessDeniedError",
		File:  "/__w/sdk/sdk/tarantool-2.10/tarantool/src/box/func.c",
		Line:  uint64(535),
		Msg:   "Execute access to function 'forbidden_function' is denied for user 'no_grants'",
		Errno: uint64(0),
		Code:  uint64(42),
		Fields: map[string]any{
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
	defer func() { _ = conn.Close() }()

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	require.NoErrorf(t, err, "Failed to create a watch")
	defer watcher.Unregister()

	select {
	case event := <-events:
		assert.Equal(t, conn, event.Conn, "Unexpected event connection")
		assert.Equal(t, key, event.Key, "Unexpected event key")
		assert.Nil(t, event.Value, "Unexpected event value")
	case <-time.After(time.Second):
		require.Fail(t, "Failed to get watch event.")
	}
}

func newWatcherReconnectionPrepareTestConnection(t *testing.T) (*Connection, context.CancelFunc) {
	t.Helper()

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
	t.Cleanup(func() { test_helpers.StopTarantoolWithCleanup(inst) })
	require.NoErrorf(t, err, "Unable to start Tarantool")

	ctx, cancel := test_helpers.GetConnectContext()

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 0
	reconnectOpts.Notify = make(chan ConnEvent)
	conn, err := Connect(ctx, testDialer, reconnectOpts)
	require.NoErrorf(t, err, "Connection was not established")

	test_helpers.StopTarantool(inst)

	// Wait for reconnection process to be started.
	for conn.ConnectedNow() {
		time.Sleep(100 * time.Millisecond)
	}

	return conn, cancel
}

func TestNewWatcherDuringReconnect(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	conn, cancel := newWatcherReconnectionPrepareTestConnection(t)
	defer func() { _ = conn.Close() }()
	defer cancel()

	_, err := conn.NewWatcher("one", func(event WatchEvent) {})
	require.Error(t, err)
	assert.ErrorContains(t, err, "client connection is not ready")
}

func TestNewWatcherAfterClose(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	conn, cancel := newWatcherReconnectionPrepareTestConnection(t)
	defer cancel()

	_ = conn.Close()

	_, err := conn.NewWatcher("one", func(event WatchEvent) {})
	require.Error(t, err)
	assert.ErrorContains(t, err, "using closed connection")
}

func TestConnection_NewWatcher_noWatchersFeature(t *testing.T) {
	test_helpers.SkipIfWatchersSupported(t)

	const key = "TestConnection_NewWatcher_noWatchersFeature"
	testDialer := dialer
	testDialer.RequiredProtocolInfo = ProtocolInfo{Features: []iproto.Feature{}}
	conn := test_helpers.ConnectWithValidation(t, testDialer, opts)
	defer func() { _ = conn.Close() }()

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {})
	require.Nilf(t, watcher, "watcher must not be created")
	require.Errorf(t, err, "an error is expected")
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
	require.NoErrorf(t, err, "Unable to start Tarantool")

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 10

	conn := test_helpers.ConnectWithValidation(t, testDialer, reconnectOpts)
	defer func() { _ = conn.Close() }()

	events := make(chan WatchEvent)
	defer close(events)
	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	require.NoErrorf(t, err, "Failed to create a watch")
	defer watcher.Unregister()

	<-events

	test_helpers.StopTarantool(inst)
	require.NoErrorf(t, test_helpers.RestartTarantool(inst), "Unable to restart Tarantool")

	maxTime := reconnectOpts.Reconnect * time.Duration(reconnectOpts.MaxReconnects)
	select {
	case <-events:
	case <-time.After(maxTime):
		require.Fail(t, "Failed to get watch event.")
	}
}

func TestBroadcastRequest(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestBroadcastRequest"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	data, err := conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	require.NoErrorf(t, err, "Got broadcast error")
	assert.Equal(t, []any{}, data, "Got unexpected broadcast response data")

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	require.NoErrorf(t, err, "Failed to create a watch")
	defer watcher.Unregister()

	select {
	case event := <-events:
		assert.Equal(t, conn, event.Conn, "Unexpected event connection")
		assert.Equal(t, key, event.Key, "Unexpected event key")
		assert.Equal(t, value, event.Value, "Unexpected event value")
	case <-time.After(time.Second):
		require.Fail(t, "Failed to get watch event.")
	}
}

func TestBroadcastRequest_multi(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestBroadcastRequest_multi"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	events := make(chan WatchEvent)
	defer close(events)

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	require.NoErrorf(t, err, "Failed to create a watch")
	defer watcher.Unregister()

	<-events // Skip an initial event.
	for i := range 10 {
		val := fmt.Sprintf("%d", i)
		_, err := conn.Do(NewBroadcastRequest(key).Value(val)).Get()
		require.NoErrorf(t, err, "Failed to send a broadcast request")
		select {
		case event := <-events:
			assert.Equal(t, conn, event.Conn, "Unexpected event connection")
			assert.Equal(t, key, event.Key, "Unexpected event key")
			assert.Equal(t, val, event.Value.(string), "Unexpected event value")
		case <-time.After(time.Second):
			require.Failf(t, "Failed to get watch event", "%d", i)
		}
	}
}

func TestConnection_NewWatcher_multiOnKey(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnection_NewWatcher_multiOnKey"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
		require.NoErrorf(t, err, "Failed to create a watch")
		defer watcher.Unregister()
	}

	for i, ch := range events {
		select {
		case <-ch: // Skip an initial event.
		case <-time.After(2 * time.Second):
			require.Failf(t, "Failed to skip watch event for callback", "%d", i)
		}
	}

	_, err := conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	require.NoErrorf(t, err, "Failed to send a broadcast request")

	for i, ch := range events {
		select {
		case event := <-ch:
			assert.Equal(t, conn, event.Conn, "Unexpected event connection")
			assert.Equal(t, key, event.Key, "Unexpected event key")
			assert.Equal(t, value, event.Value.(string), "Unexpected event value")
		case <-time.After(2 * time.Second):
			require.Failf(t, "Failed to get watch event from callback", "%d", i)
		}
	}
}

func TestWatcher_Unregister(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestWatcher_Unregister"
	const value = "bar"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	events := make(chan WatchEvent)
	defer close(events)
	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {
		events <- event
	})
	require.NoErrorf(t, err, "Failed to create a watch")

	<-events
	watcher.Unregister()

	_, err = conn.Do(NewBroadcastRequest(key).Value(value)).Get()
	require.NoErrorf(t, err, "Got broadcast error")

	select {
	case event := <-events:
		require.Failf(t, "Get unexpected events", "%v", event)
	case <-time.After(time.Second):
	}
}

func TestConnection_NewWatcher_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestConnection_NewWatcher_concurrent"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	errors := make(chan error, testConcurrency)
	for i := range testConcurrency {
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
		assert.NoError(t, err, "An error found")
	}
}

func TestWatcher_Unregister_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestWatcher_Unregister_concurrent"

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	watcher, err := conn.NewWatcher(key, func(event WatchEvent) {})
	require.NoErrorf(t, err, "Failed to create a watch")

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	for range testConcurrency {
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
	require.NoErrorf(t, err, "Unable to start Tarantool")

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 10

	conn := test_helpers.ConnectWithValidation(t, testDialer, reconnectOpts)
	defer func() { _ = conn.Close() }()

	test_helpers.StopTarantool(inst)

	request := NewSelectRequest("test").Index("primary").Limit(1)
	_, err = conn.Do(request).Get()
	require.Errorf(t, err, "An error expected")

	require.NoErrorf(t, test_helpers.RestartTarantool(inst), "Unable to restart Tarantool")

	maxTime := reconnectOpts.Reconnect * time.Duration(reconnectOpts.MaxReconnects)
	timeout := time.After(maxTime)

	for {
		select {
		case <-timeout:
			require.Failf(t, "Failed to execute request without an error",
				"last error: %s", err)
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
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for range 100 {
		fut := conn.Do(NewCallRequest("create_spaces"))

		switch conn, err := Connect(ctx, dialer, opts); {
		case err != nil:
			require.ErrorIs(t, err, ErrConcurrentSchemaUpdate)
		case conn == nil:
			assert.Fail(t, "conn is nil")
		default:
			_ = conn.Close()
		}

		if _, err := fut.Get(); err != nil {
			assert.Errorf(t, err, "Failed to call create_spaces")
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

	require.Nilf(t, conn, "Connection was created after cancel")
	require.Errorf(t, err, "Expected error on connect")
	require.Contains(t, err.Error(), "operation was canceled")
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
	require.NoErrorf(t, err, "Unable to start Tarantool")

	var counter int
	mockDialer := mockSlowDialer{original: testDialer, counter: &counter}
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()

	reconnectOpts := opts
	reconnectOpts.Reconnect = 100 * time.Millisecond
	reconnectOpts.MaxReconnects = 100
	conn, err := Connect(ctx, mockDialer, reconnectOpts)
	require.NoError(t, err)
	_ = conn.Close()
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
	require.Error(t, err)
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
	require.Error(t, err)
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
	defer func() { _ = conn.Close() }()

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

	var resp []any
	err = conn.Do(NewEvalRequest(evalBody).Args([]any{})).GetTyped(&resp)
	require.NoError(t, err)
	require.Empty(t, resp[1], resp[1])
	require.Empty(t, resp[2], resp[2])
	require.Equal(t, int8(0), resp[0])
}

const (
	errNoSyncTransactionQueue = "The synchronous transaction queue doesn't belong to any instance"
)

func TestDoBeginRequest_IsSync(t *testing.T) {
	test_helpers.SkipIfIsSyncUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stream, err := conn.NewStream()
	require.NoError(t, err)

	_, err = stream.Do(NewBeginRequest().IsSync(true)).Get()
	require.NoError(t, err)

	_, err = stream.Do(
		NewReplaceRequest("test").Tuple([]any{1, "foo"}),
	).Get()
	require.NoError(t, err)

	_, err = stream.Do(NewCommitRequest()).Get()
	require.Error(t, err)
	assert.Contains(t, err.Error(), errNoSyncTransactionQueue)
}

func TestDoCommitRequest_IsSync(t *testing.T) {
	test_helpers.SkipIfIsSyncUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stream, err := conn.NewStream()
	require.NoError(t, err)

	_, err = stream.Do(NewBeginRequest()).Get()
	require.NoError(t, err)

	_, err = stream.Do(
		NewReplaceRequest("test").Tuple([]any{1, "foo"}),
	).Get()
	require.NoError(t, err)

	_, err = stream.Do(NewCommitRequest().IsSync(true)).Get()
	require.Error(t, err)
	assert.Contains(t, err.Error(), errNoSyncTransactionQueue)
}

func TestDoCommitRequest_NoSync(t *testing.T) {
	test_helpers.SkipIfIsSyncUnsupported(t)

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	stream, err := conn.NewStream()
	require.NoError(t, err)

	_, err = stream.Do(NewBeginRequest()).Get()
	require.NoError(t, err)

	_, err = stream.Do(
		NewReplaceRequest("test").Tuple([]any{1, "foo"}),
	).Get()
	require.NoError(t, err)

	_, err = stream.Do(NewCommitRequest()).Get()
	assert.NoError(t, err)
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
type mockAllocator struct {
	mu        sync.Mutex
	gets      []int
	puts      []int
	allocated map[uintptr]int
	freed     map[uintptr]int
}

func newMockAllocator() *mockAllocator {
	return &mockAllocator{
		allocated: make(map[uintptr]int),
		freed:     make(map[uintptr]int),
	}
}

func (m *mockAllocator) Get(length int) *[]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets = append(m.gets, length)
	buf := make([]byte, length)
	ptr := &buf
	m.allocated[uintptr(unsafe.Pointer(ptr))] = length
	return ptr
}

func (m *mockAllocator) Put(buf *[]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = append(m.puts, len(*buf))
	ptr := uintptr(unsafe.Pointer(buf))
	m.freed[ptr] = len(*buf)
}

func (m *mockAllocator) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.gets)
}

func (m *mockAllocator) putCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.puts)
}

func (m *mockAllocator) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets = nil
	m.puts = nil
	m.allocated = make(map[uintptr]int)
	m.freed = make(map[uintptr]int)
}

func (m *mockAllocator) allFreedWereAllocated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ptr := range m.freed {
		if _, ok := m.allocated[ptr]; !ok {
			return false
		}
	}
	return true
}

func (m *mockAllocator) allAllocatedWereFreed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ptr := range m.allocated {
		if _, ok := m.freed[ptr]; !ok {
			return false
		}
	}
	return true
}

func TestConnectionUsesAllocatorForAllocs(t *testing.T) {
	mockAlloc := newMockAllocator()

	testOpts := opts
	testOpts.Allocator = mockAlloc

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	mockAlloc.reset()

	_, err := conn.Do(
		tarantool.NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	require.NoError(t, err)

	_, err = conn.Do(
		tarantool.NewSelectRequest(spaceNo).
			Index(indexNo).
			Iterator(tarantool.IterEq).
			Key([]any{uint(1111)}),
	).Get()
	require.NoError(t, err)

	assert.Positive(t, mockAlloc.getCallCount())
}

func TestConnectionReleasesAllocatedSlices(t *testing.T) {
	mockAlloc := newMockAllocator()

	testOpts := opts
	testOpts.Allocator = mockAlloc

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	mockAlloc.reset()

	fut := conn.Do(
		tarantool.NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "test", "data"}),
	)
	_, err := fut.Get()
	require.NoError(t, err)

	fut.Release()

	assert.Positive(t, mockAlloc.putCallCount())
	assert.True(t, mockAlloc.allFreedWereAllocated())
	assert.True(t, mockAlloc.allAllocatedWereFreed())
}

func TestConnectionNoReleaseWithoutReleaseCall(t *testing.T) {
	mockAlloc := newMockAllocator()

	testOpts := opts
	testOpts.Allocator = mockAlloc

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	mockAlloc.reset()

	fut := conn.Do(
		tarantool.NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "test", "data"}),
	)
	_, err := fut.Get()
	require.NoError(t, err)

	// Do not call Release().

	assert.Zero(t, mockAlloc.putCallCount())
}

func TestConnectionReleasesAllocationsOnConnect(t *testing.T) {
	mockAlloc := newMockAllocator()

	testOpts := opts
	testOpts.Allocator = mockAlloc

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)

	assert.Positive(t, mockAlloc.getCallCount())

	require.NoError(t, conn.Close())

	assert.True(t, mockAlloc.allFreedWereAllocated())
	assert.True(t, mockAlloc.allAllocatedWereFreed())
}

type nilMockAllocator struct {
	mu      sync.Mutex
	getCall int
	putCall int
}

func newNilMockAllocator() *nilMockAllocator {
	return &nilMockAllocator{}
}

func (m *nilMockAllocator) Get(length int) *[]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCall++
	return nil
}

func (m *nilMockAllocator) Put(buf *[]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.putCall++
}

func (m *nilMockAllocator) getCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.getCall
}

func (m *nilMockAllocator) putCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.putCall
}

func TestConnectionNilAllocatorNeverCallsPut(t *testing.T) {
	mockAlloc := newNilMockAllocator()

	testOpts := opts
	testOpts.Allocator = mockAlloc

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(
		tarantool.NewReplaceRequest(spaceNo).
			Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	require.NoError(t, err)

	_, err = conn.Do(
		tarantool.NewSelectRequest(spaceNo).
			Index(indexNo).
			Iterator(tarantool.IterEq).
			Key([]any{uint(1111)}),
	).Get()
	require.NoError(t, err)

	assert.Positive(t, mockAlloc.getCallCount())
	assert.Zero(t, mockAlloc.putCallCount())
}

func runTestMain(m *testing.M) int {
	// Tarantool supports streams and interactive transactions since version 2.10.0
	isStreamUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Could not check the Tarantool version: %s", err)
	}

	startOpts.MemtxUseMvccEngine = !isStreamUnsupported

	inst, err := test_helpers.StartTarantool(startOpts)
	if err != nil {
		log.Printf("Failed to prepare test tarantool: %s", err)
		return 1
	}

	defer test_helpers.StopTarantoolWithCleanup(inst)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
