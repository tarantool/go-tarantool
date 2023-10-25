package pool_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Key      string
	Value    string
}

var testRoles = []bool{true, true, false, true, true}

func examplePool(roles []bool, connOpts tarantool.Opts) (*pool.ConnectionPool, error) {
	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	if err != nil {
		return nil, fmt.Errorf("ConnectionPool is not established")
	}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, servers, connOpts)
	if err != nil || connPool == nil {
		return nil, fmt.Errorf("ConnectionPool is not established")
	}

	return connPool, nil
}

func ExampleConnectionPool_Do() {
	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer connPool.Close()

	modes := []pool.Mode{
		pool.ANY,
		pool.RW,
		pool.RO,
		pool.PreferRW,
		pool.PreferRO,
	}
	for _, m := range modes {
		// It could be any request object.
		req := tarantool.NewPingRequest()
		_, err := connPool.Do(req, m).Get()
		fmt.Println("Ping Error", err)
	}
	// Output:
	// Ping Error <nil>
	// Ping Error <nil>
	// Ping Error <nil>
	// Ping Error <nil>
	// Ping Error <nil>
}

func ExampleConnectionPool_NewPrepared() {
	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer connPool.Close()

	stmt, err := connPool.NewPrepared("SELECT 1", pool.ANY)
	if err != nil {
		fmt.Println(err)
	}

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	_, err = connPool.Do(executeReq, pool.ANY).Get()
	if err != nil {
		fmt.Printf("Failed to execute prepared stmt")
	}
	_, err = connPool.Do(unprepareReq, pool.ANY).Get()
	if err != nil {
		fmt.Printf("Failed to prepare")
	}
}

func ExampleConnectionPool_NewWatcher() {
	const key = "foo"
	const value = "bar"

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []iproto.Feature{
		iproto.IPROTO_FEATURE_WATCHERS,
	}

	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer connPool.Close()

	callback := func(event tarantool.WatchEvent) {
		fmt.Printf("event connection: %s\n", event.Conn.Addr())
		fmt.Printf("event key: %s\n", event.Key)
		fmt.Printf("event value: %v\n", event.Value)
	}
	mode := pool.ANY
	watcher, err := connPool.NewWatcher(key, callback, mode)
	if err != nil {
		fmt.Printf("Unexpected error: %s\n", err)
		return
	}
	defer watcher.Unregister()

	connPool.Do(tarantool.NewBroadcastRequest(key).Value(value), mode).Get()
	time.Sleep(time.Second)
}

func ExampleConnectionPool_NewWatcher_noWatchersFeature() {
	const key = "foo"

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []iproto.Feature{}

	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer connPool.Close()

	callback := func(event tarantool.WatchEvent) {}
	watcher, err := connPool.NewWatcher(key, callback, pool.ANY)
	fmt.Println(watcher)
	if err != nil {
		// Need to split the error message into two lines to pass
		// golangci-lint.
		str := err.Error()
		fmt.Println(strings.Trim(str[:56], " "))
		fmt.Println(str[56:])
	} else {
		fmt.Println(err)
	}
	// Output:
	// <nil>
	// the feature IPROTO_FEATURE_WATCHERS must be required by
	// connection options to create a watcher
}

func getTestTxnOpts() tarantool.Opts {
	txnOpts := connOpts.Clone()

	// Assert that server supports expected protocol features
	txnOpts.RequiredProtocolInfo = tarantool.ProtocolInfo{
		Version: tarantool.ProtocolVersion(1),
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_STREAMS,
			iproto.IPROTO_FEATURE_TRANSACTIONS,
		},
	}

	return txnOpts
}

func ExampleCommitRequest() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	connPool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer connPool.Close()

	// example pool has only one rw instance
	stream, err := connPool.NewStream(pool.RW)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"example_commit_key", "example_commit_value"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"example_commit_key"})
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream before commit: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Commit transaction
	req = tarantool.NewCommitRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Commit: %s", err.Error())
		return
	}
	fmt.Printf("Commit transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	// example pool has only one rw instance
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after commit: response is %#v\n", resp.Data)
}

func ExampleRollbackRequest() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	// example pool has only one rw instance
	connPool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer connPool.Close()

	stream, err := connPool.NewStream(pool.RW)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"example_rollback_key", "example_rollback_value"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"example_rollback_key"})
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	// example pool has only one rw instance
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", resp.Data)
}

func ExampleBeginRequest_TxnIsolation() {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isLess, _ := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil || isLess {
		return
	}

	txnOpts := getTestTxnOpts()
	// example pool has only one rw instance
	connPool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer connPool.Close()

	stream, err := connPool.NewStream(pool.RW)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Begin transaction
	req = tarantool.NewBeginRequest().
		TxnIsolation(tarantool.ReadConfirmedLevel).
		Timeout(500 * time.Millisecond)
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Begin: %s", err.Error())
		return
	}
	fmt.Printf("Begin transaction: response is %#v\n", resp.Code)

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"isolation_level_key", "isolation_level_value"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Insert: %s", err.Error())
		return
	}
	fmt.Printf("Insert in stream: response is %#v\n", resp.Code)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"isolation_level_key"})
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select out of stream: response is %#v\n", resp.Data)

	// Select in stream
	resp, err = stream.Do(selectReq).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select in stream: response is %#v\n", resp.Data)

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		fmt.Printf("Failed to Rollback: %s", err.Error())
		return
	}
	fmt.Printf("Rollback transaction: response is %#v\n", resp.Code)

	// Select outside of transaction
	// example pool has only one rw instance
	resp, err = connPool.Do(selectReq, pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", resp.Data)
}

func ExampleConnectorAdapter() {
	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer connPool.Close()

	adapter := pool.NewConnectorAdapter(connPool, pool.RW)
	var connector tarantool.Connector = adapter

	// Ping an RW instance to check connection.
	resp, err := connector.Do(tarantool.NewPingRequest()).Get()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}

// ExampleConnectionPool_CloseGraceful_force demonstrates how to force close
// a connection pool with graceful close in progress after a while.
func ExampleConnectionPool_CloseGraceful_force() {
	connPool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
		return
	}

	eval := `local fiber = require('fiber')
	local time = ...
	fiber.sleep(time)
`
	req := tarantool.NewEvalRequest(eval).Args([]interface{}{10})
	fut := connPool.Do(req, pool.ANY)

	done := make(chan struct{})
	go func() {
		connPool.CloseGraceful()
		fmt.Println("ConnectionPool.CloseGraceful() done!")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		fmt.Println("Force ConnectionPool.Close()!")
		connPool.Close()
	}
	<-done

	fmt.Println("Result:")
	fmt.Println(fut.Get())
	// Output:
	// Force ConnectionPool.Close()!
	// ConnectionPool.CloseGraceful() done!
	// Result:
	// <nil> connection closed by client (0x4001)
}
