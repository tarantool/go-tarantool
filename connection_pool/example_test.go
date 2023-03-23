package connection_pool_test

import (
	"fmt"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

type Tuple struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Key      string
	Value    string
}

var testRoles = []bool{true, true, false, true, true}

func examplePool(roles []bool, connOpts tarantool.Opts) (*connection_pool.ConnectionPool, error) {
	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	if err != nil {
		return nil, fmt.Errorf("ConnectionPool is not established")
	}
	connPool, err := connection_pool.Connect(servers, connOpts)
	if err != nil || connPool == nil {
		return nil, fmt.Errorf("ConnectionPool is not established")
	}

	return connPool, nil
}

func ExampleConnectionPool_Select() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}
	// Insert a new tuple {"key2", "value2"}.
	_, err = conn.Insert(spaceName, &Tuple{Key: "key2", Value: "value2"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	resp, err := pool.Select(
		spaceNo, indexNo, 0, 100, tarantool.IterEq,
		[]interface{}{"key1"}, connection_pool.PreferRW)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)
	resp, err = pool.Select(
		spaceNo, indexNo, 0, 100, tarantool.IterEq,
		[]interface{}{"key2"}, connection_pool.PreferRW)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %#v\n", resp.Data)

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Delete tuple with primary key "key2".
	_, err = conn.Delete(spaceNo, indexNo, []interface{}{"key2"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}

	// Output:
	// response is []interface {}{[]interface {}{"key1", "value1"}}
	// response is []interface {}{[]interface {}{"key2", "value2"}}
}

func ExampleConnectionPool_SelectTyped() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}
	// Insert a new tuple {"key2", "value2"}.
	_, err = conn.Insert(spaceName, &Tuple{Key: "key2", Value: "value2"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	var res []Tuple
	err = pool.SelectTyped(
		spaceNo, indexNo, 0, 100, tarantool.IterEq,
		[]interface{}{"key1"}, &res, connection_pool.PreferRW)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)
	err = pool.SelectTyped(
		spaceName, indexName, 0, 100, tarantool.IterEq,
		[]interface{}{"key2"}, &res, connection_pool.PreferRW)
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	fmt.Printf("response is %v\n", res)

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Delete tuple with primary key "key2".
	_, err = conn.Delete(spaceNo, indexNo, []interface{}{"key2"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}

	// Output:
	// response is [{{} key1 value1}]
	// response is [{{} key2 value2}]
}

func ExampleConnectionPool_SelectAsync() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}
	// Insert a new tuple {"key2", "value2"}.
	_, err = conn.Insert(spaceName, &Tuple{Key: "key2", Value: "value2"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}
	// Insert a new tuple {"key3", "value3"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key3", "value3"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	var futs [3]*tarantool.Future
	futs[0] = pool.SelectAsync(
		spaceName, indexName, 0, 2, tarantool.IterEq,
		[]interface{}{"key1"}, connection_pool.PreferRW)
	futs[1] = pool.SelectAsync(
		spaceName, indexName, 0, 1, tarantool.IterEq,
		[]interface{}{"key2"}, connection_pool.RW)
	futs[2] = pool.SelectAsync(
		spaceName, indexName, 0, 1, tarantool.IterEq,
		[]interface{}{"key3"}, connection_pool.RW)
	var t []Tuple
	err = futs[0].GetTyped(&t)
	fmt.Println("Future", 0, "Error", err)
	fmt.Println("Future", 0, "Data", t)

	resp, err := futs[1].Get()
	fmt.Println("Future", 1, "Error", err)
	fmt.Println("Future", 1, "Data", resp.Data)

	resp, err = futs[2].Get()
	fmt.Println("Future", 2, "Error", err)
	fmt.Println("Future", 2, "Data", resp.Data)

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Delete tuple with primary key "key2".
	_, err = conn.Delete(spaceNo, indexNo, []interface{}{"key2"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Delete tuple with primary key "key3".
	_, err = conn.Delete(spaceNo, indexNo, []interface{}{"key3"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}

	// Output:
	// Future 0 Error <nil>
	// Future 0 Data [{{} key1 value1}]
	// Future 1 Error <nil>
	// Future 1 Data [[key2 value2]]
	// Future 2 Error <nil>
	// Future 2 Data [[key3 value3]]
}

func ExampleConnectionPool_SelectAsync_err() {
	roles := []bool{true, true, true, true, true}
	pool, err := examplePool(roles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	var futs [3]*tarantool.Future
	futs[0] = pool.SelectAsync(
		spaceName, indexName, 0, 2, tarantool.IterEq,
		[]interface{}{"key1"}, connection_pool.RW)

	err = futs[0].Err()
	fmt.Println("Future", 0, "Error", err)

	// Output:
	// Future 0 Error can't find rw instance in pool
}

func ExampleConnectionPool_Ping() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Ping a Tarantool instance to check connection.
	resp, err := pool.Ping(connection_pool.ANY)
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}

func ExampleConnectionPool_Insert() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Insert a new tuple {"key1", "value1"}.
	resp, err := pool.Insert(spaceNo, []interface{}{"key1", "value1"})
	fmt.Println("Insert key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Insert a new tuple {"key2", "value2"}.
	resp, err = pool.Insert(spaceName, &Tuple{Key: "key2", Value: "value2"}, connection_pool.PreferRW)
	fmt.Println("Insert key2")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Delete tuple with primary key "key2".
	_, err = conn.Delete(spaceNo, indexNo, []interface{}{"key2"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}
	// Output:
	// Insert key1
	// Error <nil>
	// Code 0
	// Data [[key1 value1]]
	// Insert key2
	// Error <nil>
	// Code 0
	// Data [[key2 value2]]
}

func ExampleConnectionPool_Delete() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}
	// Insert a new tuple {"key2", "value2"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key2", "value2"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	// Delete tuple with primary key {"key1"}.
	resp, err := pool.Delete(spaceNo, indexNo, []interface{}{"key1"})
	fmt.Println("Delete key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key { "key2" }.
	resp, err = pool.Delete(spaceName, indexName, []interface{}{"key2"}, connection_pool.PreferRW)
	fmt.Println("Delete key2")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Delete key1
	// Error <nil>
	// Code 0
	// Data [[key1 value1]]
	// Delete key2
	// Error <nil>
	// Code 0
	// Data [[key2 value2]]
}

func ExampleConnectionPool_Replace() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	// Replace a tuple with primary key ""key1.
	// Note, Tuple is defined within tests, and has EncdodeMsgpack and
	// DecodeMsgpack methods.
	resp, err := pool.Replace(spaceNo, []interface{}{"key1", "new_value"})
	fmt.Println("Replace key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = pool.Replace(spaceName, []interface{}{"key1", "another_value"})
	fmt.Println("Replace key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = pool.Replace(spaceName, &Tuple{Key: "key1", Value: "value2"})
	fmt.Println("Replace key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	resp, err = pool.Replace(spaceName, &Tuple{Key: "key1", Value: "new_value2"}, connection_pool.PreferRW)
	fmt.Println("Replace key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}

	// Output:
	// Replace key1
	// Error <nil>
	// Code 0
	// Data [[key1 new_value]]
	// Replace key1
	// Error <nil>
	// Code 0
	// Data [[key1 another_value]]
	// Replace key1
	// Error <nil>
	// Code 0
	// Data [[key1 value2]]
	// Replace key1
	// Error <nil>
	// Code 0
	// Data [[key1 new_value2]]
}

func ExampleConnectionPool_Update() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn, err := tarantool.Connect(servers[2], connOpts)
	if err != nil || conn == nil {
		fmt.Printf("failed to connect to %s", servers[2])
		return
	}

	// Insert a new tuple {"key1", "value1"}.
	_, err = conn.Insert(spaceNo, []interface{}{"key1", "value1"})
	if err != nil {
		fmt.Printf("Failed to insert: %s", err.Error())
		return
	}

	// Update tuple with primary key { "key1" }.
	resp, err := pool.Update(
		spaceName, indexName, []interface{}{"key1"},
		[]interface{}{[]interface{}{"=", 1, "new_value"}}, connection_pool.PreferRW)
	fmt.Println("Update key1")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)

	// Delete tuple with primary key "key1".
	_, err = conn.Delete(spaceName, indexName, []interface{}{"key1"})
	if err != nil {
		fmt.Printf("Failed to delete: %s", err.Error())
	}

	// Output:
	// Update key1
	// Error <nil>
	// Code 0
	// Data [[key1 new_value]]
}

func ExampleConnectionPool_Call() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Call a function 'simple_incr' with arguments.
	resp, err := pool.Call17("simple_incr", []interface{}{1}, connection_pool.PreferRW)
	fmt.Println("Call simple_incr()")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Call simple_incr()
	// Error <nil>
	// Code 0
	// Data [2]
}

func ExampleConnectionPool_Eval() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Run raw Lua code.
	resp, err := pool.Eval("return 1 + 2", []interface{}{}, connection_pool.PreferRW)
	fmt.Println("Eval 'return 1 + 2'")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
	// Output:
	// Eval 'return 1 + 2'
	// Error <nil>
	// Code 0
	// Data [3]
}

func ExampleConnectionPool_Do() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	// Ping a Tarantool instance to check connection.
	req := tarantool.NewPingRequest()
	resp, err := pool.Do(req, connection_pool.ANY).Get()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}

func ExampleConnectionPool_NewPrepared() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	stmt, err := pool.NewPrepared("SELECT 1", connection_pool.ANY)
	if err != nil {
		fmt.Println(err)
	}

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	_, err = pool.Do(executeReq, connection_pool.ANY).Get()
	if err != nil {
		fmt.Printf("Failed to execute prepared stmt")
	}
	_, err = pool.Do(unprepareReq, connection_pool.ANY).Get()
	if err != nil {
		fmt.Printf("Failed to prepare")
	}
}

func ExampleConnectionPool_NewWatcher() {
	const key = "foo"
	const value = "bar"

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}

	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	callback := func(event tarantool.WatchEvent) {
		fmt.Printf("event connection: %s\n", event.Conn.Addr())
		fmt.Printf("event key: %s\n", event.Key)
		fmt.Printf("event value: %v\n", event.Value)
	}
	mode := connection_pool.ANY
	watcher, err := pool.NewWatcher(key, callback, mode)
	if err != nil {
		fmt.Printf("Unexpected error: %s\n", err)
		return
	}
	defer watcher.Unregister()

	pool.Do(tarantool.NewBroadcastRequest(key).Value(value), mode).Get()
	time.Sleep(time.Second)
}

func ExampleConnectionPool_NewWatcher_noWatchersFeature() {
	const key = "foo"

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{}

	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	callback := func(event tarantool.WatchEvent) {}
	watcher, err := pool.NewWatcher(key, callback, connection_pool.ANY)
	fmt.Println(watcher)
	fmt.Println(err)
	// Output:
	// <nil>
	// the feature WatchersFeature must be required by connection options to create a watcher
}

func getTestTxnOpts() tarantool.Opts {
	txnOpts := connOpts.Clone()

	// Assert that server supports expected protocol features
	txnOpts.RequiredProtocolInfo = tarantool.ProtocolInfo{
		Version: tarantool.ProtocolVersion(1),
		Features: []tarantool.ProtocolFeature{
			tarantool.StreamsFeature,
			tarantool.TransactionsFeature,
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
	pool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()

	// example pool has only one rw instance
	stream, err := pool.NewStream(connection_pool.RW)
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
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
	pool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()

	stream, err := pool.NewStream(connection_pool.RW)
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
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
	pool, err := examplePool(testRoles, txnOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer pool.Close()

	stream, err := pool.NewStream(connection_pool.RW)
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
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
	resp, err = pool.Do(selectReq, connection_pool.RW).Get()
	if err != nil {
		fmt.Printf("Failed to Select: %s", err.Error())
		return
	}
	fmt.Printf("Select after Rollback: response is %#v\n", resp.Data)
}

func ExampleConnectorAdapter() {
	pool, err := examplePool(testRoles, connOpts)
	if err != nil {
		fmt.Println(err)
	}
	defer pool.Close()

	adapter := connection_pool.NewConnectorAdapter(pool, connection_pool.RW)
	var connector tarantool.Connector = adapter

	// Ping an RW instance to check connection.
	resp, err := connector.Ping()
	fmt.Println("Ping Code", resp.Code)
	fmt.Println("Ping Data", resp.Data)
	fmt.Println("Ping Error", err)
	// Output:
	// Ping Code 0
	// Ping Data []
	// Ping Error <nil>
}
