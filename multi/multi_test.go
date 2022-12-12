package multi

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var server1 = "127.0.0.1:3013"
var server2 = "127.0.0.1:3014"
var spaceNo = uint32(617)
var spaceName = "test"
var indexNo = uint32(0)
var connOpts = tarantool.Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var connOptsMulti = OptsMulti{
	CheckTimeout:         1 * time.Second,
	NodesGetFunctionName: "get_cluster_nodes",
	ClusterDiscoveryTime: 3 * time.Second,
}

func TestConnError_IncorrectParams(t *testing.T) {
	multiConn, err := Connect([]string{}, tarantool.Opts{})
	if err == nil {
		t.Fatalf("err is nil with incorrect params")
	}
	if multiConn != nil {
		t.Fatalf("conn is not nill with incorrect params")
	}
	if err.Error() != "addrs should not be empty" {
		t.Errorf("incorrect error: %s", err.Error())
	}

	multiConn, err = ConnectWithOpts([]string{server1}, tarantool.Opts{}, OptsMulti{})
	if err == nil {
		t.Fatal("err is nil with incorrect params")
	}
	if multiConn != nil {
		t.Fatal("conn is not nill with incorrect params")
	}
	if err.Error() != "wrong check timeout, must be greater than 0" {
		t.Errorf("incorrect error: %s", err.Error())
	}
}

func TestConnError_Connection(t *testing.T) {
	multiConn, err := Connect([]string{"err1", "err2"}, connOpts)
	if err == nil {
		t.Errorf("err is nil with incorrect params")
		return
	}
	if multiConn != nil {
		t.Errorf("conn is not nil with incorrect params")
		return
	}
}

func TestConnSuccessfully(t *testing.T) {
	multiConn, err := Connect([]string{"err", server1}, connOpts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if multiConn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer multiConn.Close()

	if !multiConn.ConnectedNow() {
		t.Errorf("conn has incorrect status")
		return
	}
	if multiConn.getCurrentConnection().Addr() != server1 {
		t.Errorf("conn has incorrect addr")
		return
	}
}

func TestReconnect(t *testing.T) {
	multiConn, _ := Connect([]string{server1, server2}, connOpts)
	if multiConn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer multiConn.Close()

	conn, _ := multiConn.getConnectionFromPool(server1)
	conn.Close()

	if multiConn.getCurrentConnection().Addr() == server1 {
		t.Errorf("conn has incorrect addr: %s after disconnect server1", multiConn.getCurrentConnection().Addr())
	}
	if !multiConn.ConnectedNow() {
		t.Errorf("incorrect multiConn status after reconnecting")
	}

	timer = time.NewTimer(100 * time.Millisecond)
	<-timer.C
	conn, _ = multiConn.getConnectionFromPool(server1)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect conn status after reconnecting")
	}
}

func TestDisconnectAll(t *testing.T) {
	multiConn, _ := Connect([]string{server1, server2}, connOpts)
	if multiConn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C
	defer multiConn.Close()

	conn, _ := multiConn.getConnectionFromPool(server1)
	conn.Close()
	conn, _ = multiConn.getConnectionFromPool(server2)
	conn.Close()

	if multiConn.ConnectedNow() {
		t.Errorf("incorrect status after desconnect all")
	}

	timer = time.NewTimer(100 * time.Millisecond)
	<-timer.C
	if !multiConn.ConnectedNow() {
		t.Errorf("incorrect multiConn status after reconnecting")
	}
	conn, _ = multiConn.getConnectionFromPool(server1)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect server1 conn status after reconnecting")
	}
	conn, _ = multiConn.getConnectionFromPool(server2)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect server2 conn status after reconnecting")
	}
}

func TestClose(t *testing.T) {
	multiConn, _ := Connect([]string{server1, server2}, connOpts)
	if multiConn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	timer := time.NewTimer(300 * time.Millisecond)
	<-timer.C

	conn, _ := multiConn.getConnectionFromPool(server1)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect conn server1 status")
	}
	conn, _ = multiConn.getConnectionFromPool(server2)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect conn server2 status")
	}

	multiConn.Close()
	timer = time.NewTimer(100 * time.Millisecond)
	<-timer.C

	if multiConn.ConnectedNow() {
		t.Errorf("incorrect multiConn status after close")
	}
	conn, _ = multiConn.getConnectionFromPool(server1)
	if conn.ConnectedNow() {
		t.Errorf("incorrect server1 conn status after close")
	}
	conn, _ = multiConn.getConnectionFromPool(server2)
	if conn.ConnectedNow() {
		t.Errorf("incorrect server2 conn status after close")
	}
}

func TestRefresh(t *testing.T) {

	multiConn, _ := ConnectWithOpts([]string{server1, server2}, connOpts, connOptsMulti)
	if multiConn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	curAddr := multiConn.addrs[0]

	// Wait for refresh timer.
	// Scenario 1 nodeload, 1 refresh, 1 nodeload.
	time.Sleep(10 * time.Second)

	newAddr := multiConn.addrs[0]

	if curAddr == newAddr {
		t.Errorf("Expect address refresh")
	}

	if !multiConn.ConnectedNow() {
		t.Errorf("Expect connection to exist")
	}

	_, err := multiConn.Call17(multiConn.opts.NodesGetFunctionName, []interface{}{})
	if err != nil {
		t.Error("Expect to get data after reconnect")
	}
}

func TestCall17(t *testing.T) {
	var resp *tarantool.Response
	var err error

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	// Call17
	resp, err = multiConn.Call17("simple_concat", []interface{}{"s"})
	if err != nil {
		t.Fatalf("Failed to use Call: %s", err.Error())
	}
	if resp.Data[0].(string) != "ss" {
		t.Fatalf("result is not {{1}} : %v", resp.Data)
	}
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	stmt, err := multiConn.NewPrepared("SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;")
	require.Nilf(t, err, "fail to prepare statement: %v", err)

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	resp, err := multiConn.Do(executeReq.Args([]interface{}{1, "test"})).Get()
	if err != nil {
		t.Fatalf("failed to execute prepared: %v", err)
	}
	if resp == nil {
		t.Fatalf("nil response")
	}
	if resp.Code != tarantool.OkCode {
		t.Fatalf("failed to execute prepared: code %d", resp.Code)
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

	// the second argument for unprepare request is unused - it already belongs to some connection
	resp, err = multiConn.Do(unprepareReq).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
	}
	if resp.Code != tarantool.OkCode {
		t.Errorf("failed to unprepare prepared statement: code %d", resp.Code)
	}

	_, err = multiConn.Do(unprepareReq).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = multiConn.Do(executeReq).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")
}

func TestDoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool")

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	req := test_helpers.NewStrangerRequest()

	_, err = multiConn.Do(req).Get()
	if err == nil {
		t.Fatalf("nil error caught")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("Unexpected error caught")
	}
}

func TestStream_Commit(t *testing.T) {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	stream, _ := multiConn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Fatalf("Failed to Begin: wrong code returned %d", resp.Code)
	}

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"1001", "hello2", "world2"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Errorf("Failed to Insert: wrong code returned %d", resp.Code)
	}
	defer test_helpers.DeleteRecordByKey(t, multiConn, spaceNo, indexNo, []interface{}{"1001"})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"1001"})
	resp, err = multiConn.Do(selectReq).Get()
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
		if id, ok := tpl[0].(string); !ok || id != "1001" {
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
	req = tarantool.NewCommitRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Commit: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Fatalf("Failed to Commit: wrong code returned %d", resp.Code)
	}

	// Select outside of transaction
	resp, err = multiConn.Do(selectReq).Get()
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
		if id, ok := tpl[0].(string); !ok || id != "1001" {
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
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	stream, _ := multiConn.NewStream()

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Begin: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Fatalf("Failed to Begin: wrong code returned %d", resp.Code)
	}

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"1001", "hello2", "world2"})
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Insert: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Errorf("Failed to Insert: wrong code returned %d", resp.Code)
	}
	defer test_helpers.DeleteRecordByKey(t, multiConn, spaceNo, indexNo, []interface{}{"1001"})

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"1001"})
	resp, err = multiConn.Do(selectReq).Get()
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
		if id, ok := tpl[0].(string); !ok || id != "1001" {
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
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	if err != nil {
		t.Fatalf("Failed to Rollback: %s", err.Error())
	}
	if resp.Code != tarantool.OkCode {
		t.Fatalf("Failed to Rollback: wrong code returned %d", resp.Code)
	}

	// Select outside of transaction
	resp, err = multiConn.Do(selectReq).Get()
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

func TestConnectionMulti_NewWatcher(t *testing.T) {
	test_helpers.SkipIfStreamsUnsupported(t)

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	watcher, err := multiConn.NewWatcher("foo", func(event tarantool.WatchEvent) {})
	if watcher != nil {
		t.Errorf("Unexpected watcher")
	}
	if err == nil {
		t.Fatalf("Unexpected success")
	}
	if err.Error() != "ConnectionMulti is deprecated use ConnectionPool" {
		t.Fatalf("Unexpected error: %s", err)
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	initScript := "config.lua"
	waitStart := 100 * time.Millisecond
	connectRetry := 3
	retryTimeout := 500 * time.Millisecond

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isStreamUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Could not check the Tarantool version")
	}

	inst1, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:         initScript,
		Listen:             server1,
		WorkDir:            "work_dir1",
		User:               connOpts.User,
		Pass:               connOpts.Pass,
		WaitStart:          waitStart,
		ConnectRetry:       connectRetry,
		RetryTimeout:       retryTimeout,
		MemtxUseMvccEngine: !isStreamUnsupported,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst1)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	inst2, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   initScript,
		Listen:       server2,
		WorkDir:      "work_dir2",
		User:         connOpts.User,
		Pass:         connOpts.Pass,
		WaitStart:    waitStart,
		ConnectRetry: connectRetry,
		RetryTimeout: retryTimeout,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst2)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
