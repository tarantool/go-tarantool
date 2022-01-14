package multi

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var server1 = "127.0.0.1:3013"
var server2 = "127.0.0.1:3014"
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
		t.Errorf("err is nil with incorrect params")
	}
	if multiConn != nil {
		t.Errorf("conn is not nill with incorrect params")
	}
	if err.Error() != "addrs should not be empty" {
		t.Errorf("incorrect error: %s", err.Error())
	}

	multiConn, err = ConnectWithOpts([]string{server1}, tarantool.Opts{}, OptsMulti{})
	if err == nil {
		t.Errorf("err is nil with incorrect params")
	}
	if multiConn != nil {
		t.Errorf("conn is not nill with incorrect params")
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
		t.Errorf("incorrect multiConn status after reconecting")
	}

	timer = time.NewTimer(100 * time.Millisecond)
	<-timer.C
	conn, _ = multiConn.getConnectionFromPool(server1)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect conn status after reconecting")
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
		t.Errorf("incorrect multiConn status after reconecting")
	}
	conn, _ = multiConn.getConnectionFromPool(server1)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect server1 conn status after reconecting")
	}
	conn, _ = multiConn.getConnectionFromPool(server2)
	if !conn.ConnectedNow() {
		t.Errorf("incorrect server2 conn status after reconecting")
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

	// wait for refresh timer
	// scenario 1 nodeload, 1 refresh, 1 nodeload
	time.Sleep(10 * time.Second)

	newAddr := multiConn.addrs[0]

	if curAddr == newAddr {
		t.Errorf("Expect address refresh")
	}

	if !multiConn.ConnectedNow() {
		t.Errorf("Expect connection to exist")
	}

	_, err := multiConn.Call(multiConn.opts.NodesGetFunctionName, []interface{}{})
	if err != nil {
		t.Error("Expect to get data after reconnect")
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
	var connectRetry uint = 3
	retryTimeout := 500 * time.Millisecond

	inst1, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   initScript,
		Listen:       server1,
		WorkDir:      "work_dir1",
		User:         connOpts.User,
		Pass:         connOpts.Pass,
		WaitStart:    waitStart,
		ConnectRetry: connectRetry,
		RetryTimeout: retryTimeout,
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
