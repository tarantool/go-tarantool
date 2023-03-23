//go:build linux || (darwin && !cgo)
// +build linux darwin,!cgo

// Use OS build flags since signals are system-dependent.

package tarantool_test

import (
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

var shtdnServer = "127.0.0.1:3014"
var shtdnClntOpts = Opts{
	User:                 opts.User,
	Pass:                 opts.Pass,
	Timeout:              20 * time.Second,
	Reconnect:            200 * time.Millisecond,
	MaxReconnects:        10,
	RequiredProtocolInfo: ProtocolInfo{Features: []ProtocolFeature{WatchersFeature}},
}
var shtdnSrvOpts = test_helpers.StartOpts{
	InitScript:   "config.lua",
	Listen:       shtdnServer,
	User:         shtdnClntOpts.User,
	Pass:         shtdnClntOpts.Pass,
	WaitStart:    100 * time.Millisecond,
	ConnectRetry: 3,
	RetryTimeout: 500 * time.Millisecond,
}

var evalMsg = "got enough sleep"
var evalBody = `
	local fiber = require('fiber')
	local time, msg = ...
	fiber.sleep(time)
	return msg
`

func testGracefulShutdown(t *testing.T, conn *Connection, inst *test_helpers.TarantoolInstance) {
	var resp *Response
	var err error

	// Set a big timeout so it would be easy to differ
	// if server went down on timeout or after all connections were terminated.
	serverShutdownTimeout := 60 // in seconds
	_, err = conn.Call("box.ctl.set_on_shutdown_timeout", []interface{}{serverShutdownTimeout})
	require.Nil(t, err)

	// Send request with sleep.
	evalSleep := 1 // in seconds
	require.Lessf(t,
		time.Duration(evalSleep)*time.Second,
		shtdnClntOpts.Timeout,
		"test request won't be failed by timeout")

	// Create a helper watcher to ensure that async
	// shutdown is set up.
	helperCh := make(chan WatchEvent, 10)
	helperW, herr := conn.NewWatcher("box.shutdown", func(event WatchEvent) {
		helperCh <- event
	})
	require.Nil(t, herr)
	defer helperW.Unregister()
	<-helperCh

	req := NewEvalRequest(evalBody).Args([]interface{}{evalSleep, evalMsg})

	fut := conn.Do(req)

	// SIGTERM the server.
	shutdownStart := time.Now()
	require.Nil(t, inst.Cmd.Process.Signal(syscall.SIGTERM))

	// Check that we can't send new requests after shutdown starts.
	// Retry helps to wait a bit until server starts to shutdown
	// and send us the shutdown event.
	shutdownWaitRetries := 5
	shutdownWaitTimeout := 100 * time.Millisecond

	err = test_helpers.Retry(func(interface{}) error {
		_, err = conn.Do(NewPingRequest()).Get()
		if err == nil {
			return fmt.Errorf("expected error for requests sent on shutdown")
		}

		if err.Error() != "server shutdown in progress (0x4005)" {
			return err
		}

		return nil
	}, nil, shutdownWaitRetries, shutdownWaitTimeout)
	require.Nil(t, err)

	// Check that requests started before the shutdown finish successfully.
	resp, err = fut.Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, resp.Data, []interface{}{evalMsg})

	// Wait until server go down.
	// Server will go down only when it process all requests from our connection
	// (or on timeout).
	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)
	shutdownFinish := time.Now()
	shutdownTime := shutdownFinish.Sub(shutdownStart)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	// Check that it wasn't a timeout.
	require.Lessf(t,
		shutdownTime,
		time.Duration(serverShutdownTimeout/2)*time.Second,
		"server went down not by timeout")

	// Connection is unavailable when server is down.
	require.Equal(t, false, conn.ConnectedNow())
}

func TestGracefulShutdown(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var conn *Connection
	var err error

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn = test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	testGracefulShutdown(t, conn, &inst)
}

func TestGracefulShutdownWithReconnect(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var err error

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn := test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	testGracefulShutdown(t, conn, &inst)

	err = test_helpers.RestartTarantool(&inst)
	require.Nilf(t, err, "Failed to restart tarantool")

	connected := test_helpers.WaitUntilReconnected(conn, shtdnClntOpts.MaxReconnects, shtdnClntOpts.Reconnect)
	require.Truef(t, connected, "Reconnect success")

	testGracefulShutdown(t, conn, &inst)
}

func TestNoGracefulShutdown(t *testing.T) {
	// No watchers = no graceful shutdown.
	noShtdnClntOpts := shtdnClntOpts.Clone()
	noShtdnClntOpts.RequiredProtocolInfo = ProtocolInfo{}
	test_helpers.SkipIfWatchersSupported(t)

	var inst test_helpers.TarantoolInstance
	var conn *Connection
	var err error

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn = test_helpers.ConnectWithValidation(t, shtdnServer, noShtdnClntOpts)
	defer conn.Close()

	evalSleep := 10             // in seconds
	serverShutdownTimeout := 60 // in seconds
	require.Less(t, evalSleep, serverShutdownTimeout)

	// Send request with sleep.
	require.Lessf(t,
		time.Duration(evalSleep)*time.Second,
		shtdnClntOpts.Timeout,
		"test request won't be failed by timeout")

	req := NewEvalRequest(evalBody).Args([]interface{}{evalSleep, evalMsg})

	fut := conn.Do(req)

	// SIGTERM the server.
	shutdownStart := time.Now()
	require.Nil(t, inst.Cmd.Process.Signal(syscall.SIGTERM))

	// Check that request was interrupted.
	_, err = fut.Get()
	require.NotNilf(t, err, "sleep request error")

	// Wait until server go down.
	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)
	shutdownFinish := time.Now()
	shutdownTime := shutdownFinish.Sub(shutdownStart)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	// Check that server finished without waiting for eval to finish.
	require.Lessf(t,
		shutdownTime,
		time.Duration(evalSleep/2)*time.Second,
		"server went down without any additional waiting")
}

func TestGracefulShutdownRespectsClose(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var conn *Connection
	var err error

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn = test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	// Create a helper watcher to ensure that async
	// shutdown is set up.
	helperCh := make(chan WatchEvent, 10)
	helperW, herr := conn.NewWatcher("box.shutdown", func(event WatchEvent) {
		helperCh <- event
	})
	require.Nil(t, herr)
	defer helperW.Unregister()
	<-helperCh

	// Set a big timeout so it would be easy to differ
	// if server went down on timeout or after all connections were terminated.
	serverShutdownTimeout := 60 // in seconds
	_, err = conn.Call("box.ctl.set_on_shutdown_timeout", []interface{}{serverShutdownTimeout})
	require.Nil(t, err)

	// Send request with sleep.
	evalSleep := 10 // in seconds
	require.Lessf(t,
		time.Duration(evalSleep)*time.Second,
		shtdnClntOpts.Timeout,
		"test request won't be failed by timeout")

	req := NewEvalRequest(evalBody).Args([]interface{}{evalSleep, evalMsg})

	fut := conn.Do(req)

	// SIGTERM the server.
	shutdownStart := time.Now()
	require.Nil(t, inst.Cmd.Process.Signal(syscall.SIGTERM))

	// Close the connection.
	conn.Close()

	// Connection is closed.
	require.Equal(t, true, conn.ClosedNow())

	// Check that request was interrupted.
	_, err = fut.Get()
	require.NotNilf(t, err, "sleep request error")

	// Wait until server go down.
	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)
	shutdownFinish := time.Now()
	shutdownTime := shutdownFinish.Sub(shutdownStart)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	// Check that server finished without waiting for eval to finish.
	require.Lessf(t,
		shutdownTime,
		time.Duration(evalSleep/2)*time.Second,
		"server went down without any additional waiting")

	// Check that it wasn't a timeout.
	require.Lessf(t,
		shutdownTime,
		time.Duration(serverShutdownTimeout/2)*time.Second,
		"server went down not by timeout")

	// Connection is still closed.
	require.Equal(t, true, conn.ClosedNow())
}

func TestGracefulShutdownNotRacesWithRequestReconnect(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var conn *Connection
	var err error

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn = test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	// Create a helper watcher to ensure that async
	// shutdown is set up.
	helperCh := make(chan WatchEvent, 10)
	helperW, herr := conn.NewWatcher("box.shutdown", func(event WatchEvent) {
		helperCh <- event
	})
	require.Nil(t, herr)
	defer helperW.Unregister()
	<-helperCh

	// Set a small timeout so server will shutdown before requesst finishes.
	serverShutdownTimeout := 1 // in seconds
	_, err = conn.Call("box.ctl.set_on_shutdown_timeout", []interface{}{serverShutdownTimeout})
	require.Nil(t, err)

	// Send request with sleep.
	evalSleep := 5 // in seconds
	require.Lessf(t,
		serverShutdownTimeout,
		evalSleep,
		"test request will be failed by timeout")
	require.Lessf(t,
		time.Duration(serverShutdownTimeout)*time.Second,
		shtdnClntOpts.Timeout,
		"test request will be failed by timeout")

	req := NewEvalRequest(evalBody).Args([]interface{}{evalSleep, evalMsg})

	evalStart := time.Now()
	fut := conn.Do(req)

	// SIGTERM the server.
	require.Nil(t, inst.Cmd.Process.Signal(syscall.SIGTERM))

	// Wait until server go down.
	// Server is expected to go down on timeout.
	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	// Check that request failed by server disconnect, not a client timeout.
	_, err = fut.Get()
	require.NotNil(t, err)
	require.NotContains(t, err.Error(), "client timeout for request")

	evalFinish := time.Now()
	evalTime := evalFinish.Sub(evalStart)

	// Check that it wasn't a client timeout.
	require.Lessf(t,
		evalTime,
		shtdnClntOpts.Timeout,
		"server went down not by timeout")
}

func TestGracefulShutdownCloseConcurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var err error
	var srvShtdnStart, srvShtdnFinish time.Time

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn := test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	// Create a helper watcher to ensure that async
	// shutdown is set up.
	helperCh := make(chan WatchEvent, 10)
	helperW, herr := conn.NewWatcher("box.shutdown", func(event WatchEvent) {
		helperCh <- event
	})
	require.Nil(t, herr)
	defer helperW.Unregister()
	<-helperCh

	// Set a big timeout so it would be easy to differ
	// if server went down on timeout or after all connections were terminated.
	serverShutdownTimeout := 60 // in seconds
	_, err = conn.Call("box.ctl.set_on_shutdown_timeout", []interface{}{serverShutdownTimeout})
	require.Nil(t, err)
	conn.Close()

	const testConcurrency = 50

	var caseWg, srvToStop, srvStop sync.WaitGroup
	caseWg.Add(testConcurrency)
	srvToStop.Add(testConcurrency)
	srvStop.Add(1)

	// Create many connections.
	for i := 0; i < testConcurrency; i++ {
		go func(i int) {
			defer caseWg.Done()

			// Do not wait till Tarantool register out watcher,
			// test everything is ok even on async.

			conn := test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
			defer conn.Close()

			// Wait till all connections created.
			srvToStop.Done()
			srvStop.Wait()
		}(i)
	}

	var sret error
	go func(inst *test_helpers.TarantoolInstance) {
		srvToStop.Wait()
		srvShtdnStart = time.Now()
		cerr := inst.Cmd.Process.Signal(syscall.SIGTERM)
		if cerr != nil {
			sret = cerr
		}
		srvStop.Done()
	}(&inst)

	srvStop.Wait()
	require.Nil(t, sret, "No errors on server SIGTERM")

	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	srvShtdnFinish = time.Now()
	srvShtdnTime := srvShtdnFinish.Sub(srvShtdnStart)

	require.Less(t,
		srvShtdnTime,
		time.Duration(serverShutdownTimeout/2)*time.Second,
		"server went down not by timeout")
}

func TestGracefulShutdownConcurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	var inst test_helpers.TarantoolInstance
	var err error
	var srvShtdnStart, srvShtdnFinish time.Time

	inst, err = test_helpers.StartTarantool(shtdnSrvOpts)
	require.Nil(t, err)
	defer test_helpers.StopTarantoolWithCleanup(inst)

	conn := test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
	defer conn.Close()

	// Set a big timeout so it would be easy to differ
	// if server went down on timeout or after all connections were terminated.
	serverShutdownTimeout := 60 // in seconds
	_, err = conn.Call("box.ctl.set_on_shutdown_timeout", []interface{}{serverShutdownTimeout})
	require.Nil(t, err)
	conn.Close()

	const testConcurrency = 50

	var caseWg, srvToStop, srvStop sync.WaitGroup
	caseWg.Add(testConcurrency)
	srvToStop.Add(testConcurrency)
	srvStop.Add(1)

	// Create many connections.
	var ret error
	for i := 0; i < testConcurrency; i++ {
		go func(i int) {
			defer caseWg.Done()

			conn := test_helpers.ConnectWithValidation(t, shtdnServer, shtdnClntOpts)
			defer conn.Close()

			// Create a helper watcher to ensure that async
			// shutdown is set up.
			helperCh := make(chan WatchEvent, 10)
			helperW, _ := conn.NewWatcher("box.shutdown", func(event WatchEvent) {
				helperCh <- event
			})
			defer helperW.Unregister()
			<-helperCh

			evalSleep := 1 // in seconds
			req := NewEvalRequest(evalBody).Args([]interface{}{evalSleep, evalMsg})
			fut := conn.Do(req)

			// Wait till all connections had started sleeping.
			srvToStop.Done()
			srvStop.Wait()

			_, gerr := fut.Get()
			if gerr != nil {
				ret = gerr
			}
		}(i)
	}

	var sret error
	go func(inst *test_helpers.TarantoolInstance) {
		srvToStop.Wait()
		srvShtdnStart = time.Now()
		cerr := inst.Cmd.Process.Signal(syscall.SIGTERM)
		if cerr != nil {
			sret = cerr
		}
		srvStop.Done()
	}(&inst)

	srvStop.Wait()
	require.Nil(t, sret, "No errors on server SIGTERM")

	caseWg.Wait()
	require.Nil(t, ret, "No errors on concurrent wait")

	_, err = inst.Cmd.Process.Wait()
	require.Nil(t, err)

	// Help test helpers to properly clean up.
	inst.Cmd.Process = nil

	srvShtdnFinish = time.Now()
	srvShtdnTime := srvShtdnFinish.Sub(srvShtdnStart)

	require.Less(t,
		srvShtdnTime,
		time.Duration(serverShutdownTimeout/2)*time.Second,
		"server went down not by timeout")
}
