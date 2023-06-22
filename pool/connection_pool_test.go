package pool_test

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var spaceNo = uint32(520)
var spaceName = "testPool"
var indexNo = uint32(0)

var ports = []string{"3013", "3014", "3015", "3016", "3017"}
var host = "127.0.0.1"
var servers = []string{
	strings.Join([]string{host, ports[0]}, ":"),
	strings.Join([]string{host, ports[1]}, ":"),
	strings.Join([]string{host, ports[2]}, ":"),
	strings.Join([]string{host, ports[3]}, ":"),
	strings.Join([]string{host, ports[4]}, ":"),
}

var connOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
	User:    "test",
	Pass:    "test",
}

var defaultCountRetry = 5
var defaultTimeoutRetry = 500 * time.Millisecond

var instances []test_helpers.TarantoolInstance

func TestConnError_IncorrectParams(t *testing.T) {
	connPool, err := pool.Connect([]string{}, tarantool.Opts{})
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "addrs (first argument) should not be empty", err.Error())

	connPool, err = pool.Connect([]string{"err1", "err2"}, connOpts)
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "no active connections", err.Error())

	connPool, err = pool.ConnectWithOpts(servers, tarantool.Opts{}, pool.Opts{})
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "wrong check timeout, must be greater than 0", err.Error())
}

func TestConnSuccessfully(t *testing.T) {
	server := servers[0]
	connPool, err := pool.Connect([]string{"err", server}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)
}

func TestConnSuccessfullyDuplicates(t *testing.T) {
	server := servers[0]
	connPool, err := pool.Connect([]string{server, server, server, server}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)

	addrs := connPool.GetAddrs()
	require.Equalf(t, []string{server}, addrs, "should be only one address")
}

func TestReconnect(t *testing.T) {
	server := servers[0]

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	test_helpers.StopTarantoolWithCleanup(instances[0])

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: false,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	err = test_helpers.RestartTarantool(&instances[0])
	require.Nilf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestDisconnect_withReconnect(t *testing.T) {
	const serverId = 0

	opts := connOpts
	opts.Reconnect = 10 * time.Second

	connPool, err := pool.Connect([]string{servers[serverId]}, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Test.
	test_helpers.StopTarantoolWithCleanup(instances[serverId])
	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{servers[serverId]},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			servers[serverId]: false,
		},
	}
	err = test_helpers.Retry(test_helpers.CheckPoolStatuses,
		args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// Restart the server after success.
	err = test_helpers.RestartTarantool(&instances[serverId])
	require.Nilf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{servers[serverId]},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[serverId]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses,
		args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestDisconnectAll(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	test_helpers.StopTarantoolWithCleanup(instances[0])
	test_helpers.StopTarantoolWithCleanup(instances[1])

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	err = test_helpers.RestartTarantool(&instances[0])
	require.Nilf(t, err, "failed to restart tarantool")

	err = test_helpers.RestartTarantool(&instances[1])
	require.Nilf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	for _, server := range servers[1:] {
		err = connPool.Add(server)
		require.Nil(t, err)
	}

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
			servers[1]: true,
			servers[2]: true,
			servers[3]: true,
			servers[4]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd_exist(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	err = connPool.Add(servers[0])
	require.Equal(t, pool.ErrExists, err)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd_unreachable(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	err = connPool.Add("127.0.0.2:6667")
	// The OS-dependent error so we just check for existence.
	require.NotNil(t, err)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd_afterClose(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	connPool.Close(true)
	err = connPool.Add(servers[0])
	assert.Equal(t, err, pool.ErrClosed)
}

func TestAdd_Close_concurrent(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Add(servers[1])
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	connPool.Close(true)

	wg.Wait()
}

func TestAdd_Close_graceful_concurrent(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Add(servers[1])
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	connPool.Close(false)

	wg.Wait()
}

func TestRemove(t *testing.T) {
	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	for _, server := range servers[1:] {
		err = connPool.Remove(server)
		require.Nil(t, err)
	}

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestRemove_double(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0], servers[1]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	err = connPool.Remove(servers[1])
	require.Nil(t, err)
	err = connPool.Remove(servers[1])
	require.ErrorContains(t, err, "endpoint not exist")

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestRemove_unknown(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0], servers[1]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	err = connPool.Remove("not_exist:6667")
	require.ErrorContains(t, err, "endpoint not exist")

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
			servers[1]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestRemove_concurrent(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0], servers[1]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	const concurrency = 10
	var (
		wg   sync.WaitGroup
		ok   uint32
		errs uint32
	)

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			err := connPool.Remove(servers[1])
			if err == nil {
				atomic.AddUint32(&ok, 1)
			} else {
				assert.ErrorContains(t, err, "endpoint not exist")
				atomic.AddUint32(&errs, 1)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, uint32(1), ok)
	assert.Equal(t, uint32(concurrency-1), errs)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestRemove_Close_concurrent(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0], servers[1]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Remove(servers[1])
		assert.Nil(t, err)
	}()

	connPool.Close(true)

	wg.Wait()
}

func TestRemove_Close_graceful_concurrent(t *testing.T) {
	connPool, err := pool.Connect([]string{servers[0], servers[1]}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Remove(servers[1])
		assert.Nil(t, err)
	}()

	connPool.Close(false)

	wg.Wait()
}

func TestClose(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)

	connPool.Close(true)

	args = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestClose_graceful(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)

	eval := `local fiber = require('fiber')
	local time = ...
	fiber.sleep(time)
`
	evalSleep := 3 // In seconds.
	req := tarantool.NewEvalRequest(eval).Args([]interface{}{evalSleep})
	fut := connPool.Do(req, pool.ANY)
	go func() {
		connPool.Close(false)
	}()

	// Check that a request rejected if graceful shutdown in progress.
	time.Sleep((time.Duration(evalSleep) * time.Second) / 2)
	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ANY).Get()
	require.ErrorContains(t, err, "can't find healthy instance in pool")

	// Check that a previous request was successful.
	resp, err := fut.Get()
	require.Nilf(t, err, "sleep request no error")
	require.NotNilf(t, resp, "sleep response exists")

	args = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

type testHandler struct {
	discovered, deactivated uint32
	errs                    []error
}

func (h *testHandler) addErr(err error) {
	h.errs = append(h.errs, err)
}

func (h *testHandler) Discovered(conn *tarantool.Connection,
	role pool.Role) error {
	discovered := atomic.AddUint32(&h.discovered, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("discovered conn == nil"))
		return nil
	}

	// discovered < 3 - initial open of connections
	// discovered >= 3 - update a connection after a role update
	addr := conn.Addr()
	if addr == servers[0] {
		if discovered < 3 && role != pool.MasterRole {
			h.addErr(fmt.Errorf("unexpected init role %d for addr %s", role, addr))
		}
		if discovered >= 3 && role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected updated role %d for addr %s", role, addr))
		}
	} else if addr == servers[1] {
		if discovered >= 3 {
			h.addErr(fmt.Errorf("unexpected discovery for addr %s", addr))
		}
		if role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected role %d for addr %s", role, addr))
		}
	} else {
		h.addErr(fmt.Errorf("unexpected discovered addr %s", addr))
	}

	return nil
}

func (h *testHandler) Deactivated(conn *tarantool.Connection,
	role pool.Role) error {
	deactivated := atomic.AddUint32(&h.deactivated, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("removed conn == nil"))
		return nil
	}

	addr := conn.Addr()
	if deactivated == 1 && addr == servers[0] {
		// A first close is a role update.
		if role != pool.MasterRole {
			h.addErr(fmt.Errorf("unexpected removed role %d for addr %s", role, addr))
		}
		return nil
	}

	if addr == servers[0] || addr == servers[1] {
		// Close.
		if role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected removed role %d for addr %s", role, addr))
		}
	} else {
		h.addErr(fmt.Errorf("unexpected removed addr %s", addr))
	}

	return nil
}

func TestConnectionHandlerOpenUpdateClose(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	roles := []bool{false, true}

	err := test_helpers.SetClusterRO(poolServers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	h := &testHandler{}
	poolOpts := pool.Opts{
		CheckTimeout:      100 * time.Microsecond,
		ConnectionHandler: h,
	}
	connPool, err := pool.ConnectWithOpts(poolServers, connOpts, poolOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	_, err = connPool.Call17("box.cfg", []interface{}{map[string]bool{
		"read_only": true,
	}}, pool.RW)
	require.Nilf(t, err, "failed to make ro")

	for i := 0; i < 100; i++ {
		// Wait for read_only update, it should report about close connection
		// with old role.
		if atomic.LoadUint32(&h.discovered) >= 3 {
			break
		}
		time.Sleep(poolOpts.CheckTimeout)
	}

	discovered := atomic.LoadUint32(&h.discovered)
	deactivated := atomic.LoadUint32(&h.deactivated)
	require.Equalf(t, uint32(3), discovered,
		"updated not reported as discovered")
	require.Equalf(t, uint32(1), deactivated,
		"updated not reported as deactivated")

	connPool.Close(true)

	for i := 0; i < 100; i++ {
		// Wait for close of all connections.
		if atomic.LoadUint32(&h.deactivated) >= 3 {
			break
		}
		time.Sleep(poolOpts.CheckTimeout)
	}

	for _, err := range h.errs {
		t.Errorf("Unexpected error: %s", err)
	}
	connected, err := connPool.ConnectedNow(pool.ANY)
	require.Nilf(t, err, "failed to get connected state")
	require.Falsef(t, connected, "connection pool still be connected")

	discovered = atomic.LoadUint32(&h.discovered)
	deactivated = atomic.LoadUint32(&h.deactivated)
	require.Equalf(t, uint32(len(poolServers)+1), discovered,
		"unexpected discovered count")
	require.Equalf(t, uint32(len(poolServers)+1), deactivated,
		"unexpected deactivated count")
}

type testAddErrorHandler struct {
	discovered, deactivated int
}

func (h *testAddErrorHandler) Discovered(conn *tarantool.Connection,
	role pool.Role) error {
	h.discovered++
	return fmt.Errorf("any error")
}

func (h *testAddErrorHandler) Deactivated(conn *tarantool.Connection,
	role pool.Role) error {
	h.deactivated++
	return nil
}

func TestConnectionHandlerOpenError(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}

	h := &testAddErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout:      100 * time.Microsecond,
		ConnectionHandler: h,
	}
	connPool, err := pool.ConnectWithOpts(poolServers, connOpts, poolOpts)
	if err == nil {
		defer connPool.Close(true)
	}
	require.NotNilf(t, err, "success to connect")
	require.Equalf(t, 2, h.discovered, "unexpected discovered count")
	require.Equalf(t, 0, h.deactivated, "unexpected deactivated count")
}

type testUpdateErrorHandler struct {
	discovered, deactivated uint32
}

func (h *testUpdateErrorHandler) Discovered(conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.discovered, 1)

	if atomic.LoadUint32(&h.deactivated) != 0 {
		// Don't add a connection into a pool again after it was deleted.
		return fmt.Errorf("any error")
	}
	return nil
}

func (h *testUpdateErrorHandler) Deactivated(conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.deactivated, 1)
	return nil
}

func TestConnectionHandlerUpdateError(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	roles := []bool{false, false}

	err := test_helpers.SetClusterRO(poolServers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	h := &testUpdateErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout:      100 * time.Microsecond,
		ConnectionHandler: h,
	}
	connPool, err := pool.ConnectWithOpts(poolServers, connOpts, poolOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	connected, err := connPool.ConnectedNow(pool.ANY)
	require.Nilf(t, err, "failed to get ConnectedNow()")
	require.Truef(t, connected, "should be connected")

	for i := 0; i < len(poolServers); i++ {
		_, err = connPool.Call17("box.cfg", []interface{}{map[string]bool{
			"read_only": true,
		}}, pool.RW)
		require.Nilf(t, err, "failed to make ro")
	}

	for i := 0; i < 100; i++ {
		// Wait for updates done.
		connected, err = connPool.ConnectedNow(pool.ANY)
		if !connected || err != nil {
			break
		}
		time.Sleep(poolOpts.CheckTimeout)
	}
	connected, err = connPool.ConnectedNow(pool.ANY)

	require.Nilf(t, err, "failed to get ConnectedNow()")
	require.Falsef(t, connected, "should not be any active connection")

	connPool.Close(true)

	connected, err = connPool.ConnectedNow(pool.ANY)

	require.Nilf(t, err, "failed to get ConnectedNow()")
	require.Falsef(t, connected, "should be deactivated")
	discovered := atomic.LoadUint32(&h.discovered)
	deactivated := atomic.LoadUint32(&h.deactivated)
	require.GreaterOrEqualf(t, discovered, deactivated, "discovered < deactivated")
	require.Nilf(t, err, "failed to get ConnectedNow()")
}

func TestRequestOnClosed(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	test_helpers.StopTarantoolWithCleanup(instances[0])
	test_helpers.StopTarantoolWithCleanup(instances[1])

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}
	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	_, err = connPool.Ping(pool.ANY)
	require.NotNilf(t, err, "err is nil after Ping")

	err = test_helpers.RestartTarantool(&instances[0])
	require.Nilf(t, err, "failed to restart tarantool")

	err = test_helpers.RestartTarantool(&instances[1])
	require.Nilf(t, err, "failed to restart tarantool")
}

func TestGetPoolInfo(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	srvs := []string{server1, server2}
	expected := []string{server1, server2}
	connPool, err := pool.Connect(srvs, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	srvs[0] = "x"
	connPool.GetAddrs()[1] = "y"

	require.ElementsMatch(t, expected, connPool.GetAddrs())
}

func TestCall(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// PreferRO
	resp, err := connPool.Call("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val := resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Call("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Call("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Call("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestCall16(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// PreferRO
	resp, err := connPool.Call16("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val := resp.Data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Call16("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Call16("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Call16("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestCall17(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// PreferRO
	resp, err := connPool.Call17("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val := resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Call17("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Call17("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Call17("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestEval(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// PreferRO
	resp, err := connPool.Eval("return box.info().ro", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok := resp.Data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, val, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, val, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, val, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, val, "expected `false` with mode `RW`")
}

type Member struct {
	id  uint
	val string
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
	if m.id, err = d.DecodeUint(); err != nil {
		return err
	}
	if m.val, err = d.DecodeString(); err != nil {
		return err
	}
	return nil
}

func TestExecute(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	request := "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0 == 1;"
	// Execute
	resp, err := connPool.Execute(request, []interface{}{}, pool.ANY)
	require.Nilf(t, err, "failed to Execute")
	require.NotNilf(t, resp, "response is nil after Execute")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Execute")
	require.Equalf(t, len(resp.Data[0].([]interface{})), 2, "unexpected response")

	// ExecuteTyped
	mem := []Member{}
	info, _, err := connPool.ExecuteTyped(request, []interface{}{}, &mem, pool.ANY)
	require.Nilf(t, err, "failed to ExecuteTyped")
	require.Equalf(t, info.AffectedCount, uint64(0), "unexpected info.AffectedCount")
	require.Equalf(t, len(mem), 1, "wrong count of results")

	// ExecuteAsync
	fut := connPool.ExecuteAsync(request, []interface{}{}, pool.ANY)
	resp, err = fut.Get()
	require.Nilf(t, err, "failed to ExecuteAsync")
	require.NotNilf(t, resp, "response is nil after ExecuteAsync")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after ExecuteAsync")
	require.Equalf(t, len(resp.Data[0].([]interface{})), 2, "unexpected response")
}

func TestRoundRobinStrategy(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	allPorts := map[string]bool{
		servers[0]: true,
		servers[1]: true,
		servers[2]: true,
		servers[3]: true,
		servers[4]: true,
	}

	masterPorts := map[string]bool{
		servers[0]: true,
		servers[2]: true,
		servers[3]: true,
	}

	replicaPorts := map[string]bool{
		servers[1]: true,
		servers[4]: true,
	}

	serversNumber := len(servers)

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)
}

func TestRoundRobinStrategy_NoReplica(t *testing.T) {
	roles := []bool{false, false, false, false, false}
	serversNumber := len(servers)

	allPorts := map[string]bool{
		servers[0]: true,
		servers[1]: true,
		servers[2]: true,
		servers[3]: true,
		servers[4]: true,
	}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// RO
	_, err = connPool.Eval("return box.cfg.listen", []interface{}{}, pool.RO)
	require.NotNilf(t, err, "expected to fail after Eval, but error is nil")
	require.Equal(t, "can't find ro instance in pool", err.Error())

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)
}

func TestRoundRobinStrategy_NoMaster(t *testing.T) {
	roles := []bool{true, true, true, true, true}
	serversNumber := len(servers)

	allPorts := map[string]bool{
		servers[0]: true,
		servers[1]: true,
		servers[2]: true,
		servers[3]: true,
		servers[4]: true,
	}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// RW
	_, err = connPool.Eval("return box.cfg.listen", []interface{}{}, pool.RW)
	require.NotNilf(t, err, "expected to fail after Eval, but error is nil")
	require.Equal(t, "can't find rw instance in pool", err.Error())

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)
}

func TestUpdateInstancesRoles(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	allPorts := map[string]bool{
		servers[0]: true,
		servers[1]: true,
		servers[2]: true,
		servers[3]: true,
		servers[4]: true,
	}

	masterPorts := map[string]bool{
		servers[0]: true,
		servers[2]: true,
		servers[3]: true,
	}

	replicaPorts := map[string]bool{
		servers[1]: true,
		servers[4]: true,
	}

	serversNumber := len(servers)

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	roles = []bool{true, false, true, true, false}

	masterPorts = map[string]bool{
		servers[1]: true,
		servers[4]: true,
	}

	replicaPorts = map[string]bool{
		servers[0]: true,
		servers[2]: true,
		servers[3]: true,
	}

	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	// ANY
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.RW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.RO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestInsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Insert(spaceName, []interface{}{"rw_insert_key", "rw_insert_value"})
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Insert")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "rw_insert_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "rw_insert_value", value, "unexpected body of Insert (1)")

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"rw_insert_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_insert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_insert_value", value, "unexpected body of Select (1)")

	// PreferRW
	resp, err = connPool.Insert(spaceName, []interface{}{"preferRW_insert_key", "preferRW_insert_value"})
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Insert")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "preferRW_insert_key", key, "unexpected body of Insert (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "preferRW_insert_value", value, "unexpected body of Insert (1)")

	sel = tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"preferRW_insert_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "preferRW_insert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "preferRW_insert_value", value, "unexpected body of Select (1)")
}

func TestDelete(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	ins := tarantool.NewInsertRequest(spaceNo).Tuple([]interface{}{"delete_key", "delete_value"})
	resp, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Insert")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "delete_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "delete_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Delete(spaceName, indexNo, []interface{}{"delete_key"})
	require.Nilf(t, err, "failed to Delete")
	require.NotNilf(t, resp, "response is nil after Delete")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Delete")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Delete")
	require.Equalf(t, 2, len(tpl), "unexpected body of Delete")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Delete (0)")
	require.Equalf(t, "delete_key", key, "unexpected body of Delete (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Delete (1)")
	require.Equalf(t, "delete_value", value, "unexpected body of Delete (1)")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"delete_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, 0, len(resp.Data), "response Body len != 0 after Select")
}

func TestUpsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Upsert(spaceName, []interface{}{"upsert_key", "upsert_value"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, resp, "response is nil after Upsert")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"upsert_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "upsert_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "upsert_value", value, "unexpected body of Select (1)")

	// PreferRW
	resp, err = connPool.Upsert(
		spaceName, []interface{}{"upsert_key", "upsert_value"},
		[]interface{}{[]interface{}{"=", 1, "new_value"}}, pool.PreferRW)

	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, resp, "response is nil after Upsert")

	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "upsert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")
}

func TestUpdate(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{"update_key", "update_value"})
	resp, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Insert")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "update_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Update(spaceName, indexNo, []interface{}{"update_key"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	require.Nilf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"update_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")

	// PreferRW
	resp, err = connPool.Update(
		spaceName, indexNo, []interface{}{"update_key"},
		[]interface{}{[]interface{}{"=", 1, "another_value"}}, pool.PreferRW)

	require.Nilf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "another_value", value, "unexpected body of Select (1)")
}

func TestReplace(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{"replace_key", "replace_value"})
	resp, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Insert")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "replace_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "replace_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err = connPool.Replace(spaceNo, []interface{}{"new_key", "new_value"})
	require.Nilf(t, err, "failed to Replace")
	require.NotNilf(t, resp, "response is nil after Replace")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"new_key"})
	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "new_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")

	// PreferRW
	resp, err = connPool.Replace(spaceNo, []interface{}{"new_key", "new_value"}, pool.PreferRW)
	require.Nilf(t, err, "failed to Replace")
	require.NotNilf(t, resp, "response is nil after Replace")

	resp, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "new_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")
}

func TestSelect(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	roServers := []string{servers[0], servers[1], servers[3]}
	rwServers := []string{servers[2], servers[4]}
	allServers := []string{servers[0], servers[1], servers[2], servers[3], servers[4]}

	roTpl := []interface{}{"ro_select_key", "ro_select_value"}
	rwTpl := []interface{}{"rw_select_key", "rw_select_value"}
	anyTpl := []interface{}{"any_select_key", "any_select_value"}

	roKey := []interface{}{"ro_select_key"}
	rwKey := []interface{}{"rw_select_key"}
	anyKey := []interface{}{"any_select_key"}

	err = test_helpers.InsertOnInstances(roServers, connOpts, spaceNo, roTpl)
	require.Nil(t, err)

	err = test_helpers.InsertOnInstances(rwServers, connOpts, spaceNo, rwTpl)
	require.Nil(t, err)

	err = test_helpers.InsertOnInstances(allServers, connOpts, spaceNo, anyTpl)
	require.Nil(t, err)

	//default: ANY
	resp, err := connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, anyKey)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok := resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "any_select_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "any_select_value", value, "unexpected body of Select (1)")

	// PreferRO
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, pool.PreferRO)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Select (0)")

	// PreferRW
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, pool.PreferRW)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_select_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_select_value", value, "unexpected body of Select (1)")

	// RO
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, pool.RO)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "ro_select_value", value, "unexpected body of Select (1)")

	// RW
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, pool.RW)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_select_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_select_value", value, "unexpected body of Select (1)")
}

func TestPing(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	// ANY
	resp, err := connPool.Ping(pool.ANY)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RW
	resp, err = connPool.Ping(pool.RW)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RO
	resp, err = connPool.Ping(pool.RO)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRW
	resp, err = connPool.Ping(pool.PreferRW)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRO
	resp, err = connPool.Ping(pool.PreferRO)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")
}

func TestDo(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	req := tarantool.NewPingRequest()
	// ANY
	resp, err := connPool.Do(req, pool.ANY).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RW
	resp, err = connPool.Do(req, pool.RW).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RO
	resp, err = connPool.Do(req, pool.RO).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRW
	resp, err = connPool.Do(req, pool.PreferRW).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRO
	resp, err = connPool.Do(req, pool.PreferRO).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	stmt, err := connPool.NewPrepared("SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;", pool.RO)
	require.Nilf(t, err, "fail to prepare statement: %v", err)

	if connPool.GetPoolInfo()[stmt.Conn.Addr()].ConnRole != pool.ReplicaRole {
		t.Errorf("wrong role for the statement's connection")
	}

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	resp, err := connPool.Do(executeReq.Args([]interface{}{1, "test"}), pool.ANY).Get()
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
	resp, err = connPool.Do(unprepareReq, pool.ANY).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
	}
	if resp.Code != tarantool.OkCode {
		t.Errorf("failed to unprepare prepared statement: code %d", resp.Code)
	}

	_, err = connPool.Do(unprepareReq, pool.ANY).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = connPool.Do(executeReq, pool.ANY).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")
}

func TestDoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool")

	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close(true)

	req := test_helpers.NewStrangerRequest()

	_, err = connPool.Do(req, pool.ANY).Get()
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

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Begin")
	require.NotNilf(t, resp, "response is nil after Begin")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Begin: wrong code returned")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"commit_key", "commit_value"})
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Insert: wrong code returned")

	// Connect to servers[2] to check if tuple
	// was inserted outside of stream on RW instance
	// before transaction commit
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"commit_key"})
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
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "commit_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "commit_value", value, "unexpected body of Select (1)")

	// Commit transaction
	req = tarantool.NewCommitRequest()
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Commit")
	require.NotNilf(t, resp, "response is nil after Commit")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Commit: wrong code returned")

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, len(resp.Data), 1, "response Body len != 1 after Select")

	tpl, ok = resp.Data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "commit_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "commit_value", value, "unexpected body of Select (1)")
}

func TestStream_Rollback(t *testing.T) {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Begin")
	require.NotNilf(t, resp, "response is nil after Begin")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Begin: wrong code returned")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"rollback_key", "rollback_value"})
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, resp, "response is nil after Insert")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Insert: wrong code returned")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"rollback_key"})
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
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rollback_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rollback_value", value, "unexpected body of Select (1)")

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	resp, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Rollback")
	require.NotNilf(t, resp, "response is nil after Rollback")
	require.Equalf(t, tarantool.OkCode, resp.Code, "failed to Rollback: wrong code returned")

	// Select outside of transaction
	resp, err = conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, 0, len(resp.Data), "response Body len != 0 after Select")

	// Select inside of stream after rollback
	resp, err = stream.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, 0, len(resp.Data), "response Body len != 0 after Select")
}

func TestStream_TxnIsolationLevel(t *testing.T) {
	var req tarantool.Request
	var resp *tarantool.Response
	var err error

	txnIsolationLevels := []tarantool.TxnIsolationLevel{
		tarantool.DefaultIsolationLevel,
		tarantool.ReadCommittedLevel,
		tarantool.ReadConfirmedLevel,
		tarantool.BestEffortLevel,
	}

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close(true)

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = tarantool.NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Begin")
		require.NotNilf(t, resp, "response is nil after Begin")
		require.Equalf(t, tarantool.OkCode, resp.Code, "wrong code returned")

		// Insert in stream
		req = tarantool.NewInsertRequest(spaceName).
			Tuple([]interface{}{"level_key", "level_value"})
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Insert")
		require.NotNilf(t, resp, "response is nil after Insert")
		require.Equalf(t, tarantool.OkCode, resp.Code, "wrong code returned")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := tarantool.NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(tarantool.IterEq).
			Key([]interface{}{"level_key"})
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
		require.Equalf(t, 2, len(tpl), "unexpected body of Select")

		key, ok := tpl[0].(string)
		require.Truef(t, ok, "unexpected body of Select (0)")
		require.Equalf(t, "level_key", key, "unexpected body of Select (0)")

		value, ok := tpl[1].(string)
		require.Truef(t, ok, "unexpected body of Select (1)")
		require.Equalf(t, "level_value", value, "unexpected body of Select (1)")

		// Rollback transaction
		req = tarantool.NewRollbackRequest()
		resp, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Rollback")
		require.NotNilf(t, resp, "response is nil after Rollback")
		require.Equalf(t, tarantool.OkCode, resp.Code, "wrong code returned")

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

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{"level_key"})
	}
}

func TestConnectionPool_NewWatcher_noWatchersFeature(t *testing.T) {
	const key = "TestConnectionPool_NewWatcher_noWatchersFeature"

	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	watcher, err := connPool.NewWatcher(key,
		func(event tarantool.WatchEvent) {}, pool.ANY)
	require.Nilf(t, watcher, "watcher must not be created")
	require.NotNilf(t, err, "an error is expected")
	expected := "the feature WatchersFeature must be required by connection " +
		"options to create a watcher"
	require.Equal(t, expected, err.Error())
}

func TestConnectionPool_NewWatcher_modes(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnectionPool_NewWatcher_modes"

	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	modes := []pool.Mode{
		pool.ANY,
		pool.RW,
		pool.RO,
		pool.PreferRW,
		pool.PreferRO,
	}
	for _, mode := range modes {
		t.Run(fmt.Sprintf("%d", mode), func(t *testing.T) {
			expectedServers := []string{}
			for i, server := range servers {
				if roles[i] && mode == pool.RW {
					continue
				} else if !roles[i] && mode == pool.RO {
					continue
				}
				expectedServers = append(expectedServers, server)
			}

			events := make(chan tarantool.WatchEvent, 1024)
			defer close(events)

			watcher, err := connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
				require.Equal(t, key, event.Key)
				require.Equal(t, nil, event.Value)
				events <- event
			}, mode)
			require.Nilf(t, err, "failed to register a watcher")
			defer watcher.Unregister()

			testMap := make(map[string]int)

			for i := 0; i < len(expectedServers); i++ {
				select {
				case event := <-events:
					require.NotNil(t, event.Conn)
					addr := event.Conn.Addr()
					if val, ok := testMap[addr]; ok {
						testMap[addr] = val + 1
					} else {
						testMap[addr] = 1
					}
				case <-time.After(time.Second):
					t.Errorf("Failed to get a watch event.")
					break
				}
			}

			for _, server := range expectedServers {
				if val, ok := testMap[server]; !ok {
					t.Errorf("Server not found: %s", server)
				} else if val != 1 {
					t.Errorf("Too many events %d for server %s", val, server)
				}
			}
		})
	}
}

func TestConnectionPool_NewWatcher_update(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnectionPool_NewWatcher_update"
	const mode = pool.RW
	const initCnt = 2
	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	poolOpts := pool.Opts{
		CheckTimeout: 500 * time.Millisecond,
	}
	pool, err := pool.ConnectWithOpts(servers, opts, poolOpts)

	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer pool.Close(true)

	events := make(chan tarantool.WatchEvent, 1024)
	defer close(events)

	watcher, err := pool.NewWatcher(key, func(event tarantool.WatchEvent) {
		require.Equal(t, key, event.Key)
		require.Equal(t, nil, event.Value)
		events <- event
	}, mode)
	require.Nilf(t, err, "failed to create a watcher")
	defer watcher.Unregister()

	// Wait for all initial events.
	testMap := make(map[string]int)
	for i := 0; i < initCnt; i++ {
		select {
		case event := <-events:
			require.NotNil(t, event.Conn)
			addr := event.Conn.Addr()
			if val, ok := testMap[addr]; ok {
				testMap[addr] = val + 1
			} else {
				testMap[addr] = 1
			}
		case <-time.After(poolOpts.CheckTimeout * 2):
			t.Errorf("Failed to get a watch init event.")
			break
		}
	}

	// Just invert roles for simplify the test.
	for i, role := range roles {
		roles[i] = !role
	}
	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	// Wait for all updated events.
	for i := 0; i < len(servers)-initCnt; i++ {
		select {
		case event := <-events:
			require.NotNil(t, event.Conn)
			addr := event.Conn.Addr()
			if val, ok := testMap[addr]; ok {
				testMap[addr] = val + 1
			} else {
				testMap[addr] = 1
			}
		case <-time.After(time.Second):
			t.Errorf("Failed to get a watch update event.")
			break
		}
	}

	// Check that all an event happen for an each connection.
	for _, server := range servers {
		if val, ok := testMap[server]; !ok {
			t.Errorf("Server not found: %s", server)
		} else {
			require.Equal(t, val, 1, fmt.Sprintf("for server %s", server))
		}
	}
}

func TestWatcher_Unregister(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestWatcher_Unregister"
	const mode = pool.RW
	const expectedCnt = 2
	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	pool, err := pool.Connect(servers, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer pool.Close(true)

	events := make(chan tarantool.WatchEvent, 1024)
	defer close(events)

	watcher, err := pool.NewWatcher(key, func(event tarantool.WatchEvent) {
		require.Equal(t, key, event.Key)
		require.Equal(t, nil, event.Value)
		events <- event
	}, mode)
	require.Nilf(t, err, "failed to create a watcher")

	for i := 0; i < expectedCnt; i++ {
		select {
		case <-events:
		case <-time.After(time.Second):
			t.Fatalf("Failed to skip initial events.")
		}
	}
	watcher.Unregister()

	broadcast := tarantool.NewBroadcastRequest(key).Value("foo")
	for i := 0; i < expectedCnt; i++ {
		_, err := pool.Do(broadcast, mode).Get()
		require.Nilf(t, err, "failed to send a broadcast request")
	}

	select {
	case event := <-events:
		t.Fatalf("Get unexpected event: %v", event)
	case <-time.After(time.Second):
	}

	// Reset to the initial state.
	broadcast = tarantool.NewBroadcastRequest(key)
	for i := 0; i < expectedCnt; i++ {
		_, err := pool.Do(broadcast, mode).Get()
		require.Nilf(t, err, "failed to send a broadcast request")
	}
}

func TestConnectionPool_NewWatcher_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestConnectionPool_NewWatcher_concurrent"

	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	mode := pool.ANY
	callback := func(event tarantool.WatchEvent) {}
	for i := 0; i < testConcurrency; i++ {
		go func(i int) {
			defer wg.Done()

			watcher, err := connPool.NewWatcher(key, callback, mode)
			if err != nil {
				t.Errorf("Failed to create a watcher: %s", err)
			} else {
				watcher.Unregister()
			}
		}(i)
	}
	wg.Wait()
}

func TestWatcher_Unregister_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestWatcher_Unregister_concurrent"

	roles := []bool{true, false, false, true, true}

	opts := connOpts.Clone()
	opts.RequiredProtocolInfo.Features = []tarantool.ProtocolFeature{
		tarantool.WatchersFeature,
	}
	err := test_helpers.SetClusterRO(servers, opts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := pool.Connect(servers, opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close(true)

	mode := pool.ANY
	watcher, err := connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
	}, mode)
	require.Nilf(t, err, "failed to create a watcher")

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

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	initScript := "config.lua"
	waitStart := 100 * time.Millisecond
	connectRetry := 10
	retryTimeout := 500 * time.Millisecond

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isStreamUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Could not check the Tarantool version")
	}

	instances, err = test_helpers.StartTarantoolInstances(servers, nil, test_helpers.StartOpts{
		InitScript:         initScript,
		User:               connOpts.User,
		Pass:               connOpts.Pass,
		WaitStart:          waitStart,
		ConnectRetry:       connectRetry,
		RetryTimeout:       retryTimeout,
		MemtxUseMvccEngine: !isStreamUnsupported,
	})

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
		return -1
	}

	defer test_helpers.StopTarantoolInstances(instances)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
