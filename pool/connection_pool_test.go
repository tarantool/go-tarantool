package pool_test

import (
	"context"
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
	"github.com/vmihailenco/msgpack/v5"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var user = "test"
var pass = "test"
var spaceNo = uint32(520)
var spaceName = "testPool"
var indexNo = uint32(0)

var ports = []string{"3013", "3014", "3015", "3016", "3017"}
var host = "127.0.0.1"

type DialersMap = map[string]tarantool.Dialer

var servers = []string{
	strings.Join([]string{host, ports[0]}, ":"),
	strings.Join([]string{host, ports[1]}, ":"),
	strings.Join([]string{host, ports[2]}, ":"),
	strings.Join([]string{host, ports[3]}, ":"),
	strings.Join([]string{host, ports[4]}, ":"),
}

func makeDialer(serv string) tarantool.Dialer {
	return tarantool.NetDialer{
		Address:  serv,
		User:     user,
		Password: pass,
	}
}

func makeDialers(servers []string) []tarantool.Dialer {
	dialers := make([]tarantool.Dialer, 0, len(servers))
	for _, serv := range servers {
		dialers = append(dialers, makeDialer(serv))
	}
	return dialers
}

func makeDialersMap(servers []string) DialersMap {
	dialersMap := DialersMap{}
	for _, serv := range servers {
		dialersMap[serv] = makeDialer(serv)
	}
	return dialersMap
}

var dialers = makeDialers(servers)
var dialersMap = makeDialersMap(servers)

var connOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

var defaultCountRetry = 5
var defaultTimeoutRetry = 500 * time.Millisecond

var instances []test_helpers.TarantoolInstance

func TestConnError_IncorrectParams(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, DialersMap{}, tarantool.Opts{})
	cancel()
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "dialers (second argument) should not be empty", err.Error())

	ctx, cancel = test_helpers.GetPoolConnectContext()
	connPool, err = pool.Connect(ctx, DialersMap{
		"err1": tarantool.NetDialer{Address: "err1"},
		"err2": tarantool.NetDialer{Address: "err2"},
	}, connOpts)
	cancel()
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "no active connections", err.Error())

	ctx, cancel = test_helpers.GetPoolConnectContext()
	connPool, err = pool.ConnectWithOpts(ctx, dialersMap, tarantool.Opts{}, pool.Opts{})
	cancel()
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "wrong check timeout, must be greater than 0", err.Error())
}

func TestConnSuccessfully(t *testing.T) {
	healthyServ := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{healthyServ, "err"}), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{healthyServ},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			healthyServ: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)
}

func TestConnErrorAfterCtxCancel(t *testing.T) {
	var connLongReconnectOpts = tarantool.Opts{
		Timeout:       5 * time.Second,
		Reconnect:     time.Second,
		MaxReconnects: 100,
	}

	ctx, cancel := context.WithCancel(context.Background())

	var connPool *pool.ConnectionPool
	var err error

	cancel()
	connPool, err = pool.Connect(ctx, dialersMap, connLongReconnectOpts)

	if connPool != nil || err == nil {
		t.Fatalf("ConnectionPool was created after cancel")
	}
	if !strings.Contains(err.Error(), "operation was canceled") {
		t.Fatalf("Unexpected error, expected to contain %s, got %v",
			"operation was canceled", err)
	}
}

type mockClosingDialer struct {
	addr      string
	cnt       *int
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func (m *mockClosingDialer) Dial(ctx context.Context,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	dialer := tarantool.NetDialer{
		Address:  m.addr,
		User:     user,
		Password: pass,
	}
	conn, err := dialer.Dial(m.ctx, tarantool.DialOpts{})

	if *m.cnt == 0 {
		m.ctxCancel()
	}
	*m.cnt++

	return conn, err
}

func TestContextCancelInProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cnt := new(int)
	poolDialers := DialersMap{}
	for _, serv := range servers {
		poolDialers[serv] = &mockClosingDialer{
			addr:      serv,
			cnt:       cnt,
			ctx:       ctx,
			ctxCancel: cancel,
		}
	}

	connPool, err := pool.Connect(ctx, poolDialers, tarantool.Opts{})
	require.NotNilf(t, err, "expected err after ctx cancel")
	assert.Truef(t, strings.Contains(err.Error(), "operation was canceled"),
		fmt.Sprintf("unexpected error, expected to contain %s, got %v",
			"operation was canceled", err))
	require.Nilf(t, connPool, "conn is not nil after ctx cancel")
}

func TestConnSuccessfullyDuplicates(t *testing.T) {
	server := servers[0]

	poolDialers := DialersMap{}
	for i := 0; i < 4; i++ {
		poolDialers[fmt.Sprintf("c%d", i)] = tarantool.NetDialer{
			Address:  server,
			User:     user,
			Password: pass,
		}
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, poolDialers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            []string{"c0", "c1", "c2", "c3"},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			"c0": true,
			"c1": true,
			"c2": true,
			"c3": true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)
}

func TestReconnect(t *testing.T) {
	server := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestDisconnect_withReconnect(t *testing.T) {
	serverId := 0
	server := servers[serverId]

	opts := connOpts
	opts.Reconnect = 10 * time.Second

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server}), opts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server1, server2}), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:1]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	for _, server := range servers[1:] {
		ctx, cancel := test_helpers.GetConnectContext()
		err = connPool.Add(ctx, server, dialersMap[server])
		cancel()
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
	server := servers[0]
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server}), connOpts)
	cancel()
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	ctx, cancel = test_helpers.GetConnectContext()
	err = connPool.Add(ctx, server, makeDialer(server))
	cancel()
	require.Equal(t, pool.ErrExists, err)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd_unreachable(t *testing.T) {
	server := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server}), connOpts)
	cancel()
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	unhealthyServ := "127.0.0.2:6667"
	ctx, cancel = test_helpers.GetConnectContext()
	err = connPool.Add(ctx, unhealthyServ, tarantool.NetDialer{
		Address: unhealthyServ,
	})
	cancel()
	// The OS-dependent error so we just check for existence.
	require.NotNil(t, err)

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               pool.ANY,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestAdd_afterClose(t *testing.T) {
	server := servers[0]
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server}), connOpts)
	cancel()
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	connPool.Close()
	ctx, cancel = test_helpers.GetConnectContext()
	err = connPool.Add(ctx, server, dialersMap[server])
	cancel()
	assert.Equal(t, err, pool.ErrClosed)
}

func TestAdd_Close_concurrent(t *testing.T) {
	serv0 := servers[0]
	serv1 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{serv0}), connOpts)
	cancel()
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, cancel := test_helpers.GetConnectContext()
		err = connPool.Add(ctx, serv1, makeDialer(serv1))
		cancel()
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	connPool.Close()

	wg.Wait()
}

func TestAdd_CloseGraceful_concurrent(t *testing.T) {
	serv0 := servers[0]
	serv1 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{serv0}), connOpts)
	cancel()
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, cancel := test_helpers.GetConnectContext()
		err = connPool.Add(ctx, serv1, makeDialer(serv1))
		cancel()
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	connPool.CloseGraceful()

	wg.Wait()
}

func TestRemove(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:2]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:2]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:2]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:2]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Remove(servers[1])
		assert.Nil(t, err)
	}()

	connPool.Close()

	wg.Wait()
}

func TestRemove_CloseGraceful_concurrent(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap(servers[:2]), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Remove(servers[1])
		assert.Nil(t, err)
	}()

	connPool.CloseGraceful()

	wg.Wait()
}

func TestClose(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server1, server2}), connOpts)
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

	connPool.Close()

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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestCloseGraceful(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server1, server2}), connOpts)
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
		connPool.CloseGraceful()
	}()

	// Check that a request rejected if graceful shutdown in progress.
	time.Sleep((time.Duration(evalSleep) * time.Second) / 2)
	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ANY).Get()
	require.ErrorContains(t, err, "can't find healthy instance in pool")

	// Check that a previous request was successful.
	_, err = fut.Get()
	require.Nilf(t, err, "sleep request no error")

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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

type testHandler struct {
	discovered, deactivated uint32
	errs                    []error
}

func (h *testHandler) addErr(err error) {
	h.errs = append(h.errs, err)
}

func (h *testHandler) Discovered(id string, conn *tarantool.Connection,

	role pool.Role) error {
	discovered := atomic.AddUint32(&h.discovered, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("discovered conn == nil"))
		return nil
	}

	// discovered < 3 - initial open of connections
	// discovered >= 3 - update a connection after a role update
	if id == servers[0] {
		if discovered < 3 && role != pool.MasterRole {
			h.addErr(fmt.Errorf("unexpected init role %d for id %s", role, id))
		}
		if discovered >= 3 && role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected updated role %d for id %s", role, id))
		}
	} else if id == servers[1] {
		if discovered >= 3 {
			h.addErr(fmt.Errorf("unexpected discovery for id %s", id))
		}
		if role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected role %d for id %s", role, id))
		}
	} else {
		h.addErr(fmt.Errorf("unexpected discovered id %s", id))
	}

	return nil
}

func (h *testHandler) Deactivated(id string, conn *tarantool.Connection,

	role pool.Role) error {
	deactivated := atomic.AddUint32(&h.deactivated, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("removed conn == nil"))
		return nil
	}

	if deactivated == 1 && id == servers[0] {
		// A first close is a role update.
		if role != pool.MasterRole {
			h.addErr(fmt.Errorf("unexpected removed role %d for id %s", role, id))
		}
		return nil
	}

	if id == servers[0] || id == servers[1] {
		// Close.
		if role != pool.ReplicaRole {
			h.addErr(fmt.Errorf("unexpected removed role %d for id %s", role, id))
		}
	} else {
		h.addErr(fmt.Errorf("unexpected removed id %s", id))
	}

	return nil
}

func TestConnectionHandlerOpenUpdateClose(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	poolDialers := makeDialersMap(poolServers)
	roles := []bool{false, true}

	err := test_helpers.SetClusterRO(makeDialers(poolServers), connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	h := &testHandler{}
	poolOpts := pool.Opts{
		CheckTimeout:      100 * time.Microsecond,
		ConnectionHandler: h,
	}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.ConnectWithOpts(ctx, poolDialers, connOpts, poolOpts)
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

	connPool.Close()

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

func (h *testAddErrorHandler) Discovered(id string, conn *tarantool.Connection,

	role pool.Role) error {
	h.discovered++
	return fmt.Errorf("any error")
}

func (h *testAddErrorHandler) Deactivated(id string, conn *tarantool.Connection,

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
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.ConnectWithOpts(ctx, makeDialersMap(poolServers), connOpts, poolOpts)
	if err == nil {
		defer connPool.Close()
	}
	require.NotNilf(t, err, "success to connect")
	require.Equalf(t, 2, h.discovered, "unexpected discovered count")
	require.Equalf(t, 0, h.deactivated, "unexpected deactivated count")
}

type testUpdateErrorHandler struct {
	discovered, deactivated uint32
}

func (h *testUpdateErrorHandler) Discovered(id string, conn *tarantool.Connection,

	role pool.Role) error {
	atomic.AddUint32(&h.discovered, 1)

	if atomic.LoadUint32(&h.deactivated) != 0 {
		// Don't add a connection into a pool again after it was deleted.
		return fmt.Errorf("any error")
	}
	return nil
}

func (h *testUpdateErrorHandler) Deactivated(id string, conn *tarantool.Connection,

	role pool.Role) error {
	atomic.AddUint32(&h.deactivated, 1)
	return nil
}

func TestConnectionHandlerUpdateError(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	poolDialers := makeDialersMap(poolServers)
	roles := []bool{false, false}

	err := test_helpers.SetClusterRO(makeDialers(poolServers), connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	h := &testUpdateErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout:      100 * time.Microsecond,
		ConnectionHandler: h,
	}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.ConnectWithOpts(ctx, poolDialers, connOpts, poolOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

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

	connPool.Close()

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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, makeDialersMap([]string{server1, server2}), connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	_, err = connPool.Ping(pool.ANY)
	require.NotNilf(t, err, "err is nil after Ping")

	err = test_helpers.RestartTarantool(&instances[0])
	require.Nilf(t, err, "failed to restart tarantool")

	err = test_helpers.RestartTarantool(&instances[1])
	require.Nilf(t, err, "failed to restart tarantool")
}

func TestCall(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	data, err := connPool.Call("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val := data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	data, err = connPool.Call("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	data, err = connPool.Call("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	data, err = connPool.Call("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestCall16(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	data, err := connPool.Call16("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val := data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	data, err = connPool.Call16("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	data, err = connPool.Call16("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	data, err = connPool.Call16("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].([]interface{})[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestCall17(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	data, err := connPool.Call17("box.info", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val := data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	data, err = connPool.Call17("box.info", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	data, err = connPool.Call17("box.info", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	data, err = connPool.Call17("box.info", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, data, "response is nil after Call")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Call")

	val = data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}

func TestEval(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	data, err := connPool.Eval("return box.info().ro", []interface{}{}, pool.PreferRO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, data, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Eval")

	val, ok := data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, val, "expected `true` with mode `PreferRO`")

	// PreferRW
	data, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.PreferRW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, data, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Eval")

	val, ok = data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, val, "expected `false` with mode `PreferRW`")

	// RO
	data, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.RO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, data, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Eval")

	val, ok = data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, val, "expected `true` with mode `RO`")

	// RW
	data, err = connPool.Eval("return box.info().ro", []interface{}{}, pool.RW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, data, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Eval")

	val, ok = data[0].(bool)
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	request := "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0 == 1;"
	// Execute
	data, err := connPool.Execute(request, []interface{}{}, pool.ANY)
	require.Nilf(t, err, "failed to Execute")
	require.NotNilf(t, data, "response is nil after Execute")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Execute")
	require.Equalf(t, len(data[0].([]interface{})), 2, "unexpected response")

	// ExecuteTyped
	mem := []Member{}
	info, _, err := connPool.ExecuteTyped(request, []interface{}{}, &mem, pool.ANY)
	require.Nilf(t, err, "failed to ExecuteTyped")
	require.Equalf(t, info.AffectedCount, uint64(0), "unexpected info.AffectedCount")
	require.Equalf(t, len(mem), 1, "wrong count of results")

	// ExecuteAsync
	fut := connPool.ExecuteAsync(request, []interface{}{}, pool.ANY)
	data, err = fut.Get()
	require.Nilf(t, err, "failed to ExecuteAsync")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after ExecuteAsync")
	require.Equalf(t, len(data[0].([]interface{})), 2, "unexpected response")
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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

	err = test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	// ANY
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          pool.ANY,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.RW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.RO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          pool.PreferRO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args,
		defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestInsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	data, err := connPool.Insert(spaceName, []interface{}{"rw_insert_key", "rw_insert_value"})
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, data, "response is nil after Insert")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]interface{})
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
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"rw_insert_key"})
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_insert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_insert_value", value, "unexpected body of Select (1)")

	// PreferRW
	data, err = connPool.Insert(spaceName,
		[]interface{}{"preferRW_insert_key", "preferRW_insert_value"})
	require.Nilf(t, err, "failed to Insert")
	require.NotNilf(t, data, "response is nil after Insert")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Insert")

	tpl, ok = data[0].([]interface{})
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
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	ins := tarantool.NewInsertRequest(spaceNo).Tuple([]interface{}{"delete_key", "delete_value"})
	data, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "delete_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "delete_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	data, err = connPool.Delete(spaceName, indexNo, []interface{}{"delete_key"})
	require.Nilf(t, err, "failed to Delete")
	require.NotNilf(t, data, "response is nil after Delete")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Delete")

	tpl, ok = data[0].([]interface{})
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
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 0, len(data), "response Body len != 0 after Select")
}

func TestUpsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	data, err := connPool.Upsert(spaceName,
		[]interface{}{"upsert_key", "upsert_value"},
		tarantool.NewOperations().Assign(1, "new_value"))
	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, data, "response is nil after Upsert")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"upsert_key"})
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok := data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "upsert_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "upsert_value", value, "unexpected body of Select (1)")

	// PreferRW
	data, err = connPool.Upsert(
		spaceName, []interface{}{"upsert_key", "upsert_value"},
		tarantool.NewOperations().Assign(1, "new_value"), pool.PreferRW)

	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, data, "response is nil after Upsert")

	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{"update_key", "update_value"})
	data, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "update_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Update(spaceName, indexNo,
		[]interface{}{"update_key"}, tarantool.NewOperations().Assign(1, "new_value"))
	require.Nilf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"update_key"})
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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
		tarantool.NewOperations().Assign(1, "another_value"), pool.PreferRW)

	require.Nilf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]interface{}{"replace_key", "replace_value"})
	data, err := conn.Do(ins).Get()
	require.Nilf(t, err, "failed to Insert")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Insert")
	require.Equalf(t, 2, len(tpl), "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "replace_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "replace_value", value, "unexpected body of Insert (1)")

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Replace(spaceNo, []interface{}{"new_key", "new_value"})
	require.Nilf(t, err, "failed to Replace")
	require.NotNilf(t, resp, "response is nil after Replace")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"new_key"})
	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	data, err = conn.Do(sel).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	roServers := []string{servers[0], servers[1], servers[3]}
	rwServers := []string{servers[2], servers[4]}
	allServers := []string{servers[0], servers[1], servers[2], servers[3], servers[4]}

	roTpl := []interface{}{"ro_select_key", "ro_select_value"}
	rwTpl := []interface{}{"rw_select_key", "rw_select_value"}
	anyTpl := []interface{}{"any_select_key", "any_select_value"}

	roKey := []interface{}{"ro_select_key"}
	rwKey := []interface{}{"rw_select_key"}
	anyKey := []interface{}{"any_select_key"}

	err = test_helpers.InsertOnInstances(makeDialers(roServers), connOpts, spaceNo, roTpl)
	require.Nil(t, err)

	err = test_helpers.InsertOnInstances(makeDialers(rwServers), connOpts, spaceNo, rwTpl)
	require.Nil(t, err)

	err = test_helpers.InsertOnInstances(makeDialers(allServers), connOpts, spaceNo, anyTpl)
	require.Nil(t, err)

	//default: ANY
	data, err := connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, anyKey)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, data, "response is nil after Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok := data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "any_select_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "any_select_value", value, "unexpected body of Select (1)")

	// PreferRO
	data, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, pool.PreferRO)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, data, "response is nil after Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Select (0)")

	// PreferRW
	data, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, pool.PreferRW)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, data, "response is nil after Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_select_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_select_value", value, "unexpected body of Select (1)")

	// RO
	data, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, pool.RO)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, data, "response is nil after Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
	require.Truef(t, ok, "unexpected body of Select")
	require.Equalf(t, 2, len(tpl), "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "ro_select_value", value, "unexpected body of Select (1)")

	// RW
	data, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, pool.RW)
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, data, "response is nil after Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// ANY
	data, err := connPool.Ping(pool.ANY)
	require.Nilf(t, err, "failed to Ping")
	require.Nilf(t, data, "response data is not nil after Ping")

	// RW
	data, err = connPool.Ping(pool.RW)
	require.Nilf(t, err, "failed to Ping")
	require.Nilf(t, data, "response data is not nil after Ping")

	// RO
	data, err = connPool.Ping(pool.RO)
	require.Nilf(t, err, "failed to Ping")
	require.Nilf(t, data, "response data is not nil after Ping")

	// PreferRW
	data, err = connPool.Ping(pool.PreferRW)
	require.Nilf(t, err, "failed to Ping")
	require.Nilf(t, data, "response data is not nil after Ping")

	// PreferRO
	data, err = connPool.Ping(pool.PreferRO)
	require.Nilf(t, err, "failed to Ping")
	require.Nilf(t, data, "response data is not nil after Ping")
}

func TestDo(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	req := tarantool.NewPingRequest()
	// ANY
	_, err = connPool.Do(req, pool.ANY).Get()
	require.Nilf(t, err, "failed to Ping")

	// RW
	_, err = connPool.Do(req, pool.RW).Get()
	require.Nilf(t, err, "failed to Ping")

	// RO
	_, err = connPool.Do(req, pool.RO).Get()
	require.Nilf(t, err, "failed to Ping")

	// PreferRW
	_, err = connPool.Do(req, pool.PreferRW).Get()
	require.Nilf(t, err, "failed to Ping")

	// PreferRO
	_, err = connPool.Do(req, pool.PreferRO).Get()
	require.Nilf(t, err, "failed to Ping")
}

func TestDo_concurrent(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	req := tarantool.NewPingRequest()
	const concurrency = 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			_, err := connPool.Do(req, pool.ANY).Get()
			assert.Nil(t, err)
		}()
	}

	wg.Wait()
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	stmt, err := connPool.NewPrepared(
		"SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;", pool.RO)
	require.Nilf(t, err, "fail to prepare statement: %v", err)

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	resp, err := connPool.Do(executeReq.Args([]interface{}{1, "test"}), pool.ANY).GetResponse()
	if err != nil {
		t.Fatalf("failed to execute prepared: %v", err)
	}
	if resp == nil {
		t.Fatalf("nil response")
	}
	data, err := resp.Decode()
	if err != nil {
		t.Fatalf("failed to Decode: %s", err.Error())
	}
	if reflect.DeepEqual(data[0], []interface{}{1, "test"}) {
		t.Error("Select with named arguments failed")
	}
	prepResp, ok := resp.(*tarantool.ExecuteResponse)
	if !ok {
		t.Fatalf("Not a Prepare response")
	}
	metaData, err := prepResp.MetaData()
	if err != nil {
		t.Errorf("Error while getting MetaData: %s", err.Error())
	}
	if metaData[0].FieldType != "unsigned" ||
		metaData[0].FieldName != "NAME0" ||
		metaData[1].FieldType != "string" ||
		metaData[1].FieldName != "NAME1" {
		t.Error("Wrong metadata")
	}

	// the second argument for unprepare request is unused - it already belongs to some connection
	_, err = connPool.Do(unprepareReq, pool.ANY).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
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
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to " +
		"the current connection pool")

	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Begin")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"commit_key", "commit_value"})
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Insert")

	// Connect to servers[2] to check if tuple
	// was inserted outside of stream on RW instance
	// before transaction commit
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"commit_key"})
	data, err := conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 0, len(data), "response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 1, len(data), "response Body len != 1 after Select")

	tpl, ok := data[0].([]interface{})
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
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Commit")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, len(data), 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]interface{})
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
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Begin")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]interface{}{"rollback_key", "rollback_value"})
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Insert")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]interface{}{"rollback_key"})
	data, err := conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 0, len(data), "response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 1, len(data), "response Body len != 1 after Select")

	tpl, ok := data[0].([]interface{})
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
	_, err = stream.Do(req).Get()
	require.Nilf(t, err, "failed to Rollback")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 0, len(data), "response Body len != 0 after Select")

	// Select inside of stream after rollback
	data, err = stream.Do(selectReq).Get()
	require.Nilf(t, err, "failed to Select")
	require.Equalf(t, 0, len(data), "response Body len != 0 after Select")
}

func TestStream_TxnIsolationLevel(t *testing.T) {
	var req tarantool.Request
	var err error

	txnIsolationLevels := []tarantool.TxnIsolationLevel{
		tarantool.DefaultIsolationLevel,
		tarantool.ReadCommittedLevel,
		tarantool.ReadConfirmedLevel,
		tarantool.BestEffortLevel,
	}

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	err = test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on RW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer conn.Close()

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = tarantool.NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		_, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Begin")

		// Insert in stream
		req = tarantool.NewInsertRequest(spaceName).
			Tuple([]interface{}{"level_key", "level_value"})
		_, err = stream.Do(req).Get()
		require.Nilf(t, err, "failed to Insert")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := tarantool.NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(tarantool.IterEq).
			Key([]interface{}{"level_key"})
		data, err := conn.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 0, len(data), "response Data len != 0")

		// Select in stream
		data, err = stream.Do(selectReq).Get()
		require.Nilf(t, err, "failed to Select")
		require.Equalf(t, 1, len(data), "response Body len != 1 after Select")

		tpl, ok := data[0].([]interface{})
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

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []interface{}{"level_key"})
	}
}

func TestConnectionPool_NewWatcher_no_watchers(t *testing.T) {
	test_helpers.SkipIfWatchersSupported(t)

	const key = "TestConnectionPool_NewWatcher_no_watchers"

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nill after Connect")
	defer connPool.Close()

	ch := make(chan struct{})
	connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
		close(ch)
	}, pool.ANY)

	select {
	case <-time.After(time.Second):
		break
	case <-ch:
		t.Fatalf("watcher was created for connection that doesn't support it")
	}
}

func TestConnectionPool_NewWatcher_modes(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestConnectionPool_NewWatcher_modes"

	roles := []bool{true, false, false, true, true}

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

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
					addr := event.Conn.Addr().String()
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	poolOpts := pool.Opts{
		CheckTimeout: 500 * time.Millisecond,
	}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	pool, err := pool.ConnectWithOpts(ctx, dialersMap, connOpts, poolOpts)

	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer pool.Close()

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
			addr := event.Conn.Addr().String()
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
	err = test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	// Wait for all updated events.
	for i := 0; i < len(servers)-initCnt; i++ {
		select {
		case event := <-events:
			require.NotNil(t, event.Conn)
			addr := event.Conn.Addr().String()
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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	pool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer pool.Close()

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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

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

	err := test_helpers.SetClusterRO(dialers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.Connect(ctx, dialersMap, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

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
		log.Fatalf("Could not check the Tarantool version: %s", err)
	}

	instsOpts := make([]test_helpers.StartOpts, 0, len(servers))
	for _, serv := range servers {
		instsOpts = append(instsOpts, test_helpers.StartOpts{
			Listen: serv,
			Dialer: tarantool.NetDialer{
				Address:  serv,
				User:     user,
				Password: pass,
			},
			InitScript:         initScript,
			WaitStart:          waitStart,
			ConnectRetry:       connectRetry,
			RetryTimeout:       retryTimeout,
			MemtxUseMvccEngine: !isStreamUnsupported,
		})
	}

	instances, err = test_helpers.StartTarantoolInstances(instsOpts)

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
