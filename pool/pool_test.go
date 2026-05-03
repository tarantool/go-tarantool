package pool_test

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

var user = "test"
var userNoExec = "test_noexec"
var pass = "test"
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

func makeDialer(server string) tarantool.Dialer {
	return tarantool.NetDialer{
		Address:  server,
		User:     user,
		Password: pass,
	}
}

func makeDialers(servers []string) []tarantool.Dialer {
	dialers := make([]tarantool.Dialer, 0, len(servers))
	for _, server := range servers {
		dialers = append(dialers, makeDialer(server))
	}
	return dialers
}

var dialers = makeDialers(servers)

func makeInstance(server string, opts tarantool.Opts) pool.Instance {
	return pool.Instance{
		Name: server,
		Dialer: tarantool.NetDialer{
			Address:  server,
			User:     user,
			Password: pass,
		},
		Opts: opts,
	}
}

func makeNoExecuteInstance(server string, opts tarantool.Opts) pool.Instance {
	return pool.Instance{
		Name: server,
		Dialer: tarantool.NetDialer{
			Address:  server,
			User:     userNoExec,
			Password: pass,
		},
		Opts: opts,
	}
}

func makeInstances(servers []string, opts tarantool.Opts) []pool.Instance {
	var instances []pool.Instance
	for _, server := range servers {
		instances = append(instances, makeInstance(server, opts))
	}
	return instances
}

var instances = makeInstances(servers, connOpts)
var connOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

const defaultCountRetry = 5

const tick = 500 * time.Millisecond
const timeout = defaultCountRetry * tick

var helpInstances []*test_helpers.TarantoolInstance

func TestNew_error_duplicate(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{"foo", "foo"}, connOpts))
	cancel()

	assert.Nil(t, connPool)
	require.EqualError(t, err, "duplicate instance name: \"foo\"")
}

func TestNew_error_reconnect(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Reconnect = time.Second

	connPool, err := pool.New(ctx, makeInstances([]string{"any"}, opts))

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
}

func TestNew_error_max_reconnects(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.MaxReconnects = 3

	connPool, err := pool.New(ctx, makeInstances([]string{"any"}, opts))

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestNew_error_notify(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Notify = make(chan tarantool.ConnEvent)

	connPool, err := pool.New(ctx, makeInstances([]string{"any"}, opts))

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestNew_error_multiple_opts(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Reconnect = time.Second
	opts.MaxReconnects = 3
	opts.Notify = make(chan tarantool.ConnEvent)

	connPool, err := pool.New(ctx, makeInstances([]string{"any"}, opts))

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestNew_error_multiple_instances(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	badOpts1 := connOpts
	badOpts1.Reconnect = time.Second
	badOpts2 := connOpts
	badOpts2.MaxReconnects = 3
	instances := []pool.Instance{
		makeInstance("bad1", badOpts1),
		makeInstance("bad2", badOpts2),
	}

	connPool, err := pool.New(ctx, instances)

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestNewWithOpts_error_no_timeout(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.NewWithOpts(ctx, makeInstances([]string{"any"}, connOpts),
		pool.Opts{})
	cancel()
	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrWrongCheckTimeout)
}

func TestConnectWithOpts_error_reconnect(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Reconnect = time.Second

	connPool, err := pool.NewWithOpts(ctx, makeInstances([]string{"any"}, opts),
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
}

func TestConnectWithOpts_error_max_reconnects(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.MaxReconnects = 3

	connPool, err := pool.NewWithOpts(ctx, makeInstances([]string{"any"}, opts),
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestConnectWithOpts_error_notify(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Notify = make(chan tarantool.ConnEvent)

	connPool, err := pool.NewWithOpts(ctx, makeInstances([]string{"any"}, opts),
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestConnectWithOpts_error_reconnect_partial(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	badOpts := connOpts
	badOpts.Reconnect = time.Second
	instances := []pool.Instance{
		makeInstance(servers[0], connOpts),
		makeInstance("bad", badOpts),
	}

	connPool, err := pool.NewWithOpts(ctx, instances,
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
}

func TestConnectWithOpts_error_max_reconnects_partial(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	badOpts := connOpts
	badOpts.MaxReconnects = 3
	instances := []pool.Instance{
		makeInstance(servers[0], connOpts),
		makeInstance("bad", badOpts),
	}

	connPool, err := pool.NewWithOpts(ctx, instances,
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestConnectWithOpts_error_notify_partial(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	badOpts := connOpts
	badOpts.Notify = make(chan tarantool.ConnEvent)
	instances := []pool.Instance{
		makeInstance(servers[0], connOpts),
		makeInstance("bad", badOpts),
	}

	connPool, err := pool.NewWithOpts(ctx, instances,
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestConnectWithOpts_error_multiple_opts(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	opts := connOpts
	opts.Reconnect = time.Second
	opts.MaxReconnects = 3
	opts.Notify = make(chan tarantool.ConnEvent)

	connPool, err := pool.NewWithOpts(ctx, makeInstances([]string{"any"}, opts),
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestConnectWithOpts_error_multiple_instances(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	badOpts1 := connOpts
	badOpts1.Reconnect = time.Second
	badOpts2 := connOpts
	badOpts2.MaxReconnects = 3
	instances := []pool.Instance{
		makeInstance("bad1", badOpts1),
		makeInstance("bad2", badOpts2),
	}

	connPool, err := pool.NewWithOpts(ctx, instances,
		pool.Opts{CheckTimeout: time.Second})

	assert.Nil(t, connPool)
	require.ErrorIs(t, err, pool.ErrOptsReconnect)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestConnSuccessfully(t *testing.T) {
	healthyServ := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{healthyServ, "err"}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{healthyServ},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			healthyServ: true,
			"err":       false,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)
}

func TestConn_no_execute_supported(t *testing.T) {
	test_helpers.SkipIfWatchOnceUnsupported(t)

	healthyServ := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx,
		[]pool.Instance{makeNoExecuteInstance(healthyServ, connOpts)})
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{healthyServ},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			healthyServ: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeAny).Get()
	require.NoError(t, err)
}

func TestConn_no_execute_unsupported(t *testing.T) {
	test_helpers.SkipIfWatchOnceSupported(t)

	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	healthyServ := servers[0]

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       logger,
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.NewWithOpts(ctx,
		[]pool.Instance{makeNoExecuteInstance(healthyServ, connOpts)}, poolOpts)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	output := buf.String()
	require.Contains(t, output, pool.LogMsgConnectFailed)
	require.Contains(t, output, pool.LogKeyInstance)
	require.Contains(t, output, healthyServ)

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{healthyServ},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			healthyServ: false,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeAny).Get()
	require.Error(t, err)
	require.Equal(t, "can't find healthy instance in pool", err.Error())
}

func TestNew_empty(t *testing.T) {
	cases := []struct {
		Name      string
		Instances []pool.Instance
	}{
		{"nil", nil},
		{"empty", []pool.Instance{}},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			ctx, cancel := test_helpers.GetPoolConnectContext()
			defer cancel()
			connPool, err := pool.New(ctx, tc.Instances)
			if connPool != nil {
				defer func() { _ = connPool.Close() }()
			}

			require.NoError(t, err, "failed to create a pool")
			require.NotNilf(t, connPool, "pool is nil after Connect")
			require.Emptyf(t, connPool.Info(), "empty pool expected")
		})
	}
}

func TestNew_unavailable(t *testing.T) {
	servers := []string{"err1", "err2"}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	insts := makeInstances([]string{"err1", "err2"}, connOpts)

	connPool, err := pool.New(ctx, insts)
	cancel()

	if connPool != nil {
		defer func() { _ = connPool.Close() }()
	}

	require.NoError(t, err, "failed to create a pool")
	require.NotNilf(t, connPool, "pool is nil after Connect")
	require.Equal(t, map[string]pool.Info{
		servers[0]: pool.Info{
			ConnectedNow: false, Role: pool.RoleUnknown, Instance: insts[0]},
		servers[1]: pool.Info{
			ConnectedNow: false, Role: pool.RoleUnknown, Instance: insts[1]},
	}, connPool.Info())
}

func TestNew_single_server_hang(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	insts := makeInstances([]string{l.Addr().String()}, connOpts)

	connPool, err := pool.New(ctx, insts)
	if connPool != nil {
		defer func() { _ = connPool.Close() }()
	}

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, connPool)
}

func TestNew_server_hang(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	servers := []string{l.Addr().String(), servers[0]}
	insts := makeInstances(servers, connOpts)

	connPool, err := pool.New(ctx, insts)
	if connPool != nil {
		defer func() { _ = connPool.Close() }()
	}

	require.NoError(t, err, "failed to create a pool")
	require.NotNil(t, connPool, "pool is nil after Connect")
	require.Equal(t, map[string]pool.Info{
		servers[0]: pool.Info{
			ConnectedNow: false, Role: pool.RoleUnknown, Instance: insts[0]},
		servers[1]: pool.Info{
			ConnectedNow: true, Role: pool.RoleMaster, Instance: insts[1]},
	}, connPool.Info())
}

func TestConnErrorAfterCtxCancel(t *testing.T) {
	var longTimeoutOpts = tarantool.Opts{
		Timeout: 5 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())

	var connPool *pool.Pool
	var err error

	cancel()
	connPool, err = pool.New(ctx, makeInstances(servers, longTimeoutOpts))

	require.Nil(t, connPool, "Pool was created after cancel")
	require.Error(t, err, "Pool was created after cancel")
	require.Contains(t, err.Error(), "context canceled",
		"Unexpected error, expected to contain %s", "operation was canceled")
}

type mockClosingDialer struct {
	addr      string
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

	m.ctxCancel()

	return conn, err
}

func TestConnectContextCancelAfterConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var instances []pool.Instance
	for _, server := range servers {
		instances = append(instances, pool.Instance{
			Name: server,
			Dialer: &mockClosingDialer{
				addr:      server,
				ctx:       ctx,
				ctxCancel: cancel,
			},
			Opts: connOpts,
		})
	}

	connPool, err := pool.New(ctx, instances)
	if connPool != nil {
		defer func() { _ = connPool.Close() }()
	}

	require.NoError(t, err, "expected err after ctx cancel")
	assert.NotNil(t, connPool)
}

func TestConnSuccessfullyDuplicates(t *testing.T) {
	server := servers[0]

	var instances []pool.Instance
	for i := range 4 {
		instances = append(instances, pool.Instance{
			Name: fmt.Sprintf("c%d", i),
			Dialer: tarantool.NetDialer{
				Address:  server,
				User:     user,
				Password: pass,
			},
			Opts: connOpts,
		})
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
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
	require.NoError(t, err)
}

func TestReconnect(t *testing.T) {
	server := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	test_helpers.StopTarantoolWithCleanup(helpInstances[0])

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: false,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	err = test_helpers.RestartTarantool(helpInstances[0])
	require.NoErrorf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestDisconnect_withReconnect(t *testing.T) {
	serverId := 0
	server := servers[serverId]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{server}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// Test.
	test_helpers.StopTarantoolWithCleanup(helpInstances[serverId])
	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{servers[serverId]},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			servers[serverId]: false,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	// Restart the server after success.
	err = test_helpers.RestartTarantool(helpInstances[serverId])
	require.NoErrorf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{servers[serverId]},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[serverId]: true,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestDisconnectAll(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{server1, server2}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	test_helpers.StopTarantoolWithCleanup(helpInstances[0])
	test_helpers.StopTarantoolWithCleanup(helpInstances[1])

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	err = test_helpers.RestartTarantool(helpInstances[0])
	require.NoErrorf(t, err, "failed to restart tarantool")

	err = test_helpers.RestartTarantool(helpInstances[1])
	require.NoErrorf(t, err, "failed to restart tarantool")

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestAdd(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	for _, server := range servers {
		ctx, cancel := test_helpers.GetConnectContext()
		defer cancel()

		err = connPool.Add(ctx, makeInstance(server, connOpts))
		require.NoError(t, err)
	}

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
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

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestAdd_canceled_ctx(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	ctx, cancel = test_helpers.GetConnectContext()
	cancel()

	err = connPool.Add(ctx, makeInstance(servers[0], connOpts))
	require.Error(t, err)
}

func TestAdd_reconnect_error(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	opts := connOpts
	opts.Reconnect = time.Second

	err = connPool.Add(ctx, makeInstance(servers[0], opts))

	require.ErrorIs(t, err, pool.ErrOptsReconnect)
}

func TestAdd_max_reconnects_error(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	opts := connOpts
	opts.MaxReconnects = 3

	err = connPool.Add(ctx, makeInstance(servers[0], opts))

	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
}

func TestAdd_notify_error(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	opts := connOpts
	opts.Notify = make(chan tarantool.ConnEvent)

	err = connPool.Add(ctx, makeInstance(servers[0], opts))

	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestAdd_error_multiple_opts(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	opts := connOpts
	opts.Reconnect = time.Second
	opts.MaxReconnects = 3
	opts.Notify = make(chan tarantool.ConnEvent)

	err = connPool.Add(ctx, makeInstance(servers[0], opts))

	require.ErrorIs(t, err, pool.ErrOptsReconnect)
	require.ErrorIs(t, err, pool.ErrOptsMaxReconnects)
	require.ErrorIs(t, err, pool.ErrOptsNotify)
}

func TestAdd_exist(t *testing.T) {
	server := servers[0]
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{server}, connOpts))
	cancel()
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	ctx, cancel = test_helpers.GetConnectContext()
	defer cancel()

	err = connPool.Add(ctx, makeInstance(server, connOpts))
	require.Equal(t, pool.ErrExists, err)

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestAdd_unreachable(t *testing.T) {
	server := servers[0]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{server}, connOpts))
	cancel()
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	unhealthyServ := "unreachable:6667"
	err = connPool.Add(context.Background(), pool.Instance{
		Name: unhealthyServ,
		Dialer: tarantool.NetDialer{
			Address: unhealthyServ,
		},
		Opts: connOpts,
	})
	require.NoError(t, err)

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server:        true,
			unhealthyServ: false,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestAdd_afterClose(t *testing.T) {
	server := servers[0]
	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{server}, connOpts))
	cancel()
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	_ = connPool.Close()

	ctx, cancel = test_helpers.GetConnectContext()
	defer cancel()

	err = connPool.Add(ctx, makeInstance(server, connOpts))
	assert.Equal(t, err, pool.ErrClosed)
}

func TestAdd_Close_concurrent(t *testing.T) {
	serv0 := servers[0]
	serv1 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{serv0}, connOpts))
	cancel()
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, cancel := test_helpers.GetConnectContext()
		defer cancel()

		err = connPool.Add(ctx, makeInstance(serv1, connOpts))
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	_ = connPool.Close()

	wg.Wait()
}

func TestAdd_CloseGraceful_concurrent(t *testing.T) {
	serv0 := servers[0]
	serv1 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	connPool, err := pool.New(ctx, makeInstances([]string{serv0}, connOpts))
	cancel()
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, cancel := test_helpers.GetConnectContext()
		defer cancel()

		err := connPool.Add(ctx, makeInstance(serv1, connOpts))
		if err != nil {
			assert.Equal(t, pool.ErrClosed, err)
		}
	}()

	err = connPool.CloseGraceful()
	require.NoError(t, err)

	wg.Wait()
}

func TestRemove(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	for _, server := range servers[1:] {
		err = connPool.Remove(server)
		require.NoError(t, err)
	}

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestRemove_double(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances(servers[:2], connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	err = connPool.Remove(servers[1])
	require.NoError(t, err)
	err = connPool.Remove(servers[1])
	require.ErrorContains(t, err, "endpoint not exist")

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestRemove_unknown(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances(servers[:2], connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	err = connPool.Remove("not_exist:6667")
	require.ErrorContains(t, err, "endpoint not exist")

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
			servers[1]: true,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestRemove_concurrent(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances(servers[:2], connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	const concurrency = 10
	var (
		wg   sync.WaitGroup
		ok   uint32
		errs uint32
	)

	wg.Add(concurrency)
	for range concurrency {
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
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestRemove_Close_concurrent(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances(servers[:2], connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err = connPool.Remove(servers[1])
		assert.NoError(t, err)
	}()

	_ = connPool.Close()

	wg.Wait()
}

func TestRemove_CloseGraceful_concurrent(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances(servers[:2], connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := connPool.Remove(servers[1])
		assert.NoError(t, err)
	}()

	err = connPool.CloseGraceful()
	require.NoError(t, err)

	wg.Wait()
}

func TestClose(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{server1, server2}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	_ = connPool.Close()

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestCloseGraceful(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{server1, server2}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server1: true,
			server2: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	eval := `local fiber = require('fiber')
		local time = ...
		fiber.sleep(time)

`

	evalSleep := 3 // In seconds.
	req := tarantool.NewEvalRequest(eval).Args([]any{evalSleep})
	fut := connPool.Do(req, pool.ModeAny)
	go func() {
		err := connPool.CloseGraceful()
		assert.NoError(t, err)
	}()

	// Check that a request rejected if graceful shutdown in progress.
	time.Sleep((time.Duration(evalSleep) * time.Second) / 2)
	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeAny).Get()
	require.ErrorContains(t, err, "can't find healthy instance in pool")

	// Check that a previous request was successful.
	_, err = fut.Get()
	require.NoErrorf(t, err, "sleep request no error")

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

type testHandler struct {
	discovered, deactivated uint32
	errs                    []error
}

func (h *testHandler) addErr(err error) {
	h.errs = append(h.errs, err)
}

func (h *testHandler) Discovered(name string, conn *tarantool.Connection,
	role pool.Role) error {
	discovered := atomic.AddUint32(&h.discovered, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("discovered conn == nil"))
		return nil
	}

	// discovered < 3 - initial open of connections
	// discovered >= 3 - update a connection after a role update
	switch name {
	case servers[0]:
		if discovered < 3 && role != pool.RoleMaster {
			h.addErr(fmt.Errorf("unexpected init role %d for name %s", role, name))
		}
		if discovered >= 3 && role != pool.RoleReplica {
			h.addErr(fmt.Errorf("unexpected updated role %d for name %s", role, name))
		}
	case servers[1]:
		if discovered >= 3 {
			h.addErr(fmt.Errorf("unexpected discovery for name %s", name))
		}
		if role != pool.RoleReplica {
			h.addErr(fmt.Errorf("unexpected role %d for name %s", role, name))
		}
	default:
		h.addErr(fmt.Errorf("unexpected discovered name %s", name))
	}

	return nil
}

func (h *testHandler) Deactivated(name string, conn *tarantool.Connection,
	role pool.Role) error {
	deactivated := atomic.AddUint32(&h.deactivated, 1)

	if conn == nil {
		h.addErr(fmt.Errorf("removed conn == nil"))
		return nil
	}

	if deactivated == 1 && name == servers[0] {
		// A first close is a role update.
		if role != pool.RoleMaster {
			h.addErr(fmt.Errorf("unexpected removed role %d for name %s", role, name))
		}
		return nil
	}

	if name == servers[0] || name == servers[1] {
		// Close.
		if role != pool.RoleReplica {
			h.addErr(fmt.Errorf("unexpected removed role %d for name %s", role, name))
		}
	} else {
		h.addErr(fmt.Errorf("unexpected removed name %s", name))
	}

	return nil
}

func TestHandlerOpenUpdateClose(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	poolInstances := makeInstances(poolServers, connOpts)
	roles := []bool{false, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, makeDialers(poolServers), connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	h := &testHandler{}
	poolOpts := pool.Opts{
		CheckTimeout: 100 * time.Millisecond,
		Handler:      h,
	}
	connPool, err := pool.NewWithOpts(ctx, poolInstances, poolOpts)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	_, err = connPool.Do(tarantool.NewCallRequest("box.cfg").
		Args([]any{map[string]bool{
			"read_only": true,
		}}),
		pool.ModeRW).GetResponse()
	require.NoErrorf(t, err, "failed to make ro")

	for range 100 {
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

	_ = connPool.Close()

	for range 100 {
		// Wait for close of all connections.
		if atomic.LoadUint32(&h.deactivated) >= 3 {
			break
		}
		time.Sleep(poolOpts.CheckTimeout)
	}

	assert.Empty(t, h.errs, "Unexpected errors")
	connected, err := connPool.ConnectedNow(pool.ModeAny)
	require.NoErrorf(t, err, "failed to get connected state")
	require.Falsef(t, connected, "connection pool still be connected")

	discovered = atomic.LoadUint32(&h.discovered)
	deactivated = atomic.LoadUint32(&h.deactivated)
	require.Equalf(t, uint32(len(poolServers)+1), discovered,
		"unexpected discovered count")
	require.Equalf(t, uint32(len(poolServers)+1), deactivated,
		"unexpected deactivated count")
}

type testAddErrorHandler struct {
	discovered, deactivated uint32
}

func (h *testAddErrorHandler) Discovered(name string, conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.discovered, 1)
	return fmt.Errorf("any error")
}

func (h *testAddErrorHandler) Deactivated(name string, conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.deactivated, 1)
	return nil
}

func TestHandlerOpenError(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}

	h := &testAddErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout: 100 * time.Microsecond,
		Handler:      h,
	}
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	insts := makeInstances(poolServers, connOpts)
	connPool, err := pool.NewWithOpts(ctx, insts, poolOpts)
	if err == nil {
		defer func() { _ = connPool.Close() }()
	}
	require.NoError(t, err, "failed to connect")
	require.NotNil(t, connPool, "pool expected")
	require.Equal(t, map[string]pool.Info{
		servers[0]: pool.Info{
			ConnectedNow: false, Role: pool.RoleUnknown, Instance: insts[0]},
		servers[1]: pool.Info{
			ConnectedNow: false, Role: pool.RoleUnknown, Instance: insts[1]},
	}, connPool.Info())
	_ = connPool.Close()

	// It could happen additional reconnect attempts in the background, but
	// at least 2 connects on start.
	require.GreaterOrEqualf(t, atomic.LoadUint32(&h.discovered), uint32(2),
		"unexpected discovered count")
	require.Equalf(t, uint32(0), atomic.LoadUint32(&h.deactivated),
		"unexpected deactivated count")
}

type testUpdateErrorHandler struct {
	discovered, deactivated uint32
}

func (h *testUpdateErrorHandler) Discovered(name string, conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.discovered, 1)

	if atomic.LoadUint32(&h.deactivated) != 0 {
		// Don't add a connection into a pool again after it was deleted.
		return fmt.Errorf("any error")
	}
	return nil
}

func (h *testUpdateErrorHandler) Deactivated(name string, conn *tarantool.Connection,
	role pool.Role) error {
	atomic.AddUint32(&h.deactivated, 1)
	return nil
}

func TestHandlerUpdateError(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	poolInstances := makeInstances(poolServers, connOpts)
	roles := []bool{false, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, makeDialers(poolServers), connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	h := &testUpdateErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout: 100 * time.Microsecond,
		Handler:      h,
	}
	connPool, err := pool.NewWithOpts(ctx, poolInstances, poolOpts)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	connected, err := connPool.ConnectedNow(pool.ModeAny)
	require.NoErrorf(t, err, "failed to get ConnectedNow()")
	require.Truef(t, connected, "should be connected")

	roles = []bool{true, true}
	err = test_helpers.SetClusterRO(ctx, makeDialers(poolServers), connOpts, roles)
	require.NoErrorf(t, err, "failed to make ro")

	require.Eventuallyf(t, func() bool {
		connected, err = connPool.ConnectedNow(pool.ModeAny)
		return err == nil && !connected
	}, timeout, poolOpts.CheckTimeout, "should not be any active connection")
	require.NoErrorf(t, err, "failed to get ConnectedNow()")

	_ = connPool.Close()

	connected, err = connPool.ConnectedNow(pool.ModeAny)

	require.NoErrorf(t, err, "failed to get ConnectedNow()")
	require.Falsef(t, connected, "should be deactivated")
	discovered := atomic.LoadUint32(&h.discovered)
	deactivated := atomic.LoadUint32(&h.deactivated)
	require.GreaterOrEqualf(t, discovered, deactivated, "discovered < deactivated")
	require.NoErrorf(t, err, "failed to get ConnectedNow()")
}

type testDeactivatedErrorHandler struct {
	mut         sync.Mutex
	deactivated []string
}

func (h *testDeactivatedErrorHandler) Discovered(name string, conn *tarantool.Connection,
	role pool.Role) error {
	return nil
}

func (h *testDeactivatedErrorHandler) Deactivated(name string, conn *tarantool.Connection,
	role pool.Role) error {
	h.mut.Lock()
	defer h.mut.Unlock()

	h.deactivated = append(h.deactivated, name)
	return nil
}

func TestHandlerDeactivated_on_remove(t *testing.T) {
	poolServers := []string{servers[0], servers[1]}
	poolInstances := makeInstances(poolServers, connOpts)
	roles := []bool{false, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, makeDialers(poolServers), connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	h := &testDeactivatedErrorHandler{}
	poolOpts := pool.Opts{
		CheckTimeout: 100 * time.Millisecond,
		Handler:      h,
	}
	connPool, err := pool.NewWithOpts(ctx, poolInstances, poolOpts)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			servers[0]: true,
			servers[1]: true,
		},
	}
	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	for _, server := range poolServers {
		err = connPool.Remove(server)
		require.NoError(t, err)
	}

	args = test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            servers,
		ExpectedPoolStatus: false,
	}
	err = test_helpers.CheckPoolStatuses(args)
	require.NoError(t, err)

	h.mut.Lock()
	defer h.mut.Unlock()
	require.ElementsMatch(t, poolServers, h.deactivated)
}

func TestRequestOnClosed(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, makeInstances([]string{server1, server2}, connOpts))
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	test_helpers.StopTarantoolWithCleanup(helpInstances[0])
	test_helpers.StopTarantoolWithCleanup(helpInstances[1])

	args := test_helpers.CheckStatusesArgs{
		Pool:               connPool,
		Mode:               pool.ModeAny,
		Servers:            []string{server1, server2},
		ExpectedPoolStatus: false,
		ExpectedStatuses: map[string]bool{
			server1: false,
			server2: false,
		},
	}
	assert.Eventually(t, func() bool {
		err = test_helpers.CheckPoolStatuses(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeAny).Get()
	require.Errorf(t, err, "err is nil after Do with PingRequest")

	err = test_helpers.RestartTarantool(helpInstances[0])
	require.NoErrorf(t, err, "failed to restart tarantool")

	err = test_helpers.RestartTarantool(helpInstances[1])
	require.NoErrorf(t, err, "failed to restart tarantool")
}

func TestDoWithCallRequest(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModePreferRO
	data, err := connPool.Do(
		tarantool.NewCallRequest("box.info").
			Args([]any{}),
		pool.ModePreferRO).Get()
	require.NoErrorf(t, err, "failed to Do with CallRequest")
	require.NotNilf(t, data, "response is nil after Do with CallRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with CallRequest")

	val := data[0].(map[any]any)["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `ModePreferRO`")
	require.Truef(t, ro, "expected `true` with mode `ModePreferRO`")

	// ModePreferRW
	data, err = connPool.Do(
		tarantool.NewCallRequest("box.info").
			Args([]any{}),
		pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Do with CallRequest")
	require.NotNilf(t, data, "response is nil after Do with CallRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with CallRequest")

	val = data[0].(map[any]any)["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `ModePreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `ModePreferRW`")

	// ModeRO
	data, err = connPool.Do(
		tarantool.NewCallRequest("box.info").
			Args([]any{}),
		pool.ModeRO).Get()
	require.NoErrorf(t, err, "failed to Do with CallRequest")
	require.NotNilf(t, data, "response is nil after Do with CallRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with CallRequest")

	val = data[0].(map[any]any)["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `ModeRO`")
	require.Truef(t, ro, "expected `true` with mode `ModeRO`")

	// ModeRW
	data, err = connPool.Do(
		tarantool.NewCallRequest("box.info").
			Args([]any{}),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with CallRequest")
	require.NotNilf(t, data, "response is nil after Do with CallRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with CallRequest")

	val = data[0].(map[any]any)["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `ModeRW`")
	require.Falsef(t, ro, "expected `false` with mode `ModeRW`")
}

func TestDoWithEvalRequest(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModePreferRO
	data, err := connPool.Do(
		tarantool.NewEvalRequest("return box.info().ro").
			Args([]any{}),
		pool.ModePreferRO).Get()
	require.NoErrorf(t, err, "failed to Do with EvalRequest")
	require.NotNilf(t, data, "response is nil after Do with EvalRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with EvalRequest")

	val, ok := data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `ModePreferRO`")
	require.Truef(t, val, "expected `true` with mode `ModePreferRO`")

	// ModePreferRW
	data, err = connPool.Do(
		tarantool.NewEvalRequest("return box.info().ro").
			Args([]any{}),
		pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Do with EvalRequest")
	require.NotNilf(t, data, "response is nil after Do with EvalRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with EvalRequest")

	val, ok = data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `ModePreferRW`")
	require.Falsef(t, val, "expected `false` with mode `ModePreferRW`")

	// ModeRO
	data, err = connPool.Do(
		tarantool.NewEvalRequest("return box.info().ro").
			Args([]any{}),
		pool.ModeRO).Get()
	require.NoErrorf(t, err, "failed to Do with EvalRequest")
	require.NotNilf(t, data, "response is nil after Do with EvalRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with EvalRequest")

	val, ok = data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `ModeRO`")
	require.Truef(t, val, "expected `true` with mode `ModeRO`")

	// ModeRW
	data, err = connPool.Do(
		tarantool.NewEvalRequest("return box.info().ro").
			Args([]any{}),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with EvalRequest")
	require.NotNilf(t, data, "response is nil after Do with EvalRequest")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with EvalRequest")

	val, ok = data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `ModeRW`")
	require.Falsef(t, val, "expected `false` with mode `ModeRW`")
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

func TestDoWithExecuteRequest(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{false, true, false, false, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	request := "SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0 == 1;"
	mem := []Member{}

	fut := connPool.Do(tarantool.NewExecuteRequest(request).Args([]any{}), pool.ModeAny)
	data, err := fut.Get()
	require.NoErrorf(t, err, "failed to Do with ExecuteRequest")
	require.NotNilf(t, data, "response is nil after Execute")
	require.GreaterOrEqualf(t, len(data), 1, "response.Data is empty after Do with ExecuteRequest")
	require.Lenf(t, data[0].([]any), 2, "unexpected response")
	err = fut.GetTyped(&mem)
	require.NoErrorf(t, err, "Unable to GetTyped of fut")
	require.Lenf(t, mem, 1, "wrong count of result")
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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeAny
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeAny,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModeRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModeRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)
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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeRO
	_, err = connPool.Do(
		tarantool.NewEvalRequest("return box.cfg.listen").
			Args([]any{}),
		pool.ModeRO).Get()
	require.Errorf(t, err, "expected to fail after Do with EvalRequest, but error is nil")
	require.Equal(t, "can't find ro instance in pool", err.Error())

	// ModeAny
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeAny,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)
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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeRW
	_, err = connPool.Do(
		tarantool.NewEvalRequest("return box.cfg.listen").
			Args([]any{}),
		pool.ModeRW).Get()
	require.Errorf(t, err, "expected to fail after Do with EvalRequest, but error is nil")
	require.Equal(t, "can't find rw instance in pool", err.Error())

	// ModeAny
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeAny,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)
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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeAny
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeAny,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModeRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModeRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModeRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

	// ModePreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.NoError(t, err)

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

	ctxSetRoles, cancelSetRoles := test_helpers.GetPoolConnectContext()
	err = test_helpers.SetClusterRO(ctxSetRoles, dialers, connOpts, roles)
	cancelSetRoles()
	require.NoErrorf(t, err, "fail to set roles for cluster")

	// ModeAny
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		Pool:          connPool,
		Mode:          pool.ModeAny,
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.ProcessListenOnInstance(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	// ModeRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModeRW,
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.ProcessListenOnInstance(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	// ModeRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModeRO,
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.ProcessListenOnInstance(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	// ModePreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRW,
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.ProcessListenOnInstance(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)

	// ModePreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		Pool:          connPool,
		Mode:          pool.ModePreferRO,
	}

	assert.Eventually(t, func() bool {
		err = test_helpers.ProcessListenOnInstance(args)
		return err == nil
	}, timeout, tick)
	require.NoError(t, err)
}

func TestDoWithInsertRequest(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeRW
	data, err := connPool.Do(tarantool.NewInsertRequest(spaceName).
		Tuple([]any{"rw_insert_key", "rw_insert_value"}),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Insert")
	require.NotNilf(t, data, "response is nil after Insert")
	require.Lenf(t, data, 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Insert")
	require.Lenf(t, tpl, 2, "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "rw_insert_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "rw_insert_value", value, "unexpected body of Insert (1)")

	// Connect to servers[2] to check if tuple
	// was inserted on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"rw_insert_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rw_insert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rw_insert_value", value, "unexpected body of Select (1)")

	// ModePreferRW
	data, err = connPool.Do(tarantool.NewInsertRequest(spaceName).Tuple(
		[]any{"preferRW_insert_key", "preferRW_insert_value"}), pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Insert")
	require.NotNilf(t, data, "response is nil after Insert")
	require.Lenf(t, data, 1, "response Body len != 1 after Insert")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Insert")
	require.Lenf(t, tpl, 2, "unexpected body of Insert")

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
		Key([]any{"preferRW_insert_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "preferRW_insert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "preferRW_insert_value", value, "unexpected body of Select (1)")
}

func TestDoWithDeleteRequest(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// Connect to servers[2] to check if tuple
	// was inserted on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	ins := tarantool.NewInsertRequest(spaceNo).Tuple([]any{"delete_key", "delete_value"})
	data, err := conn.Do(ins).Get()
	require.NoErrorf(t, err, "failed to Insert")
	require.Lenf(t, data, 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Insert")
	require.Lenf(t, tpl, 2, "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "delete_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "delete_value", value, "unexpected body of Insert (1)")

	data, err = connPool.Do(
		tarantool.NewDeleteRequest(spaceName).
			Index(indexNo).
			Key([]any{"delete_key"}),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with DeleteRequest")
	require.NotNilf(t, data, "response is nil after Do with DeleteRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with DeleteRequest")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with DeleteRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with DeleteRequest")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with DeleteRequest (0)")
	require.Equalf(t, "delete_key", key, "unexpected body of Do with DeleteRequest (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Do with DeleteRequest (1)")
	require.Equalf(t, "delete_value", value, "unexpected body of Do with DeleteRequest (1)")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"delete_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Emptyf(t, data, "response Body len != 0 after Select")
}

func TestDoWithUpsertRequest(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// Connect to servers[2] to check if tuple
	// was inserted on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	// ModeRW
	data, err := connPool.Do(tarantool.NewUpsertRequest(spaceName).Tuple(
		[]any{"upsert_key", "upsert_value"}).Operations(
		tarantool.NewOperations().Assign(1, "new_value")), pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Upsert")
	require.NotNilf(t, data, "response is nil after Upsert")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"upsert_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "upsert_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "upsert_value", value, "unexpected body of Select (1)")

	// ModePreferRW
	data, err = connPool.Do(tarantool.NewUpsertRequest(
		spaceName).Tuple([]any{"upsert_key", "upsert_value"}).Operations(
		tarantool.NewOperations().Assign(1, "new_value")), pool.ModePreferRW).Get()

	require.NoErrorf(t, err, "failed to Upsert")
	require.NotNilf(t, data, "response is nil after Upsert")

	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "upsert_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")
}

func TestDoWithUpdateRequest(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// Connect to servers[2] to check if tuple
	// was inserted on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]any{"update_key", "update_value"})
	data, err := conn.Do(ins).Get()
	require.NoErrorf(t, err, "failed to Insert")
	require.Lenf(t, data, 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Insert")
	require.Lenf(t, tpl, 2, "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "update_value", value, "unexpected body of Insert (1)")

	// ModeRW
	resp, err := connPool.Do(tarantool.NewUpdateRequest(spaceName).
		Index(indexNo).
		Key([]any{"update_key"}).
		Operations(tarantool.NewOperations().Assign(1, "new_value")),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"update_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")

	// ModePreferRW
	resp, err = connPool.Do(tarantool.NewUpdateRequest(spaceName).
		Index(indexNo).
		Key([]any{"update_key"}).
		Operations(tarantool.NewOperations().Assign(1, "another_value")),
		pool.ModePreferRW).Get()

	require.NoErrorf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "update_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "another_value", value, "unexpected body of Select (1)")
}

func TestDoWithReplaceRequest(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// Connect to servers[2] to check if tuple
	// was inserted on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	ins := tarantool.NewInsertRequest(spaceNo).
		Tuple([]any{"replace_key", "replace_value"})
	data, err := conn.Do(ins).Get()
	require.NoErrorf(t, err, "failed to Insert")
	require.Lenf(t, data, 1, "response Body len != 1 after Insert")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Insert")
	require.Lenf(t, tpl, 2, "unexpected body of Insert")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Insert (0)")
	require.Equalf(t, "replace_key", key, "unexpected body of Insert (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Insert (1)")
	require.Equalf(t, "replace_value", value, "unexpected body of Insert (1)")

	// ModeRW
	resp, err := connPool.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]any{"new_key", "new_value"}),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with ReplaceRequest")
	require.NotNilf(t, resp, "response is nil after Do with ReplaceRequest")

	sel := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"new_key"})
	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "new_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")

	// ModePreferRW
	resp, err = connPool.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]any{"new_key", "new_value"}),
		pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Do with ReplaceRequest")
	require.NotNilf(t, resp, "response is nil after Do with ReplaceRequest")

	data, err = conn.Do(sel).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "new_key", key, "unexpected body of Select (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "new_value", value, "unexpected body of Select (1)")
}

func TestDoWithSelectRequest(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	roServers := []string{servers[0], servers[1], servers[3]}
	rwServers := []string{servers[2], servers[4]}
	allServers := []string{servers[0], servers[1], servers[2], servers[3], servers[4]}

	roTpl := []any{"ro_select_key", "ro_select_value"}
	rwTpl := []any{"rw_select_key", "rw_select_value"}
	anyTpl := []any{"any_select_key", "any_select_value"}

	roKey := []any{"ro_select_key"}
	rwKey := []any{"rw_select_key"}
	anyKey := []any{"any_select_key"}

	err = test_helpers.InsertOnInstances(ctx, makeDialers(roServers), connOpts, spaceNo, roTpl)
	require.NoError(t, err)

	err = test_helpers.InsertOnInstances(ctx, makeDialers(rwServers), connOpts, spaceNo, rwTpl)
	require.NoError(t, err)

	err = test_helpers.InsertOnInstances(ctx, makeDialers(allServers), connOpts, spaceNo, anyTpl)
	require.NoError(t, err)

	// ModeAny
	data, err := connPool.Do(tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key(anyKey),
		pool.ModeAny).Get()
	require.NoErrorf(t, err, "failed to Do with SelectRequest")
	require.NotNilf(t, data, "response is nil after Do with SelectRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with SelectRequest")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with SelectRequest")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (0)")
	require.Equalf(t, "any_select_key", key, "unexpected body of Do with SelectRequest (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (1)")
	require.Equalf(t, "any_select_value", value, "unexpected body of Do with SelectRequest (1)")

	// ModePreferRO
	data, err = connPool.Do(tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key(roKey),
		pool.ModePreferRO).Get()
	require.NoErrorf(t, err, "failed to Do with SelectRequest")
	require.NotNilf(t, data, "response is nil after Do with SelectRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with SelectRequest")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with SelectRequest")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Do with SelectRequest (0)")

	// ModePreferRW
	data, err = connPool.Do(tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key(rwKey),
		pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Do with SelectRequest")
	require.NotNilf(t, data, "response is nil after Do with SelectRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with SelectRequest")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with SelectRequest")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (0)")
	require.Equalf(t, "rw_select_key", key, "unexpected body of Do with SelectRequest (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (1)")
	require.Equalf(t, "rw_select_value", value, "unexpected body of Do with SelectRequest (1)")

	// ModeRO
	data, err = connPool.Do(tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key(roKey),
		pool.ModeRO).Get()
	require.NoErrorf(t, err, "failed to Do with SelectRequest")
	require.NotNilf(t, data, "response is nil after Do with SelectRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with SelectRequest")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with SelectRequest")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (0)")
	require.Equalf(t, "ro_select_key", key, "unexpected body of Do with SelectRequest (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (1)")
	require.Equalf(t, "ro_select_value", value, "unexpected body of Do with SelectRequest (1)")

	// ModeRW
	data, err = connPool.Do(tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Offset(0).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key(rwKey),
		pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with SelectRequest")
	require.NotNilf(t, data, "response is nil after Do with SelectRequest")
	require.Lenf(t, data, 1, "response Body len != 1 after Do with SelectRequest")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest")
	require.Lenf(t, tpl, 2, "unexpected body of Do with SelectRequest")

	key, ok = tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (0)")
	require.Equalf(t, "rw_select_key", key, "unexpected body of Do with SelectRequest (0)")

	value, ok = tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Do with SelectRequest (1)")
	require.Equalf(t, "rw_select_value", value, "unexpected body of Do with SelectRequest (1)")
}

func TestDoWithPingRequest(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	// ModeAny
	data, err := connPool.Do(tarantool.NewPingRequest(), pool.ModeAny).Get()
	require.NoErrorf(t, err, "failed to Do with Ping Request")
	require.Nilf(t, data, "response data is not nil after Do with Ping Request")

	// ModeRW
	data, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Do with Ping Request")
	require.Nilf(t, data, "response data is not nil after Do with Ping Request")

	// ModeRO
	_, err = connPool.Do(tarantool.NewPingRequest(), pool.ModeRO).Get()
	require.NoErrorf(t, err, "failed to Do with Ping Request")

	// ModePreferRW
	data, err = connPool.Do(tarantool.NewPingRequest(), pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Do with Ping Request")
	require.Nilf(t, data, "response data is not nil after Do with Ping Request")

	// ModePreferRO
	data, err = connPool.Do(tarantool.NewPingRequest(), pool.ModePreferRO).Get()
	require.NoErrorf(t, err, "failed to Do with Ping Request")
	require.Nilf(t, data, "response data is not nil after Do with Ping Request")
}

func TestDo(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	req := tarantool.NewPingRequest()
	// ModeAny
	_, err = connPool.Do(req, pool.ModeAny).Get()
	require.NoErrorf(t, err, "failed to Ping")

	// ModeRW
	_, err = connPool.Do(req, pool.ModeRW).Get()
	require.NoErrorf(t, err, "failed to Ping")

	// ModeRO
	_, err = connPool.Do(req, pool.ModeRO).Get()
	require.NoErrorf(t, err, "failed to Ping")

	// ModePreferRW
	_, err = connPool.Do(req, pool.ModePreferRW).Get()
	require.NoErrorf(t, err, "failed to Ping")

	// ModePreferRO
	_, err = connPool.Do(req, pool.ModePreferRO).Get()
	require.NoErrorf(t, err, "failed to Ping")
}

func TestDo_concurrent(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	req := tarantool.NewPingRequest()
	const concurrency = 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			_, err := connPool.Do(req, pool.ModeAny).Get()
			assert.NoError(t, err)
		}()
	}

	wg.Wait()
}

func TestDoOn(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	req := tarantool.NewEvalRequest("return box.cfg.listen")
	for _, server := range servers {
		data, err := connPool.DoOn(req, server).Get()
		require.NoError(t, err)
		assert.Equal(t, []any{server}, data)
	}
}

func TestDoOn_not_found(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, []pool.Instance{})
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	data, err := connPool.DoOn(tarantool.NewPingRequest(), "not_exist").Get()
	assert.Nil(t, data)
	require.ErrorIs(t, err, pool.ErrNoHealthyInstance)
}

func TestDoOn_concurrent(t *testing.T) {
	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	eval := tarantool.NewEvalRequest("return box.cfg.listen")
	ping := tarantool.NewPingRequest()
	const concurrency = 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()

			for _, server := range servers {
				data, err := connPool.DoOn(eval, server).Get()
				assert.NoError(t, err)
				assert.Equal(t, []any{server}, data)
			}
			_, err := connPool.DoOn(ping, "not_exist").Get()
			assert.ErrorIs(t, err, pool.ErrNoHealthyInstance)
		}()
	}

	wg.Wait()
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	stmt, err := connPool.NewPrepared(
		"SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;", pool.ModeRO)
	require.NoErrorf(t, err, "fail to prepare statement: %v", err)

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	resp, err := connPool.Do(executeReq.Args([]any{1, "test"}), pool.ModeAny).GetResponse()
	require.NoError(t, err, "failed to execute prepared")
	require.NotNil(t, resp, "nil response")
	data, err := resp.Decode()
	require.NoError(t, err, "failed to Decode")
	assert.NotEqual(t, []any{1, "test"}, data[0], "Select with named arguments failed")
	prepResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "Not a Prepare response")
	metaData, err := prepResp.MetaData()
	require.NoError(t, err, "Error while getting MetaData")
	assert.Equal(t, "unsigned", metaData[0].FieldType)
	assert.Equal(t, "NAME0", metaData[0].FieldName)
	assert.Equal(t, "string", metaData[1].FieldType)
	assert.Equal(t, "NAME1", metaData[1].FieldName)

	// the second argument for unprepare request is unused - it already belongs to some connection
	_, err = connPool.Do(unprepareReq, pool.ModeAny).Get()
	require.NoError(t, err, "failed to unprepare prepared statement")

	_, err = connPool.Do(unprepareReq, pool.ModeAny).Get()
	require.Error(t, err, "the statement must be already unprepared")
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = connPool.Do(executeReq, pool.ModeAny).Get()
	require.Error(t, err, "the statement must be already unprepared")
	require.Contains(t, err.Error(), "Prepared statement with id")
}

func TestDoWithStrangerConn(t *testing.T) {
	expectedErr := fmt.Errorf("the passed connected request doesn't belong to " +
		"the current connection pool")

	roles := []bool{true, true, false, true, false}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer func() { _ = connPool.Close() }()

	req := test_helpers.NewMockRequest()

	_, err = connPool.Do(req, pool.ModeAny).Get()
	require.Error(t, err, "nil error caught")
	require.EqualError(t, err, expectedErr.Error(), "Unexpected error caught")
}

func TestStream_Commit(t *testing.T) {
	var req tarantool.Request
	var err error

	test_helpers.SkipIfStreamsUnsupported(t)

	roles := []bool{true, true, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err = test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	stream, err := connPool.NewStream(pool.ModePreferRW)
	require.NoErrorf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Begin")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]any{"commit_key", "commit_value"})
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Insert")

	// Connect to servers[2] to check if tuple
	// was inserted outside of stream on ModeRW instance
	// before transaction commit
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"commit_key"})
	data, err := conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Emptyf(t, data, "response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "commit_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "commit_value", value, "unexpected body of Select (1)")

	// Commit transaction
	req = tarantool.NewCommitRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Commit")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok = data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err = test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	stream, err := connPool.NewStream(pool.ModePreferRW)
	require.NoErrorf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Begin transaction
	req = tarantool.NewBeginRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Begin")

	// Insert in stream
	req = tarantool.NewInsertRequest(spaceName).
		Tuple([]any{"rollback_key", "rollback_value"})
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Insert")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	// Select not related to the transaction
	// while transaction is not committed
	// result of select is empty
	selectReq := tarantool.NewSelectRequest(spaceNo).
		Index(indexNo).
		Limit(1).
		Iterator(tarantool.IterEq).
		Key([]any{"rollback_key"})
	data, err := conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Emptyf(t, data, "response Data len != 0")

	// Select in stream
	data, err = stream.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Lenf(t, data, 1, "response Body len != 1 after Select")

	tpl, ok := data[0].([]any)
	require.Truef(t, ok, "unexpected body of Select")
	require.Lenf(t, tpl, 2, "unexpected body of Select")

	key, ok := tpl[0].(string)
	require.Truef(t, ok, "unexpected body of Select (0)")
	require.Equalf(t, "rollback_key", key, "unexpected body of Select (0)")

	value, ok := tpl[1].(string)
	require.Truef(t, ok, "unexpected body of Select (1)")
	require.Equalf(t, "rollback_value", value, "unexpected body of Select (1)")

	// Rollback transaction
	req = tarantool.NewRollbackRequest()
	_, err = stream.Do(req).Get()
	require.NoErrorf(t, err, "failed to Rollback")

	// Select outside of transaction
	data, err = conn.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Emptyf(t, data, "response Body len != 0 after Select")

	// Select inside of stream after rollback
	data, err = stream.Do(selectReq).Get()
	require.NoErrorf(t, err, "failed to Select")
	require.Emptyf(t, data, "response Body len != 0 after Select")
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

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err = test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	stream, err := connPool.NewStream(pool.ModePreferRW)
	require.NoErrorf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on ModeRW instance
	conn := test_helpers.ConnectWithValidation(t, makeDialer(servers[2]), connOpts)
	defer func() { _ = conn.Close() }()

	for _, level := range txnIsolationLevels {
		// Begin transaction
		req = tarantool.NewBeginRequest().TxnIsolation(level).Timeout(500 * time.Millisecond)
		_, err = stream.Do(req).Get()
		require.NoErrorf(t, err, "failed to Begin")

		// Insert in stream
		req = tarantool.NewInsertRequest(spaceName).
			Tuple([]any{"level_key", "level_value"})
		_, err = stream.Do(req).Get()
		require.NoErrorf(t, err, "failed to Insert")

		// Select not related to the transaction
		// while transaction is not committed
		// result of select is empty
		selectReq := tarantool.NewSelectRequest(spaceNo).
			Index(indexNo).
			Limit(1).
			Iterator(tarantool.IterEq).
			Key([]any{"level_key"})
		data, err := conn.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Emptyf(t, data, "response Data len != 0")

		// Select in stream
		data, err = stream.Do(selectReq).Get()
		require.NoErrorf(t, err, "failed to Select")
		require.Lenf(t, data, 1, "response Body len != 1 after Select")

		tpl, ok := data[0].([]any)
		require.Truef(t, ok, "unexpected body of Select")
		require.Lenf(t, tpl, 2, "unexpected body of Select")

		key, ok := tpl[0].(string)
		require.Truef(t, ok, "unexpected body of Select (0)")
		require.Equalf(t, "level_key", key, "unexpected body of Select (0)")

		value, ok := tpl[1].(string)
		require.Truef(t, ok, "unexpected body of Select (1)")
		require.Equalf(t, "level_value", value, "unexpected body of Select (1)")

		// Rollback transaction
		req = tarantool.NewRollbackRequest()
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

		test_helpers.DeleteRecordByKey(t, conn, spaceNo, indexNo, []any{"level_key"})
	}
}

func TestPool_NewWatcher_no_watchers(t *testing.T) {
	test_helpers.SkipIfWatchersSupported(t)

	const key = "TestPool_NewWatcher_no_watchers"

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()
	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nill after Connect")
	defer func() { _ = connPool.Close() }()

	ch := make(chan struct{})
	_, _ = connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
		close(ch)
	}, pool.ModeAny)

	select {
	case <-time.After(time.Second):
		break
	case <-ch:
		require.Fail(t, "watcher was created for connection that doesn't support it")
	}
}

func TestPool_NewWatcher_modes(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestPool_NewWatcher_modes"

	roles := []bool{true, false, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	modes := []pool.Mode{
		pool.ModeAny,
		pool.ModeRW,
		pool.ModeRO,
		pool.ModePreferRW,
		pool.ModePreferRO,
	}
	for _, mode := range modes {
		t.Run(fmt.Sprintf("%d", mode), func(t *testing.T) {
			expectedServers := []string{}
			for i, server := range servers {
				if roles[i] && mode == pool.ModeRW {
					continue
				} else if !roles[i] && mode == pool.ModeRO {
					continue
				}
				expectedServers = append(expectedServers, server)
			}

			events := make(chan tarantool.WatchEvent, 1024)
			defer close(events)

			watcher, err := connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
				require.Equal(t, key, event.Key)
				require.Nil(t, event.Value)
				events <- event
			}, mode)
			require.NoErrorf(t, err, "failed to register a watcher")
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
					assert.Fail(t, "Failed to get a watch event.")
					break
				}
			}

			for _, server := range expectedServers {
				val, ok := testMap[server]
				assert.True(t, ok, "Server not found: %s", server)
				assert.Equal(t, 1, val, "Too many events for server %s", server)
			}
		})
	}
}

func TestPool_NewWatcher_update(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestPool_NewWatcher_update"
	const mode = pool.ModeRW
	const initCnt = 2
	roles := []bool{true, false, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	poolOpts := pool.Opts{
		CheckTimeout: 500 * time.Millisecond,
	}
	pool, err := pool.NewWithOpts(ctx, instances, poolOpts)

	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer func() { _ = pool.Close() }()

	events := make(chan tarantool.WatchEvent, 1024)
	defer close(events)

	watcher, err := pool.NewWatcher(key, func(event tarantool.WatchEvent) {
		require.Equal(t, key, event.Key)
		require.Nil(t, event.Value)
		events <- event
	}, mode)
	require.NoErrorf(t, err, "failed to create a watcher")
	defer watcher.Unregister()

	// Wait for all initial events.
	testMap := make(map[string]int)
	for range initCnt {
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
			assert.Fail(t, "Failed to get a watch init event.")
			break
		}
	}

	// Just invert roles for simplify the test.
	for i, role := range roles {
		roles[i] = !role
	}
	ctxSetRoles, cancelSetRoles := test_helpers.GetPoolConnectContext()
	err = test_helpers.SetClusterRO(ctxSetRoles, dialers, connOpts, roles)
	cancelSetRoles()
	require.NoErrorf(t, err, "fail to set roles for cluster")

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
			assert.Fail(t, "Failed to get a watch update event.")
			break
		}
	}

	// Check that all an event happen for an each connection.
	for _, server := range servers {
		val, ok := testMap[server]
		assert.True(t, ok, "Server not found: %s", server)
		require.Equalf(t, 1, val, "for server %s", server)
	}
}

func TestWatcher_Unregister(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const key = "TestWatcher_Unregister"
	const mode = pool.ModeRW
	const expectedCnt = 2
	roles := []bool{true, false, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	pool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, pool, "conn is nil after Connect")
	defer func() { _ = pool.Close() }()

	events := make(chan tarantool.WatchEvent, 1024)
	defer close(events)

	watcher, err := pool.NewWatcher(key, func(event tarantool.WatchEvent) {
		require.Equal(t, key, event.Key)
		require.Nil(t, event.Value)
		events <- event
	}, mode)
	require.NoErrorf(t, err, "failed to create a watcher")

	for range expectedCnt {
		select {
		case <-events:
		case <-time.After(time.Second):
			require.Fail(t, "Failed to skip initial events.")
		}
	}
	watcher.Unregister()

	broadcast := tarantool.NewBroadcastRequest(key).Value("foo")
	for range expectedCnt {
		_, err := pool.Do(broadcast, mode).Get()
		require.NoErrorf(t, err, "failed to send a broadcast request")
	}

	select {
	case event := <-events:
		require.Fail(t, "Get unexpected event", "event: %v", event)
	case <-time.After(time.Second):
	}

	// Reset to the initial state.
	broadcast = tarantool.NewBroadcastRequest(key)
	for range expectedCnt {
		_, err := pool.Do(broadcast, mode).Get()
		require.NoErrorf(t, err, "failed to send a broadcast request")
	}
}

func TestPool_NewWatcher_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestPool_NewWatcher_concurrent"

	roles := []bool{true, false, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	var wg sync.WaitGroup
	wg.Add(testConcurrency)

	mode := pool.ModeAny
	callback := func(event tarantool.WatchEvent) {}
	for range testConcurrency {
		go func() {
			defer wg.Done()

			watcher, err := connPool.NewWatcher(key, callback, mode)
			if assert.NoError(t, err, "Failed to create a watcher") {
				watcher.Unregister()
			}
		}()
	}
	wg.Wait()
}

func TestWatcher_Unregister_concurrent(t *testing.T) {
	test_helpers.SkipIfWatchersUnsupported(t)

	const testConcurrency = 1000
	const key = "TestWatcher_Unregister_concurrent"

	roles := []bool{true, false, false, true, true}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	err := test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
	require.NoErrorf(t, err, "fail to set roles for cluster")

	connPool, err := pool.New(ctx, instances)
	require.NoErrorf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer func() { _ = connPool.Close() }()

	mode := pool.ModeAny
	watcher, err := connPool.NewWatcher(key, func(event tarantool.WatchEvent) {
	}, mode)
	require.NoErrorf(t, err, "failed to create a watcher")

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

	helpInstances, err = test_helpers.StartTarantoolInstances(instsOpts)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
		return -1
	}

	defer test_helpers.StopTarantoolInstances(helpInstances)

	return m.Run()
}

func TestPool_Info_equal_instance_info(t *testing.T) {
	var tCases [][]pool.Instance

	tCases = append(tCases, makeInstances([]string{servers[0], servers[1]}, connOpts))
	tCases = append(tCases, makeInstances([]string{
		servers[0],
		servers[1],
		servers[3]},
		connOpts))
	tCases = append(tCases, makeInstances([]string{servers[0]}, connOpts))

	for _, tc := range tCases {
		ctx, cancel := test_helpers.GetPoolConnectContext()
		connPool, err := pool.New(ctx, tc)
		cancel()
		require.NoErrorf(t, err, "failed to connect")
		require.NotNilf(t, connPool, "conn is nil after Connect")

		info := connPool.Info()

		var infoInstances []pool.Instance

		for _, infoInst := range info {
			infoInstances = append(infoInstances, infoInst.Instance)
		}

		require.ElementsMatch(t, tc, infoInstances)
		_ = connPool.Close()
	}
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
