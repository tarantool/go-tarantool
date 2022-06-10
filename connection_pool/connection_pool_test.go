package connection_pool_test

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/connection_pool"
	"github.com/tarantool/go-tarantool/test_helpers"
)

var spaceNo = uint32(520)
var spaceName = "testPool"
var indexNo = uint32(0)
var indexName = "pk"

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
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
}

var defaultCountRetry = 5
var defaultTimeoutRetry = 500 * time.Millisecond

var instances []test_helpers.TarantoolInstance

func TestConnError_IncorrectParams(t *testing.T) {
	connPool, err := connection_pool.Connect([]string{}, tarantool.Opts{})
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "addrs (first argument) should not be empty", err.Error())

	connPool, err = connection_pool.Connect([]string{"err1", "err2"}, connOpts)
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "no active connections", err.Error())

	connPool, err = connection_pool.ConnectWithOpts(servers, tarantool.Opts{}, connection_pool.OptsPool{})
	require.Nilf(t, connPool, "conn is not nil with incorrect param")
	require.NotNilf(t, err, "err is nil with incorrect params")
	require.Equal(t, "wrong check timeout, must be greater than 0", err.Error())
}

func TestConnSuccessfully(t *testing.T) {
	server := servers[0]
	connPool, err := connection_pool.Connect([]string{"err", server}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.CheckPoolStatuses(args)
	require.Nil(t, err)
}

func TestReconnect(t *testing.T) {
	server := servers[0]

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	test_helpers.StopTarantoolWithCleanup(instances[0])

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.ANY,
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
		Mode:               connection_pool.ANY,
		Servers:            []string{server},
		ExpectedPoolStatus: true,
		ExpectedStatuses: map[string]bool{
			server: true,
		},
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestDisconnectAll(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := connection_pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	test_helpers.StopTarantoolWithCleanup(instances[0])
	test_helpers.StopTarantoolWithCleanup(instances[1])

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.ANY,
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
		Mode:               connection_pool.ANY,
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

func TestClose(t *testing.T) {
	server1 := servers[0]
	server2 := servers[1]

	connPool, err := connection_pool.Connect([]string{server1, server2}, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	args := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.ANY,
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
		Mode:               connection_pool.ANY,
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

func TestCall17(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	resp, err := connPool.Call17("box.info", []interface{}{}, connection_pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val := resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Call17("box.info", []interface{}{}, connection_pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Call17("box.info", []interface{}{}, connection_pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Call17("box.info", []interface{}{}, connection_pool.RW)
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	resp, err := connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.PreferRO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok := resp.Data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, val, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.PreferRW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, val, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.RO)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, val, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Eval("return box.info().ro", []interface{}{}, connection_pool.RW)
	require.Nilf(t, err, "failed to Eval")
	require.NotNilf(t, resp, "response is nil after Eval")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Eval")

	val, ok = resp.Data[0].(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, val, "expected `false` with mode `RW`")
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRO,
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// RO
	_, err = connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RO)
	require.NotNilf(t, err, "expected to fail after Eval, but error is nil")
	require.Equal(t, "Can't find ro instance in pool", err.Error())

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRO,
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// RW
	_, err = connPool.Eval("return box.cfg.listen", []interface{}{}, connection_pool.RW)
	require.NotNilf(t, err, "expected to fail after Eval, but error is nil")
	require.Equal(t, "Can't find rw instance in pool", err.Error())

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRO,
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// ANY
	args := test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: allPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.ANY,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RO,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRW,
	}

	err = test_helpers.ProcessListenOnInstance(args)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRO,
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
		Mode:          connection_pool.ANY,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// RO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.RO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRW
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: masterPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRW,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	// PreferRO
	args = test_helpers.ListenOnInstanceArgs{
		ServersNumber: serversNumber,
		ExpectedPorts: replicaPorts,
		ConnPool:      connPool,
		Mode:          connection_pool.PreferRO,
	}

	err = test_helpers.Retry(test_helpers.ProcessListenOnInstance, args, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
}

func TestInsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

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
	defer conn.Close()

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"rw_insert_key"})
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

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"preferRW_insert_key"})
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"delete_key", "delete_value"})
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

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"delete_key"})
	require.Nilf(t, err, "failed to Select")
	require.NotNilf(t, resp, "response is nil after Select")
	require.Equalf(t, 0, len(resp.Data), "response Body len != 0 after Select")
}

func TestUpsert(t *testing.T) {
	roles := []bool{true, true, false, true, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close()

	// Mode is `RW` by default, we have only one RW instance (servers[2])
	resp, err := connPool.Upsert(spaceName, []interface{}{"upsert_key", "upsert_value"}, []interface{}{[]interface{}{"=", 1, "new_value"}})
	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, resp, "response is nil after Upsert")

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"upsert_key"})
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
		[]interface{}{[]interface{}{"=", 1, "new_value"}}, connection_pool.PreferRW)

	require.Nilf(t, err, "failed to Upsert")
	require.NotNilf(t, resp, "response is nil after Upsert")

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"upsert_key"})
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"update_key", "update_value"})
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

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"update_key"})
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
		[]interface{}{[]interface{}{"=", 1, "another_value"}}, connection_pool.PreferRW)

	require.Nilf(t, err, "failed to Update")
	require.NotNilf(t, resp, "response is nil after Update")

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"update_key"})
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// Connect to servers[2] to check if tuple
	// was inserted on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close()

	resp, err := conn.Insert(spaceNo, []interface{}{"replace_key", "replace_value"})
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

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"new_key"})
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
	resp, err = connPool.Replace(spaceNo, []interface{}{"new_key", "new_value"}, connection_pool.PreferRW)
	require.Nilf(t, err, "failed to Replace")
	require.NotNilf(t, resp, "response is nil after Replace")

	resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{"new_key"})
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

	connPool, err := connection_pool.Connect(servers, connOpts)
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
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, connection_pool.PreferRO)
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
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, connection_pool.PreferRW)
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
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, roKey, connection_pool.RO)
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
	resp, err = connPool.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, rwKey, connection_pool.RW)
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// ANY
	resp, err := connPool.Ping(connection_pool.ANY)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RW
	resp, err = connPool.Ping(connection_pool.RW)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RO
	resp, err = connPool.Ping(connection_pool.RO)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRW
	resp, err = connPool.Ping(connection_pool.PreferRW)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRO
	resp, err = connPool.Ping(connection_pool.PreferRO)
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")
}

func TestDo(t *testing.T) {
	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	req := tarantool.NewPingRequest()
	// ANY
	resp, err := connPool.Do(req, connection_pool.ANY).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RW
	resp, err = connPool.Do(req, connection_pool.RW).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// RO
	resp, err = connPool.Do(req, connection_pool.RO).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRW
	resp, err = connPool.Do(req, connection_pool.PreferRW).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")

	// PreferRO
	resp, err = connPool.Do(req, connection_pool.PreferRO).Get()
	require.Nilf(t, err, "failed to Ping")
	require.NotNilf(t, resp, "response is nil after Ping")
}

func TestNewPrepared(t *testing.T) {
	test_helpers.SkipIfSQLUnsupported(t)

	roles := []bool{true, true, false, true, false}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	stmt, err := connPool.NewPrepared("SELECT NAME0, NAME1 FROM SQL_TEST WHERE NAME0=:id AND NAME1=:name;", connection_pool.RO)
	require.Nilf(t, err, "fail to prepare statement: %v", err)

	if connPool.GetPoolInfo()[stmt.Conn.Addr()].ConnRole != connection_pool.RO {
		t.Errorf("wrong role for the statement's connection")
	}

	executeReq := tarantool.NewExecutePreparedRequest(stmt)
	unprepareReq := tarantool.NewUnprepareRequest(stmt)

	resp, err := connPool.Do(executeReq.Args([]interface{}{1, "test"}), connection_pool.ANY).Get()
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
	resp, err = connPool.Do(unprepareReq, connection_pool.ANY).Get()
	if err != nil {
		t.Errorf("failed to unprepare prepared statement: %v", err)
	}
	if resp.Code != tarantool.OkCode {
		t.Errorf("failed to unprepare prepared statement: code %d", resp.Code)
	}

	_, err = connPool.Do(unprepareReq, connection_pool.ANY).Get()
	if err == nil {
		t.Errorf("the statement must be already unprepared")
	}
	require.Contains(t, err.Error(), "Prepared statement with id")

	_, err = connPool.Do(executeReq, connection_pool.ANY).Get()
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	req := test_helpers.NewStrangerRequest()

	_, err = connPool.Do(req, connection_pool.ANY).Get()
	if err == nil {
		t.Fatalf("nil error catched")
	}
	if err.Error() != expectedErr.Error() {
		t.Fatalf("Unexpected error catched")
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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(connection_pool.PreferRW)
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
	defer conn.Close()

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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(connection_pool.PreferRW)
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
	defer conn.Close()

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

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")
	defer connPool.Close()

	stream, err := connPool.NewStream(connection_pool.PreferRW)
	require.Nilf(t, err, "failed to create stream")
	require.NotNilf(t, connPool, "stream is nil after NewStream")

	// Connect to servers[2] to check if tuple
	// was not inserted outside of stream on RW instance
	conn := test_helpers.ConnectWithValidation(t, servers[2], connOpts)
	defer conn.Close()

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
	workDirs := []string{
		"work_dir1", "work_dir2",
		"work_dir3", "work_dir4",
		"work_dir5"}

	// Tarantool supports streams and interactive transactions since version 2.10.0
	isStreamUnsupported, err := test_helpers.IsTarantoolVersionLess(2, 10, 0)
	if err != nil {
		log.Fatalf("Could not check the Tarantool version")
	}

	instances, err = test_helpers.StartTarantoolInstances(servers, workDirs, test_helpers.StartOpts{
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
