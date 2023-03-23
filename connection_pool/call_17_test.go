//go:build go_tarantool_call_17
// +build go_tarantool_call_17

package connection_pool_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

func TestCall(t *testing.T) {
	roles := []bool{false, true, false, false, true}

	err := test_helpers.SetClusterRO(servers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(servers, connOpts)
	require.Nilf(t, err, "failed to connect")
	require.NotNilf(t, connPool, "conn is nil after Connect")

	defer connPool.Close()

	// PreferRO
	resp, err := connPool.Call("box.info", []interface{}{}, connection_pool.PreferRO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val := resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok := val.(bool)
	require.Truef(t, ok, "expected `true` with mode `PreferRO`")
	require.Truef(t, ro, "expected `true` with mode `PreferRO`")

	// PreferRW
	resp, err = connPool.Call("box.info", []interface{}{}, connection_pool.PreferRW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `PreferRW`")
	require.Falsef(t, ro, "expected `false` with mode `PreferRW`")

	// RO
	resp, err = connPool.Call("box.info", []interface{}{}, connection_pool.RO)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `true` with mode `RO`")
	require.Truef(t, ro, "expected `true` with mode `RO`")

	// RW
	resp, err = connPool.Call("box.info", []interface{}{}, connection_pool.RW)
	require.Nilf(t, err, "failed to Call")
	require.NotNilf(t, resp, "response is nil after Call")
	require.GreaterOrEqualf(t, len(resp.Data), 1, "response.Data is empty after Call")

	val = resp.Data[0].(map[interface{}]interface{})["ro"]
	ro, ok = val.(bool)
	require.Truef(t, ok, "expected `false` with mode `RW`")
	require.Falsef(t, ro, "expected `false` with mode `RW`")
}
