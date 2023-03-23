package connection_pool_test

import (
	"bytes"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"text/template"
	"time"
)

func TestUpdateRWInstances(t *testing.T) {
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

const getClusterMembersTemplate = `
function get_cluster_members()
	ret = {}
{{range $key, $value := .}}
    s = {}
	s["uri"] = "{{$key}}"
	{{if $value}}
    s["type"] = "writeable"
	{{else}}
	s["type"] = 'non-writable'
	{{end}}
	ret[#ret+1]=s
{{end}}
    return ret
end
`

func TestNewConnectionAddedToCluster(t *testing.T) {
	roles := []bool{false, true, false}
	initiallySetupServers := servers[:3]
	allPorts := map[string]bool{
		servers[0]: true,
		servers[1]: true,
		servers[2]: true,
		// Addition of [3], [4] postponed (at runtime)
	}

	masterPorts := map[string]bool{
		servers[0]: true,
		servers[2]: true,
	}

	replicaPorts := map[string]bool{
		servers[1]: true,
	}

	serversNumber := len(initiallySetupServers)

	err := test_helpers.SetClusterRO(initiallySetupServers, connOpts, roles)
	require.Nilf(t, err, "fail to set roles for cluster")

	connPool, err := connection_pool.Connect(initiallySetupServers, connOpts)
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

	getClusterMembers, err := template.New("get_cluster_members").Parse(getClusterMembersTemplate)
	require.NoError(t, err)
	serversMap := make(map[string]bool)
	for _, server := range servers {
		_, isWriteable := masterPorts[server]
		serversMap[server] = isWriteable
	}
	buf := bytes.NewBuffer([]byte{})
	require.NoError(t, getClusterMembers.Execute(buf, serversMap))
	_, err = connPool.Eval(buf.String(), []interface{}{}, connection_pool.RW)
	require.NoError(t, err)
	time.Sleep(time.Second)
	checkConnectedArgs := test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.ANY,
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

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, checkConnectedArgs, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)
	assert.Equal(t, servers, connPool.GetAddrs())

	checkConnectedArgs = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.RW,
		Servers:            []string{servers[1], servers[4]},
		ExpectedPoolStatus: true,
		ExpectedStatuses:   masterPorts,
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, checkConnectedArgs, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

	checkConnectedArgs = test_helpers.CheckStatusesArgs{
		ConnPool:           connPool,
		Mode:               connection_pool.RO,
		Servers:            []string{servers[0], servers[2], servers[3]},
		ExpectedPoolStatus: true,
		ExpectedStatuses:   replicaPorts,
	}

	err = test_helpers.Retry(test_helpers.CheckPoolStatuses, checkConnectedArgs, defaultCountRetry, defaultTimeoutRetry)
	require.Nil(t, err)

}
