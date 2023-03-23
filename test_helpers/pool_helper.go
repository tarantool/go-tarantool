package test_helpers

import (
	"fmt"
	"reflect"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
)

type ListenOnInstanceArgs struct {
	ConnPool      *connection_pool.ConnectionPool
	Mode          connection_pool.Mode
	ServersNumber int
	ExpectedPorts map[string]bool
}

type CheckStatusesArgs struct {
	ConnPool           *connection_pool.ConnectionPool
	Servers            []string
	Mode               connection_pool.Mode
	ExpectedPoolStatus bool
	ExpectedStatuses   map[string]bool
}

func compareTuples(expectedTpl []interface{}, actualTpl []interface{}) error {
	if len(actualTpl) != len(expectedTpl) {
		return fmt.Errorf("Unexpected body of Insert (tuple len)")
	}

	for i, field := range actualTpl {
		if field != expectedTpl[i] {
			return fmt.Errorf("Unexpected field, expected: %v actual: %v", expectedTpl[i], field)
		}
	}

	return nil
}

func CheckPoolStatuses(args interface{}) error {
	checkArgs, ok := args.(CheckStatusesArgs)
	if !ok {
		return fmt.Errorf("incorrect args")
	}

	connected, _ := checkArgs.ConnPool.ConnectedNow(checkArgs.Mode)
	if connected != checkArgs.ExpectedPoolStatus {
		return fmt.Errorf(
			"incorrect connection pool status: expected status %t actual status %t",
			checkArgs.ExpectedPoolStatus, connected)
	}

	poolInfo := checkArgs.ConnPool.GetPoolInfo()
	for _, server := range checkArgs.Servers {
		status := poolInfo[server] != nil && poolInfo[server].ConnectedNow
		if checkArgs.ExpectedStatuses[server] != status {
			return fmt.Errorf(
				"incorrect conn status: addr %s expected status %t actual status %t",
				server, checkArgs.ExpectedStatuses[server], status)
		}
	}

	return nil
}

// ProcessListenOnInstance helper calls "return box.cfg.listen"
// as many times as there are servers in the connection pool
// with specified mode.
// For RO mode expected received ports equals to replica ports.
// For RW mode expected received ports equals to master ports.
// For PreferRO mode expected received ports equals to replica
// ports or to all ports.
// For PreferRW mode expected received ports equals to master ports
// or to all ports.
func ProcessListenOnInstance(args interface{}) error {
	actualPorts := map[string]bool{}

	listenArgs, ok := args.(ListenOnInstanceArgs)
	if !ok {
		return fmt.Errorf("incorrect args")
	}

	for i := 0; i < listenArgs.ServersNumber; i++ {
		resp, err := listenArgs.ConnPool.Eval("return box.cfg.listen", []interface{}{}, listenArgs.Mode)
		if err != nil {
			return fmt.Errorf("fail to Eval: %s", err.Error())
		}
		if resp == nil {
			return fmt.Errorf("response is nil after Eval")
		}
		if len(resp.Data) < 1 {
			return fmt.Errorf("response.Data is empty after Eval")
		}

		port, ok := resp.Data[0].(string)
		if !ok {
			return fmt.Errorf("response.Data is incorrect after Eval")
		}

		actualPorts[port] = true
	}

	equal := reflect.DeepEqual(actualPorts, listenArgs.ExpectedPorts)
	if !equal {
		return fmt.Errorf("expected ports: %v, actual ports: %v", actualPorts, listenArgs.ExpectedPorts)
	}

	return nil
}

func Retry(f func(interface{}) error, args interface{}, count int, timeout time.Duration) error {
	var err error

	for i := 0; ; i++ {
		err = f(args)
		if err == nil {
			return err
		}

		if i >= (count - 1) {
			break
		}

		time.Sleep(timeout)
	}

	return err
}

func InsertOnInstance(server string, connOpts tarantool.Opts, space interface{}, tuple interface{}) error {
	conn, err := tarantool.Connect(server, connOpts)
	if err != nil {
		return fmt.Errorf("Fail to connect to %s: %s", server, err.Error())
	}
	if conn == nil {
		return fmt.Errorf("conn is nil after Connect")
	}
	defer conn.Close()

	resp, err := conn.Insert(space, tuple)
	if err != nil {
		return fmt.Errorf("Failed to Insert: %s", err.Error())
	}
	if resp == nil {
		return fmt.Errorf("Response is nil after Insert")
	}
	if len(resp.Data) != 1 {
		return fmt.Errorf("Response Body len != 1")
	}
	if tpl, ok := resp.Data[0].([]interface{}); !ok {
		return fmt.Errorf("Unexpected body of Insert")
	} else {
		expectedTpl, ok := tuple.([]interface{})
		if !ok {
			return fmt.Errorf("Failed to cast")
		}

		err = compareTuples(expectedTpl, tpl)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertOnInstances(servers []string, connOpts tarantool.Opts, space interface{}, tuple interface{}) error {
	serversNumber := len(servers)
	roles := make([]bool, serversNumber)
	for i := 0; i < serversNumber; i++ {
		roles[i] = false
	}

	err := SetClusterRO(servers, connOpts, roles)
	if err != nil {
		return fmt.Errorf("fail to set roles for cluster: %s", err.Error())
	}

	for _, server := range servers {
		err := InsertOnInstance(server, connOpts, space, tuple)
		if err != nil {
			return err
		}
	}

	return nil
}

func SetInstanceRO(server string, connOpts tarantool.Opts, isReplica bool) error {
	conn, err := tarantool.Connect(server, connOpts)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.Call17("box.cfg", []interface{}{map[string]bool{"read_only": isReplica}})
	if err != nil {
		return err
	}

	return nil
}

func SetClusterRO(servers []string, connOpts tarantool.Opts, roles []bool) error {
	if len(servers) != len(roles) {
		return fmt.Errorf("number of servers should be equal to number of roles")
	}

	for i, server := range servers {
		err := SetInstanceRO(server, connOpts, roles[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func StartTarantoolInstances(servers []string, workDirs []string, opts StartOpts) ([]TarantoolInstance, error) {
	isUserWorkDirs := (workDirs != nil)
	if isUserWorkDirs && (len(servers) != len(workDirs)) {
		return nil, fmt.Errorf("number of servers should be equal to number of workDirs")
	}

	instances := make([]TarantoolInstance, 0, len(servers))

	for i, server := range servers {
		opts.Listen = server
		if isUserWorkDirs {
			opts.WorkDir = workDirs[i]
		} else {
			opts.WorkDir = ""
		}

		instance, err := StartTarantool(opts)
		if err != nil {
			StopTarantoolInstances(instances)
			return nil, err
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func StopTarantoolInstances(instances []TarantoolInstance) {
	for _, instance := range instances {
		StopTarantoolWithCleanup(instance)
	}
}
