package test_helpers

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/pool"
)

type ListenOnInstanceArgs struct {
	ConnPool      *pool.ConnectionPool
	Mode          pool.Mode
	ServersNumber int
	ExpectedPorts map[string]bool
}

type CheckStatusesArgs struct {
	ConnPool           *pool.ConnectionPool
	Servers            []string
	Mode               pool.Mode
	ExpectedPoolStatus bool
	ExpectedStatuses   map[string]bool
}

func compareTuples(expectedTpl []interface{}, actualTpl []interface{}) error {
	if len(actualTpl) != len(expectedTpl) {
		return fmt.Errorf("unexpected body of Insert (tuple len)")
	}

	for i, field := range actualTpl {
		if field != expectedTpl[i] {
			return fmt.Errorf("unexpected field, expected: %v actual: %v", expectedTpl[i], field)
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

	poolInfo := checkArgs.ConnPool.GetInfo()
	for _, server := range checkArgs.Servers {
		status := poolInfo[server].ConnectedNow
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
		req := tarantool.NewEvalRequest("return box.cfg.listen")
		data, err := listenArgs.ConnPool.Do(req, listenArgs.Mode).Get()
		if err != nil {
			return fmt.Errorf("fail to Eval: %s", err.Error())
		}
		if len(data) < 1 {
			return fmt.Errorf("response.Data is empty after Eval")
		}

		port, ok := data[0].(string)
		if !ok {
			return fmt.Errorf("response.Data is incorrect after Eval")
		}

		actualPorts[port] = true
	}

	equal := reflect.DeepEqual(actualPorts, listenArgs.ExpectedPorts)
	if !equal {
		return fmt.Errorf("expected ports: %v, actual ports: %v",
			listenArgs.ExpectedPorts, actualPorts)
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

func InsertOnInstance(ctx context.Context, dialer tarantool.Dialer, connOpts tarantool.Opts,
	space interface{}, tuple interface{}) error {
	conn, err := tarantool.Connect(ctx, dialer, connOpts)
	if err != nil {
		return fmt.Errorf("fail to connect: %s", err.Error())
	}
	if conn == nil {
		return fmt.Errorf("conn is nil after Connect")
	}
	defer conn.Close()

	data, err := conn.Do(tarantool.NewInsertRequest(space).Tuple(tuple)).Get()
	if err != nil {
		return fmt.Errorf("failed to Insert: %s", err.Error())
	}
	if len(data) != 1 {
		return fmt.Errorf("response Body len != 1")
	}
	if tpl, ok := data[0].([]interface{}); !ok {
		return fmt.Errorf("unexpected body of Insert")
	} else {
		expectedTpl, ok := tuple.([]interface{})
		if !ok {
			return fmt.Errorf("failed to cast")
		}

		err = compareTuples(expectedTpl, tpl)
		if err != nil {
			return err
		}
	}

	return nil
}

func InsertOnInstances(
	dialers []tarantool.Dialer,
	connOpts tarantool.Opts,
	space interface{},
	tuple interface{}) error {
	serversNumber := len(dialers)
	roles := make([]bool, serversNumber)
	for i := 0; i < serversNumber; i++ {
		roles[i] = false
	}

	err := SetClusterRO(dialers, connOpts, roles)
	if err != nil {
		return fmt.Errorf("fail to set roles for cluster: %s", err.Error())
	}

	for _, dialer := range dialers {
		ctx, cancel := GetConnectContext()
		err := InsertOnInstance(ctx, dialer, connOpts, space, tuple)
		cancel()
		if err != nil {
			return err
		}
	}

	return nil
}

func SetInstanceRO(ctx context.Context, dialer tarantool.Dialer, connOpts tarantool.Opts,
	isReplica bool) error {
	conn, err := tarantool.Connect(ctx, dialer, connOpts)
	if err != nil {
		return err
	}

	defer conn.Close()

	req := tarantool.NewCallRequest("box.cfg").
		Args([]interface{}{map[string]bool{"read_only": isReplica}})
	if _, err := conn.Do(req).Get(); err != nil {
		return err
	}

	return nil
}

func SetClusterRO(dialers []tarantool.Dialer, connOpts tarantool.Opts,
	roles []bool) error {
	if len(dialers) != len(roles) {
		return fmt.Errorf("number of servers should be equal to number of roles")
	}

	for i, dialer := range dialers {
		ctx, cancel := GetConnectContext()
		err := SetInstanceRO(ctx, dialer, connOpts, roles[i])
		cancel()
		if err != nil {
			return err
		}
	}

	return nil
}

func StartTarantoolInstances(instsOpts []StartOpts) ([]*TarantoolInstance, error) {
	instances := make([]*TarantoolInstance, 0, len(instsOpts))

	for _, opts := range instsOpts {
		instance, err := StartTarantool(opts)
		if err != nil {
			StopTarantoolInstances(instances)
			return nil, err
		}

		instances = append(instances, instance)
	}

	return instances, nil
}

func StopTarantoolInstances(instances []*TarantoolInstance) {
	for _, instance := range instances {
		StopTarantoolWithCleanup(instance)
	}
}

func GetPoolConnectContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 500*time.Millisecond)
}
