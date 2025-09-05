package test_helpers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
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
	ctx context.Context,
	dialers []tarantool.Dialer,
	connOpts tarantool.Opts,
	space interface{},
	tuple interface{}) error {
	serversNumber := len(dialers)
	roles := make([]bool, serversNumber)
	for i := 0; i < serversNumber; i++ {
		roles[i] = false
	}

	err := SetClusterRO(ctx, dialers, connOpts, roles)
	if err != nil {
		return fmt.Errorf("fail to set roles for cluster: %s", err.Error())
	}

	errs := make([]error, len(dialers))
	var wg sync.WaitGroup
	wg.Add(len(dialers))
	for i, dialer := range dialers {
		// Pass loop variable(s) to avoid its capturing by reference (not needed since Go 1.22).
		go func(i int, dialer tarantool.Dialer) {
			defer wg.Done()
			errs[i] = InsertOnInstance(ctx, dialer, connOpts, space, tuple)
		}(i, dialer)
	}
	wg.Wait()

	return errors.Join(errs...)
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

	checkRole := func(conn *tarantool.Connection, isReplica bool) string {
		data, err := conn.Do(tarantool.NewCallRequest("box.info")).Get()
		switch {
		case err != nil:
			return fmt.Sprintf("failed to get box.info: %s", err)
		case len(data) < 1:
			return "box.info is empty"
		}

		boxInfo, ok := data[0].(map[interface{}]interface{})
		if !ok {
			return "unexpected type in box.info response"
		}

		status, statusFound := boxInfo["status"]
		readonly, readonlyFound := boxInfo["ro"]
		switch {
		case !statusFound:
			return "box.info.status is missing"
		case status != "running":
			return fmt.Sprintf("box.info.status='%s' (waiting for 'running')", status)
		case !readonlyFound:
			return "box.info.ro is missing"
		case readonly != isReplica:
			return fmt.Sprintf("box.info.ro='%v' (waiting for '%v')", readonly, isReplica)
		default:
			return ""
		}
	}

	problem := "not checked yet"

	// Wait for the role to be applied.
	for len(problem) != 0 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("%w: failed to apply role, the last problem: %s",
				ctx.Err(), problem)
		}

		problem = checkRole(conn, isReplica)
	}

	return nil
}

func SetClusterRO(ctx context.Context, dialers []tarantool.Dialer, connOpts tarantool.Opts,
	roles []bool) error {
	if len(dialers) != len(roles) {
		return fmt.Errorf("number of servers should be equal to number of roles")
	}

	// Apply roles in parallel.
	errs := make([]error, len(dialers))
	var wg sync.WaitGroup
	wg.Add(len(dialers))
	for i, dialer := range dialers {
		// Pass loop variable(s) to avoid its capturing by reference (not needed since Go 1.22).
		go func(i int, dialer tarantool.Dialer) {
			defer wg.Done()
			errs[i] = SetInstanceRO(ctx, dialer, connOpts, roles[i])
		}(i, dialer)
	}
	wg.Wait()

	return errors.Join(errs...)
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
