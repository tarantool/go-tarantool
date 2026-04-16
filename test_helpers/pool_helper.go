package test_helpers

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/box"
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

func comparePortMaps(expected map[string]bool, actual map[string]bool) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("unexpected number of ports: expected %d, actual %d",
			len(expected), len(actual))
	}

	for port, expectedValue := range expected {
		actualValue, ok := actual[port]
		if !ok {
			return fmt.Errorf("missing expected port: %s", port)
		}
		if actualValue != expectedValue {
			return fmt.Errorf("unexpected value for port %s: expected %t, actual %t",
				port, expectedValue, actualValue)
		}
	}

	return nil
}

func CheckPoolStatuses(args CheckStatusesArgs) error {
	connected, _ := args.ConnPool.ConnectedNow(args.Mode)
	if connected != args.ExpectedPoolStatus {
		return fmt.Errorf(
			"incorrect connection pool status: expected status %t actual status %t",
			args.ExpectedPoolStatus, connected)
	}

	poolInfo := args.ConnPool.GetInfo()
	for _, server := range args.Servers {
		status := poolInfo[server].ConnectedNow
		if args.ExpectedStatuses[server] != status {
			return fmt.Errorf(
				"incorrect conn status: addr %s expected status %t actual status %t",
				server, args.ExpectedStatuses[server], status)
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
func ProcessListenOnInstance(args ListenOnInstanceArgs) error {
	actualPorts := map[string]bool{}

	for i := 0; i < args.ServersNumber; i++ {
		req := tarantool.NewEvalRequest("return box.cfg.listen")
		var data []string
		err := args.ConnPool.Do(req, args.Mode).GetTyped(&data)
		if err != nil {
			return fmt.Errorf("failed to get response: %w", err)
		}
		if len(data) == 0 {
			return errors.New("empty response from Eval")
		}

		actualPorts[data[0]] = true
	}

	if err := comparePortMaps(args.ExpectedPorts, actualPorts); err != nil {
		return err
	}

	return nil
}

func InsertOnInstance(ctx context.Context, dialer tarantool.Dialer, connOpts tarantool.Opts,
	space interface{}, tuple interface{}) error {
	conn, err := tarantool.Connect(ctx, dialer, connOpts)

	switch {
	case err != nil:
		return fmt.Errorf("fail to connect: %w", err)
	case conn == nil:
		return fmt.Errorf("conn is nil after Connect")
	}

	defer func() { _ = conn.Close() }()

	data, err := conn.Do(tarantool.NewInsertRequest(space).Tuple(tuple)).Get()

	switch {
	case err != nil:
		return fmt.Errorf("failed to Insert: %w", err)
	case len(data) != 1:
		return fmt.Errorf("response Body len != 1")
	}

	tpl, ok := data[0].([]interface{})
	if !ok {
		return fmt.Errorf("unexpected body of Insert")
	}

	expectedTpl, ok := tuple.([]interface{})
	if !ok {
		return fmt.Errorf("failed to cast")
	}

	if err := compareTuples(expectedTpl, tpl); err != nil {
		return err
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
		return fmt.Errorf("fail to set roles for cluster: %w", err)
	}

	return ExecuteOnAll(ctx, dialers, func(_ context.Context, d tarantool.Dialer, _ int) error {
		return InsertOnInstance(ctx, d, connOpts, space, tuple)
	})
}

func SetInstanceRO(ctx context.Context, dialer tarantool.Dialer, connOpts tarantool.Opts,
	isReplica bool) error {
	conn, err := tarantool.Connect(ctx, dialer, connOpts)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	req := tarantool.NewCallRequest("box.cfg").
		Args([]interface{}{map[string]bool{"read_only": isReplica}})
	if _, err := conn.Do(req).Get(); err != nil {
		return err
	}

	checkRole := func(conn *tarantool.Connection, isReplica bool) error {
		boxInfo, err := box.MustNew(conn).Info()
		if err != nil {
			return fmt.Errorf("failed to get box.info: %s", err)
		}

		switch {
		case boxInfo.Status != "running":
			return fmt.Errorf("box.info.status='%s' (waiting for 'running')", boxInfo.Status)
		case boxInfo.RO != isReplica:
			return fmt.Errorf("box.info.ro='%v' (waiting for '%v')", boxInfo.RO, isReplica)
		default:
			return nil
		}
	}

	err = errors.New("not checked yet")

	// Wait for the role to be applied.
	for err != nil {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return fmt.Errorf("%w: failed to apply role, the last error: %s",
				ctx.Err(), err)
		}

		err = checkRole(conn, isReplica)
	}

	return nil
}

func ExecuteOnAll(ctx context.Context, dialers []tarantool.Dialer, fn func(context.Context, tarantool.Dialer, int) error) error {
	var wg sync.WaitGroup
	var errs []error
	var mu sync.Mutex

	wg.Add(len(dialers))
	for i, dialer := range dialers {
		go func(idx int, d tarantool.Dialer) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				mu.Lock()
				errs = append(errs, fmt.Errorf("instance %d: %w", idx, ctx.Err()))
				mu.Unlock()
			default:
				if err := fn(ctx, d, idx); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Errorf("instance %d: %w", idx, err))
					mu.Unlock()
				}
			}
		}(i, dialer)
	}

	wg.Wait()
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func SetClusterRO(ctx context.Context, dialers []tarantool.Dialer, connOpts tarantool.Opts,
	roles []bool) error {
	if len(dialers) != len(roles) {
		return fmt.Errorf("number of servers should be equal to number of roles")
	}

	return ExecuteOnAll(ctx, dialers, func(_ context.Context, d tarantool.Dialer, idx int) error {
		return SetInstanceRO(ctx, d, connOpts, roles[idx])
	})
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
	return context.WithTimeout(context.Background(), time.Second)
}
