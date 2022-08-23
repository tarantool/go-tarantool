// Package with methods to work with a Tarantool cluster
// considering master discovery.
//
// Main features:
//
// - Return available connection from pool according to round-robin strategy.
//
// - Automatic master discovery by mode parameter.
//
// Since: 1.6.0
package connection_pool

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tarantool/go-tarantool"
)

var (
	ErrEmptyAddrs        = errors.New("addrs (first argument) should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
	ErrTooManyArgs       = errors.New("too many arguments")
	ErrIncorrectResponse = errors.New("incorrect response format")
	ErrIncorrectStatus   = errors.New("incorrect instance status: status should be `running`")
	ErrNoRwInstance      = errors.New("can't find rw instance in pool")
	ErrNoRoInstance      = errors.New("can't find ro instance in pool")
	ErrNoHealthyInstance = errors.New("can't find healthy instance in pool")
)

// ConnectionHandler provides callbacks for components interested in handling
// changes of connections in a ConnectionPool.
type ConnectionHandler interface {
	// Discovered is called when a connection with a role has been detected
	// (for the first time or when a role of a connection has been changed),
	// but is not yet available to send requests. It allows for a client to
	// initialize the connection before using it in a pool.
	//
	// The client code may cancel adding a connection to the pool. The client
	// need to return an error from the Discovered call for that. In this case
	// the pool will close connection and will try to reopen it later.
	Discovered(conn *tarantool.Connection, role Role) error
	// Deactivated is called when a connection with a role has become
	// unavaileble to send requests. It happens if the connection is closed or
	// the connection role is switched.
	//
	// So if a connection switches a role, a pool calls:
	// Deactivated() + Discovered().
	//
	// Deactivated will not be called if a previous Discovered() call returns
	// an error. Because in this case, the connection does not become available
	// for sending requests.
	Deactivated(conn *tarantool.Connection, role Role) error
}

// OptsPool provides additional options (configurable via ConnectWithOpts).
type OptsPool struct {
	// Timeout for timer to reopen connections that have been closed by some
	// events and to relocate connection between subpools if ro/rw role has
	// been updated.
	CheckTimeout time.Duration
	// ConnectionHandler provides an ability to handle connection updates.
	ConnectionHandler ConnectionHandler
}

/*
ConnectionInfo structure for information about connection statuses:

- ConnectedNow reports if connection is established at the moment.

- ConnRole reports master/replica role of instance.
*/
type ConnectionInfo struct {
	ConnectedNow bool
	ConnRole     Role
}

/*
Main features:

- Return available connection from pool according to round-robin strategy.

- Automatic master discovery by mode parameter.
*/
type ConnectionPool struct {
	addrs    []string
	connOpts tarantool.Opts
	opts     OptsPool

	state      state
	done       chan struct{}
	roPool     *RoundRobinStrategy
	rwPool     *RoundRobinStrategy
	anyPool    *RoundRobinStrategy
	poolsMutex sync.RWMutex
}

var _ Pooler = (*ConnectionPool)(nil)

type connState struct {
	addr   string
	notify chan tarantool.ConnEvent
	conn   *tarantool.Connection
	role   Role
}

// ConnectWithOpts creates pool for instances with addresses addrs
// with options opts.
func ConnectWithOpts(addrs []string, connOpts tarantool.Opts, opts OptsPool) (connPool *ConnectionPool, err error) {
	if len(addrs) == 0 {
		return nil, ErrEmptyAddrs
	}
	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}

	size := len(addrs)
	rwPool := NewEmptyRoundRobin(size)
	roPool := NewEmptyRoundRobin(size)
	anyPool := NewEmptyRoundRobin(size)

	connPool = &ConnectionPool{
		addrs:    make([]string, 0, len(addrs)),
		connOpts: connOpts,
		opts:     opts,
		state:    unknownState,
		done:     make(chan struct{}),
		rwPool:   rwPool,
		roPool:   roPool,
		anyPool:  anyPool,
	}

	m := make(map[string]bool)
	for _, addr := range addrs {
		if _, ok := m[addr]; !ok {
			m[addr] = true
			connPool.addrs = append(connPool.addrs, addr)
		}
	}

	states, somebodyAlive := connPool.fillPools()
	if !somebodyAlive {
		connPool.state.set(closedState)
		connPool.closeImpl()
		for _, s := range states {
			close(s.notify)
		}
		return nil, ErrNoConnection
	}

	connPool.state.set(connectedState)

	for _, s := range states {
		go connPool.checker(s)
	}

	return connPool, nil
}

// ConnectWithOpts creates pool for instances with addresses addrs.
func Connect(addrs []string, connOpts tarantool.Opts) (connPool *ConnectionPool, err error) {
	opts := OptsPool{
		CheckTimeout: 1 * time.Second,
	}
	return ConnectWithOpts(addrs, connOpts, opts)
}

// ConnectedNow gets connected status of pool.
func (connPool *ConnectionPool) ConnectedNow(mode Mode) (bool, error) {
	connPool.poolsMutex.RLock()
	defer connPool.poolsMutex.RUnlock()

	if connPool.state.get() != connectedState {
		return false, nil
	}
	switch mode {
	case ANY:
		return !connPool.anyPool.IsEmpty(), nil
	case RW:
		return !connPool.rwPool.IsEmpty(), nil
	case RO:
		return !connPool.roPool.IsEmpty(), nil
	case PreferRW:
		fallthrough
	case PreferRO:
		return !connPool.rwPool.IsEmpty() || !connPool.roPool.IsEmpty(), nil
	default:
		return false, ErrNoHealthyInstance
	}
}

// ConfiguredTimeout gets timeout of current connection.
func (connPool *ConnectionPool) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	conn, err := connPool.getNextConnection(mode)
	if err != nil {
		return 0, err
	}

	return conn.ConfiguredTimeout(), nil
}

func (connPool *ConnectionPool) closeImpl() []error {
	errs := make([]error, 0, len(connPool.addrs))

	for _, addr := range connPool.addrs {
		if conn := connPool.anyPool.DeleteConnByAddr(addr); conn != nil {
			if err := conn.Close(); err != nil {
				errs = append(errs, err)
			}

			role := UnknownRole
			if conn := connPool.rwPool.DeleteConnByAddr(addr); conn != nil {
				role = MasterRole
			} else if conn := connPool.roPool.DeleteConnByAddr(addr); conn != nil {
				role = ReplicaRole
			}
			connPool.handlerDeactivated(conn, role)
		}
	}

	close(connPool.done)
	return errs
}

// Close closes connections in pool.
func (connPool *ConnectionPool) Close() []error {
	if connPool.state.cas(connectedState, closedState) {
		connPool.poolsMutex.Lock()
		defer connPool.poolsMutex.Unlock()

		return connPool.closeImpl()
	}
	return nil
}

// GetAddrs gets addresses of connections in pool.
func (connPool *ConnectionPool) GetAddrs() []string {
	cpy := make([]string, len(connPool.addrs))
	copy(cpy, connPool.addrs)
	return cpy
}

// GetPoolInfo gets information of connections (connected status, ro/rw role).
func (connPool *ConnectionPool) GetPoolInfo() map[string]*ConnectionInfo {
	info := make(map[string]*ConnectionInfo)

	connPool.poolsMutex.RLock()
	defer connPool.poolsMutex.RUnlock()

	if connPool.state.get() != connectedState {
		return info
	}

	for _, addr := range connPool.addrs {
		conn, role := connPool.getConnectionFromPool(addr)
		if conn != nil {
			info[addr] = &ConnectionInfo{ConnectedNow: conn.ConnectedNow(), ConnRole: role}
		}
	}

	return info
}

// Ping sends empty request to Tarantool to check connection.
func (connPool *ConnectionPool) Ping(userMode Mode) (*tarantool.Response, error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Ping()
}

// Select performs select to box space.
func (connPool *ConnectionPool) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(ANY, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Select(space, index, offset, limit, iterator, key)
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) Insert(space interface{}, tuple interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Insert(space, tuple)
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) Replace(space interface{}, tuple interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Replace(space, tuple)
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
func (connPool *ConnectionPool) Delete(space, index interface{}, key interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Delete(space, index, key)
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
func (connPool *ConnectionPool) Update(space, index interface{}, key, ops interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Update(space, index, key, ops)
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
func (connPool *ConnectionPool) Upsert(space interface{}, tuple, ops interface{}, userMode ...Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Upsert(space, tuple, ops)
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connPool *ConnectionPool) Call(functionName string, args interface{}, userMode Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call(functionName, args)
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays.
// Deprecated since Tarantool 1.7.2.
func (connPool *ConnectionPool) Call16(functionName string, args interface{}, userMode Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call16(functionName, args)
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array).
func (connPool *ConnectionPool) Call17(functionName string, args interface{}, userMode Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call17(functionName, args)
}

// Eval passes lua expression for evaluation.
func (connPool *ConnectionPool) Eval(expr string, args interface{}, userMode Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Eval(expr, args)
}

// Execute passes sql expression to Tarantool for execution.
func (connPool *ConnectionPool) Execute(expr string, args interface{}, userMode Mode) (resp *tarantool.Response, err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Execute(expr, args)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
func (connPool *ConnectionPool) GetTyped(space, index interface{}, key interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(ANY, userMode)
	if err != nil {
		return err
	}

	return conn.GetTyped(space, index, key, result)
}

// SelectTyped performs select to box space and fills typed result.
func (connPool *ConnectionPool) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(ANY, userMode)
	if err != nil {
		return err
	}

	return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) InsertTyped(space interface{}, tuple interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.InsertTyped(space, tuple, result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) ReplaceTyped(space interface{}, tuple interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.ReplaceTyped(space, tuple, result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
func (connPool *ConnectionPool) DeleteTyped(space, index interface{}, key interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.DeleteTyped(space, index, key, result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
func (connPool *ConnectionPool) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}, userMode ...Mode) (err error) {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.UpdateTyped(space, index, key, ops, result)
}

// CallTyped calls registered function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connPool *ConnectionPool) CallTyped(functionName string, args interface{}, result interface{}, userMode Mode) (err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.CallTyped(functionName, args, result)
}

// Call16Typed calls registered function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays.
// Deprecated since Tarantool 1.7.2.
func (connPool *ConnectionPool) Call16Typed(functionName string, args interface{}, result interface{}, userMode Mode) (err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.Call16Typed(functionName, args, result)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array).
func (connPool *ConnectionPool) Call17Typed(functionName string, args interface{}, result interface{}, userMode Mode) (err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.Call17Typed(functionName, args, result)
}

// EvalTyped passes lua expression for evaluation.
func (connPool *ConnectionPool) EvalTyped(expr string, args interface{}, result interface{}, userMode Mode) (err error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.EvalTyped(expr, args, result)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
func (connPool *ConnectionPool) ExecuteTyped(expr string, args interface{}, result interface{}, userMode Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return tarantool.SQLInfo{}, nil, err
	}

	return conn.ExecuteTyped(expr, args, result)
}

// SelectAsync sends select request to Tarantool and returns Future.
func (connPool *ConnectionPool) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(ANY, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.SelectAsync(space, index, offset, limit, iterator, key)
}

// InsertAsync sends insert action to Tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connPool *ConnectionPool) InsertAsync(space interface{}, tuple interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.InsertAsync(space, tuple)
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (connPool *ConnectionPool) ReplaceAsync(space interface{}, tuple interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.ReplaceAsync(space, tuple)
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (connPool *ConnectionPool) DeleteAsync(space, index interface{}, key interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.DeleteAsync(space, index, key)
}

// UpdateAsync sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (connPool *ConnectionPool) UpdateAsync(space, index interface{}, key, ops interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.UpdateAsync(space, index, key, ops)
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (connPool *ConnectionPool) UpsertAsync(space interface{}, tuple interface{}, ops interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := connPool.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.UpsertAsync(space, tuple, ops)
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connPool *ConnectionPool) CallAsync(functionName string, args interface{}, userMode Mode) *tarantool.Future {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.CallAsync(functionName, args)
}

// Call16Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is always array of arrays.
// Deprecated since Tarantool 1.7.2.
func (connPool *ConnectionPool) Call16Async(functionName string, args interface{}, userMode Mode) *tarantool.Future {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Call16Async(functionName, args)
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, so future's result will not be converted
// (though, keep in mind, result is always array).
func (connPool *ConnectionPool) Call17Async(functionName string, args interface{}, userMode Mode) *tarantool.Future {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Call17Async(functionName, args)
}

// EvalAsync sends a lua expression for evaluation and returns Future.
func (connPool *ConnectionPool) EvalAsync(expr string, args interface{}, userMode Mode) *tarantool.Future {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.EvalAsync(expr, args)
}

// ExecuteAsync sends sql expression to Tarantool for execution and returns
// Future.
func (connPool *ConnectionPool) ExecuteAsync(expr string, args interface{}, userMode Mode) *tarantool.Future {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.ExecuteAsync(expr, args)
}

// Do sends the request and returns a future.
// For requests that belong to an only one connection (e.g. Unprepare or ExecutePrepared)
// the argument of type Mode is unused.
func (connPool *ConnectionPool) Do(req tarantool.Request, userMode Mode) *tarantool.Future {
	if connectedReq, ok := req.(tarantool.ConnectedRequest); ok {
		conn, _ := connPool.getConnectionFromPool(connectedReq.Conn().Addr())
		if conn == nil {
			return newErrorFuture(fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool"))
		}
		return connectedReq.Conn().Do(req)
	}
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Do(req)
}

// NewStream creates new Stream object for connection selected
// by userMode from connPool.
//
// Since v. 2.10.0, Tarantool supports streams and interactive transactions over them.
// To use interactive transactions, memtx_use_mvcc_engine box option should be set to true.
// Since 1.7.0
func (connPool *ConnectionPool) NewStream(userMode Mode) (*tarantool.Stream, error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}
	return conn.NewStream()
}

// NewPrepared passes a sql statement to Tarantool for preparation synchronously.
func (connPool *ConnectionPool) NewPrepared(expr string, userMode Mode) (*tarantool.Prepared, error) {
	conn, err := connPool.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}
	return conn.NewPrepared(expr)
}

//
// private
//

func (connPool *ConnectionPool) getConnectionRole(conn *tarantool.Connection) (Role, error) {
	resp, err := conn.Call17("box.info", []interface{}{})
	if err != nil {
		return UnknownRole, err
	}
	if resp == nil {
		return UnknownRole, ErrIncorrectResponse
	}
	if len(resp.Data) < 1 {
		return UnknownRole, ErrIncorrectResponse
	}

	instanceStatus, ok := resp.Data[0].(map[interface{}]interface{})["status"]
	if !ok {
		return UnknownRole, ErrIncorrectResponse
	}
	if instanceStatus != "running" {
		return UnknownRole, ErrIncorrectStatus
	}

	replicaRole, ok := resp.Data[0].(map[interface{}]interface{})["ro"]
	if !ok {
		return UnknownRole, ErrIncorrectResponse
	}

	switch replicaRole {
	case false:
		return MasterRole, nil
	case true:
		return ReplicaRole, nil
	}

	return UnknownRole, nil
}

func (connPool *ConnectionPool) getConnectionFromPool(addr string) (*tarantool.Connection, Role) {
	if conn := connPool.rwPool.GetConnByAddr(addr); conn != nil {
		return conn, MasterRole
	}

	if conn := connPool.roPool.GetConnByAddr(addr); conn != nil {
		return conn, ReplicaRole
	}

	return connPool.anyPool.GetConnByAddr(addr), UnknownRole
}

func (connPool *ConnectionPool) deleteConnection(addr string) {
	if conn := connPool.anyPool.DeleteConnByAddr(addr); conn != nil {
		if conn := connPool.rwPool.DeleteConnByAddr(addr); conn != nil {
			return
		}
		connPool.roPool.DeleteConnByAddr(addr)
	}
}

func (connPool *ConnectionPool) addConnection(addr string,
	conn *tarantool.Connection, role Role) {

	connPool.anyPool.AddConn(addr, conn)

	switch role {
	case MasterRole:
		connPool.rwPool.AddConn(addr, conn)
	case ReplicaRole:
		connPool.roPool.AddConn(addr, conn)
	}
}

func (connPool *ConnectionPool) handlerDiscovered(conn *tarantool.Connection,
	role Role) bool {
	var err error
	if connPool.opts.ConnectionHandler != nil {
		err = connPool.opts.ConnectionHandler.Discovered(conn, role)
	}

	if err != nil {
		addr := conn.Addr()
		log.Printf("tarantool: storing connection to %s canceled: %s\n", addr, err)
		return false
	}
	return true
}

func (connPool *ConnectionPool) handlerDeactivated(conn *tarantool.Connection,
	role Role) {
	var err error
	if connPool.opts.ConnectionHandler != nil {
		err = connPool.opts.ConnectionHandler.Deactivated(conn, role)
	}

	if err != nil {
		addr := conn.Addr()
		log.Printf("tarantool: deactivating connection to %s by user failed: %s\n", addr, err)
	}
}

func (connPool *ConnectionPool) fillPools() ([]connState, bool) {
	states := make([]connState, len(connPool.addrs))
	somebodyAlive := false

	// It is called before checker() goroutines and before closeImpl() may be
	// called so we don't expect concurrency issues here.
	for i, addr := range connPool.addrs {
		states[i] = connState{
			addr:   addr,
			notify: make(chan tarantool.ConnEvent, 10),
			conn:   nil,
			role:   UnknownRole,
		}
		connOpts := connPool.connOpts
		connOpts.Notify = states[i].notify

		conn, err := tarantool.Connect(addr, connOpts)
		if err != nil {
			log.Printf("tarantool: connect to %s failed: %s\n", addr, err.Error())
		} else if conn != nil {
			role, err := connPool.getConnectionRole(conn)
			if err != nil {
				conn.Close()
				log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err)
				continue
			}

			if connPool.handlerDiscovered(conn, role) {
				connPool.addConnection(addr, conn, role)

				if conn.ConnectedNow() {
					states[i].conn = conn
					states[i].role = role
					somebodyAlive = true
				} else {
					connPool.deleteConnection(addr)
					conn.Close()
					connPool.handlerDeactivated(conn, role)
				}
			} else {
				conn.Close()
			}
		}
	}

	return states, somebodyAlive
}

func (pool *ConnectionPool) updateConnection(s connState) connState {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return s
	}

	if role, err := pool.getConnectionRole(s.conn); err == nil {
		if s.role != role {
			pool.deleteConnection(s.addr)
			pool.poolsMutex.Unlock()

			pool.handlerDeactivated(s.conn, s.role)
			opened := pool.handlerDiscovered(s.conn, role)
			if !opened {
				s.conn.Close()
				s.conn = nil
				s.role = UnknownRole
				return s
			}

			pool.poolsMutex.Lock()
			if pool.state.get() != connectedState {
				pool.poolsMutex.Unlock()

				s.conn.Close()
				pool.handlerDeactivated(s.conn, role)
				s.conn = nil
				s.role = UnknownRole
				return s
			}

			pool.addConnection(s.addr, s.conn, role)
			s.role = role
		}
	}

	pool.poolsMutex.Unlock()
	return s
}

func (pool *ConnectionPool) tryConnect(s connState) connState {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return s
	}

	s.conn = nil
	s.role = UnknownRole

	connOpts := pool.connOpts
	connOpts.Notify = s.notify
	conn, _ := tarantool.Connect(s.addr, connOpts)
	if conn != nil {
		role, err := pool.getConnectionRole(conn)
		pool.poolsMutex.Unlock()

		if err != nil {
			conn.Close()
			log.Printf("tarantool: storing connection to %s failed: %s\n", s.addr, err)
			return s
		}

		opened := pool.handlerDiscovered(conn, role)
		if !opened {
			conn.Close()
			return s
		}

		pool.poolsMutex.Lock()
		if pool.state.get() != connectedState {
			pool.poolsMutex.Unlock()
			conn.Close()
			pool.handlerDeactivated(conn, role)
			return s
		}

		pool.addConnection(s.addr, conn, role)
		s.conn = conn
		s.role = role
	}

	pool.poolsMutex.Unlock()
	return s
}

func (pool *ConnectionPool) reconnect(s connState) connState {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return s
	}

	pool.deleteConnection(s.addr)
	pool.poolsMutex.Unlock()

	pool.handlerDeactivated(s.conn, s.role)
	s.conn = nil
	s.role = UnknownRole

	return pool.tryConnect(s)
}

func (pool *ConnectionPool) checker(s connState) {
	timer := time.NewTicker(pool.opts.CheckTimeout)
	defer timer.Stop()

	for {
		select {
		case <-pool.done:
			close(s.notify)
			return
		case <-s.notify:
			if s.conn != nil && s.conn.ClosedNow() {
				pool.poolsMutex.Lock()
				if pool.state.get() == connectedState {
					pool.deleteConnection(s.addr)
					pool.poolsMutex.Unlock()
					pool.handlerDeactivated(s.conn, s.role)
					s.conn = nil
					s.role = UnknownRole
				} else {
					pool.poolsMutex.Unlock()
				}
			}
		case <-timer.C:
			// Reopen connection
			// Relocate connection between subpools
			// if ro/rw was updated
			if s.conn == nil {
				s = pool.tryConnect(s)
			} else if !s.conn.ClosedNow() {
				s = pool.updateConnection(s)
			} else {
				s = pool.reconnect(s)
			}
		}
	}
}

func (connPool *ConnectionPool) getNextConnection(mode Mode) (*tarantool.Connection, error) {

	switch mode {
	case ANY:
		if next := connPool.anyPool.GetNextConnection(); next != nil {
			return next, nil
		}
	case RW:
		if next := connPool.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
		return nil, ErrNoRwInstance
	case RO:
		if next := connPool.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
		return nil, ErrNoRoInstance
	case PreferRW:
		if next := connPool.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
		if next := connPool.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
	case PreferRO:
		if next := connPool.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
		if next := connPool.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
	}
	return nil, ErrNoHealthyInstance
}

func (connPool *ConnectionPool) getConnByMode(defaultMode Mode, userMode []Mode) (*tarantool.Connection, error) {
	if len(userMode) > 1 {
		return nil, ErrTooManyArgs
	}

	mode := defaultMode
	if len(userMode) > 0 {
		mode = userMode[0]
	}

	return connPool.getNextConnection(mode)
}

func newErrorFuture(err error) *tarantool.Future {
	fut := tarantool.NewFuture()
	fut.SetError(err)
	return fut
}
