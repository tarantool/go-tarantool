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
	ErrExists            = errors.New("endpoint exists")
	ErrClosed            = errors.New("pool is closed")
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
	addrs      map[string]*endpoint
	addrsMutex sync.RWMutex

	connOpts tarantool.Opts
	opts     OptsPool

	state            state
	done             chan struct{}
	roPool           *RoundRobinStrategy
	rwPool           *RoundRobinStrategy
	anyPool          *RoundRobinStrategy
	poolsMutex       sync.RWMutex
	watcherContainer watcherContainer
}

var _ Pooler = (*ConnectionPool)(nil)

type endpoint struct {
	addr   string
	notify chan tarantool.ConnEvent
	conn   *tarantool.Connection
	role   Role
	// This is used to switch a connection states.
	shutdown chan struct{}
	close    chan struct{}
	closed   chan struct{}
	closeErr error
}

func newEndpoint(addr string) *endpoint {
	return &endpoint{
		addr:     addr,
		notify:   make(chan tarantool.ConnEvent, 100),
		conn:     nil,
		role:     UnknownRole,
		shutdown: make(chan struct{}),
		close:    make(chan struct{}),
		closed:   make(chan struct{}),
	}
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
		addrs:    make(map[string]*endpoint),
		connOpts: connOpts.Clone(),
		opts:     opts,
		state:    unknownState,
		done:     make(chan struct{}),
		rwPool:   rwPool,
		roPool:   roPool,
		anyPool:  anyPool,
	}

	for _, addr := range addrs {
		connPool.addrs[addr] = nil
	}

	somebodyAlive := connPool.fillPools()
	if !somebodyAlive {
		connPool.state.set(closedState)
		return nil, ErrNoConnection
	}

	connPool.state.set(connectedState)

	for _, s := range connPool.addrs {
		go connPool.controller(s)
	}

	return connPool, nil
}

// ConnectWithOpts creates pool for instances with addresses addrs.
//
// It is useless to set up tarantool.Opts.Reconnect value for a connection.
// The connection pool has its own reconnection logic. See
// OptsPool.CheckTimeout description.
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

// Add adds a new endpoint with the address into the pool. This function
// adds the endpoint only after successful connection.
func (pool *ConnectionPool) Add(addr string) error {
	e := newEndpoint(addr)

	pool.addrsMutex.Lock()
	// Ensure that Close()/CloseGraceful() not in progress/done.
	if pool.state.get() != connectedState {
		pool.addrsMutex.Unlock()
		return ErrClosed
	}
	if _, ok := pool.addrs[addr]; ok {
		pool.addrsMutex.Unlock()
		return ErrExists
	}
	pool.addrs[addr] = e
	pool.addrsMutex.Unlock()

	if err := pool.tryConnect(e); err != nil {
		pool.addrsMutex.Lock()
		delete(pool.addrs, addr)
		pool.addrsMutex.Unlock()
		close(e.closed)
		return err
	}

	go pool.controller(e)
	return nil
}

// Remove removes an endpoint with the address from the pool. The call
// closes an active connection gracefully.
func (pool *ConnectionPool) Remove(addr string) error {
	pool.addrsMutex.Lock()
	endpoint, ok := pool.addrs[addr]
	if !ok {
		pool.addrsMutex.Unlock()
		return errors.New("endpoint not exist")
	}

	select {
	case <-endpoint.close:
		// Close() in progress/done.
	case <-endpoint.shutdown:
		// CloseGraceful()/Remove() in progress/done.
	default:
		close(endpoint.shutdown)
	}

	delete(pool.addrs, addr)
	pool.addrsMutex.Unlock()

	<-endpoint.closed
	return nil
}

func (pool *ConnectionPool) waitClose() []error {
	pool.addrsMutex.RLock()
	endpoints := make([]*endpoint, 0, len(pool.addrs))
	for _, e := range pool.addrs {
		endpoints = append(endpoints, e)
	}
	pool.addrsMutex.RUnlock()

	errs := make([]error, 0, len(endpoints))
	for _, e := range endpoints {
		<-e.closed
		if e.closeErr != nil {
			errs = append(errs, e.closeErr)
		}
	}
	return errs
}

// Close closes connections in the ConnectionPool.
func (pool *ConnectionPool) Close() []error {
	if pool.state.cas(connectedState, closedState) ||
		pool.state.cas(shutdownState, closedState) {
		pool.addrsMutex.RLock()
		for _, s := range pool.addrs {
			close(s.close)
		}
		pool.addrsMutex.RUnlock()
	}

	return pool.waitClose()
}

// CloseGraceful closes connections in the ConnectionPool gracefully. It waits
// for all requests to complete.
func (pool *ConnectionPool) CloseGraceful() []error {
	if pool.state.cas(connectedState, shutdownState) {
		pool.addrsMutex.RLock()
		for _, s := range pool.addrs {
			close(s.shutdown)
		}
		pool.addrsMutex.RUnlock()
	}

	return pool.waitClose()
}

// GetAddrs gets addresses of connections in pool.
func (pool *ConnectionPool) GetAddrs() []string {
	pool.addrsMutex.RLock()
	defer pool.addrsMutex.RUnlock()

	cpy := make([]string, len(pool.addrs))

	i := 0
	for addr := range pool.addrs {
		cpy[i] = addr
		i++
	}

	return cpy
}

// GetPoolInfo gets information of connections (connected status, ro/rw role).
func (pool *ConnectionPool) GetPoolInfo() map[string]*ConnectionInfo {
	info := make(map[string]*ConnectionInfo)

	pool.addrsMutex.RLock()
	defer pool.addrsMutex.RUnlock()
	pool.poolsMutex.RLock()
	defer pool.poolsMutex.RUnlock()

	if pool.state.get() != connectedState {
		return info
	}

	for addr := range pool.addrs {
		conn, role := pool.getConnectionFromPool(addr)
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

// NewWatcher creates a new Watcher object for the connection pool.
//
// You need to require WatchersFeature to use watchers, see examples for the
// function.
//
// The behavior is same as if Connection.NewWatcher() called for each
// connection with a suitable role.
//
// Keep in mind that garbage collection of a watcher handle doesn’t lead to the
// watcher’s destruction. In this case, the watcher remains registered. You
// need to call Unregister() directly.
//
// Unregister() guarantees that there will be no the watcher's callback calls
// after it, but Unregister() call from the callback leads to a deadlock.
//
// See:
// https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_events/#box-watchers
//
// Since 1.10.0
func (pool *ConnectionPool) NewWatcher(key string,
	callback tarantool.WatchCallback, mode Mode) (tarantool.Watcher, error) {
	watchersRequired := false
	for _, feature := range pool.connOpts.RequiredProtocolInfo.Features {
		if tarantool.WatchersFeature == feature {
			watchersRequired = true
			break
		}
	}
	if !watchersRequired {
		return nil, errors.New("the feature WatchersFeature must be " +
			"required by connection options to create a watcher")
	}

	watcher := &poolWatcher{
		container:    &pool.watcherContainer,
		mode:         mode,
		key:          key,
		callback:     callback,
		watchers:     make(map[string]tarantool.Watcher),
		unregistered: false,
	}

	watcher.container.add(watcher)

	rr := pool.anyPool
	if mode == RW {
		rr = pool.rwPool
	} else if mode == RO {
		rr = pool.roPool
	}

	conns := rr.GetConnections()
	for _, conn := range conns {
		if err := watcher.watch(conn); err != nil {
			conn.Close()
		}
	}

	return watcher, nil
}

// Do sends the request and returns a future.
// For requests that belong to the only one connection (e.g. Unprepare or ExecutePrepared)
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

func (pool *ConnectionPool) deleteConnection(addr string) {
	if conn := pool.anyPool.DeleteConnByAddr(addr); conn != nil {
		if conn := pool.rwPool.DeleteConnByAddr(addr); conn == nil {
			pool.roPool.DeleteConnByAddr(addr)
		}
		// The internal connection deinitialization.
		pool.watcherContainer.mutex.RLock()
		defer pool.watcherContainer.mutex.RUnlock()

		pool.watcherContainer.foreach(func(watcher *poolWatcher) error {
			watcher.unwatch(conn)
			return nil
		})
	}
}

func (pool *ConnectionPool) addConnection(addr string,
	conn *tarantool.Connection, role Role) error {
	// The internal connection initialization.
	pool.watcherContainer.mutex.RLock()
	defer pool.watcherContainer.mutex.RUnlock()

	watched := []*poolWatcher{}
	err := pool.watcherContainer.foreach(func(watcher *poolWatcher) error {
		watch := false
		if watcher.mode == RW {
			watch = role == MasterRole
		} else if watcher.mode == RO {
			watch = role == ReplicaRole
		} else {
			watch = true
		}
		if watch {
			if err := watcher.watch(conn); err != nil {
				return err
			}
			watched = append(watched, watcher)
		}
		return nil
	})
	if err != nil {
		for _, watcher := range watched {
			watcher.unwatch(conn)
		}
		log.Printf("tarantool: failed initialize watchers for %s: %s", addr, err)
		return err
	}

	pool.anyPool.AddConn(addr, conn)

	switch role {
	case MasterRole:
		pool.rwPool.AddConn(addr, conn)
	case ReplicaRole:
		pool.roPool.AddConn(addr, conn)
	}
	return nil
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

func (connPool *ConnectionPool) fillPools() bool {
	somebodyAlive := false

	// It is called before controller() goroutines so we don't expect
	// concurrency issues here.
	for addr := range connPool.addrs {
		end := newEndpoint(addr)
		connPool.addrs[addr] = end

		connOpts := connPool.connOpts
		connOpts.Notify = end.notify
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
				if connPool.addConnection(addr, conn, role) != nil {
					conn.Close()
					connPool.handlerDeactivated(conn, role)
				}

				if conn.ConnectedNow() {
					end.conn = conn
					end.role = role
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

	return somebodyAlive
}

func (pool *ConnectionPool) updateConnection(e *endpoint) {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return
	}

	if role, err := pool.getConnectionRole(e.conn); err == nil {
		if e.role != role {
			pool.deleteConnection(e.addr)
			pool.poolsMutex.Unlock()

			pool.handlerDeactivated(e.conn, e.role)
			opened := pool.handlerDiscovered(e.conn, role)
			if !opened {
				e.conn.Close()
				e.conn = nil
				e.role = UnknownRole
				return
			}

			pool.poolsMutex.Lock()
			if pool.state.get() != connectedState {
				pool.poolsMutex.Unlock()

				e.conn.Close()
				pool.handlerDeactivated(e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}

			if pool.addConnection(e.addr, e.conn, role) != nil {
				pool.poolsMutex.Unlock()

				e.conn.Close()
				pool.handlerDeactivated(e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}
			e.role = role
		}
		pool.poolsMutex.Unlock()
		return
	} else {
		pool.deleteConnection(e.addr)
		pool.poolsMutex.Unlock()

		e.conn.Close()
		pool.handlerDeactivated(e.conn, e.role)
		e.conn = nil
		e.role = UnknownRole
		return
	}
}

func (pool *ConnectionPool) tryConnect(e *endpoint) error {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return ErrClosed
	}

	e.conn = nil
	e.role = UnknownRole

	connOpts := pool.connOpts
	connOpts.Notify = e.notify
	conn, err := tarantool.Connect(e.addr, connOpts)
	if err == nil {
		role, err := pool.getConnectionRole(conn)
		pool.poolsMutex.Unlock()

		if err != nil {
			conn.Close()
			log.Printf("tarantool: storing connection to %s failed: %s\n", e.addr, err)
			return err
		}

		opened := pool.handlerDiscovered(conn, role)
		if !opened {
			conn.Close()
			return errors.New("storing connection canceled")
		}

		pool.poolsMutex.Lock()
		if pool.state.get() != connectedState {
			pool.poolsMutex.Unlock()
			conn.Close()
			pool.handlerDeactivated(conn, role)
			return ErrClosed
		}

		if err = pool.addConnection(e.addr, conn, role); err != nil {
			pool.poolsMutex.Unlock()
			conn.Close()
			pool.handlerDeactivated(conn, role)
			return err
		}
		e.conn = conn
		e.role = role
	}

	pool.poolsMutex.Unlock()
	return err
}

func (pool *ConnectionPool) reconnect(e *endpoint) {
	pool.poolsMutex.Lock()

	if pool.state.get() != connectedState {
		pool.poolsMutex.Unlock()
		return
	}

	pool.deleteConnection(e.addr)
	pool.poolsMutex.Unlock()

	pool.handlerDeactivated(e.conn, e.role)
	e.conn = nil
	e.role = UnknownRole

	pool.tryConnect(e)
}

func (pool *ConnectionPool) controller(e *endpoint) {
	timer := time.NewTicker(pool.opts.CheckTimeout)
	defer timer.Stop()

	shutdown := false
	for {
		if shutdown {
			// Graceful shutdown in progress. We need to wait for a finish or
			// to force close.
			select {
			case <-e.closed:
			case <-e.close:
			}
		}

		select {
		case <-e.closed:
			return
		default:
		}

		select {
		// e.close has priority to avoid concurrency with e.shutdown.
		case <-e.close:
			if e.conn != nil {
				pool.poolsMutex.Lock()
				pool.deleteConnection(e.addr)
				pool.poolsMutex.Unlock()

				if !shutdown {
					e.closeErr = e.conn.Close()
					pool.handlerDeactivated(e.conn, e.role)
					close(e.closed)
				} else {
					// Force close the connection.
					e.conn.Close()
					// And wait for a finish.
					<-e.closed
				}
			} else {
				close(e.closed)
			}
		default:
			select {
			case <-e.shutdown:
				shutdown = true
				if e.conn != nil {
					pool.poolsMutex.Lock()
					pool.deleteConnection(e.addr)
					pool.poolsMutex.Unlock()

					// We need to catch s.close in the current goroutine, so
					// we need to start an another one for the shutdown.
					go func() {
						e.closeErr = e.conn.CloseGraceful()
						close(e.closed)
					}()
				} else {
					close(e.closed)
				}
			default:
				select {
				case <-e.close:
					// Will be processed at an upper level.
				case <-e.shutdown:
					// Will be processed at an upper level.
				case <-e.notify:
					if e.conn != nil && e.conn.ClosedNow() {
						pool.poolsMutex.Lock()
						if pool.state.get() == connectedState {
							pool.deleteConnection(e.addr)
							pool.poolsMutex.Unlock()
							pool.handlerDeactivated(e.conn, e.role)
							e.conn = nil
							e.role = UnknownRole
						} else {
							pool.poolsMutex.Unlock()
						}
					}
				case <-timer.C:
					// Reopen connection.
					// Relocate connection between subpools
					// if ro/rw was updated.
					if e.conn == nil {
						pool.tryConnect(e)
					} else if !e.conn.ClosedNow() {
						pool.updateConnection(e)
					} else {
						pool.reconnect(e)
					}
				}
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
