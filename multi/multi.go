// Package with methods to work with a Tarantool cluster.
//
// Main features:
//
// - Check the active connection with a configurable time interval and switch
// to the next connection in the pool if there is a connection failure.
//
// - Get the address list from the server and reconfigure it for use in
// MultiConnection.
//
// Since: 1.5
package multi

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ice-blockchain/go-tarantool"
)

const (
	connConnected = iota
	connClosed
)

var (
	ErrEmptyAddrs        = errors.New("addrs should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
)

func indexOf(sstring string, data []string) int {
	for i, v := range data {
		if sstring == v {
			return i
		}
	}
	return -1
}

// ConnectionMulti is a handle with connections to a number of Tarantool instances.
//
// It is created and configured with Connect function, and could not be
// reconfigured later.
type ConnectionMulti struct {
	addrs    []string
	connOpts tarantool.Opts
	opts     OptsMulti

	mutex    sync.RWMutex
	notify   chan tarantool.ConnEvent
	state    uint32
	control  chan struct{}
	pool     map[string]*tarantool.Connection
	fallback *tarantool.Connection
}

var _ = tarantool.Connector(&ConnectionMulti{}) // Check compatibility with connector interface.

// OptsMulti is a way to configure Connection with multiconnect-specific options.
type OptsMulti struct {
	// CheckTimeout is a time interval to check for connection timeout and try to
	// switch connection.
	CheckTimeout time.Duration
	// Lua function name of the server called to retrieve the address list.
	NodesGetFunctionName string
	// Time interval to ask the server for an updated address list (works
	// if NodesGetFunctionName is set).
	ClusterDiscoveryTime time.Duration
}

// Connect creates and configures new ConnectionMulti with multiconnection options.
func ConnectWithOpts(addrs []string, connOpts tarantool.Opts, opts OptsMulti) (connMulti *ConnectionMulti, err error) {
	if len(addrs) == 0 {
		return nil, ErrEmptyAddrs
	}
	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}
	if opts.ClusterDiscoveryTime <= 0 {
		opts.ClusterDiscoveryTime = 60 * time.Second
	}

	notify := make(chan tarantool.ConnEvent, 10*len(addrs)) // x10 to accept disconnected and closed event (with a margin).
	connOpts.Notify = notify
	connMulti = &ConnectionMulti{
		addrs:    addrs,
		connOpts: connOpts.Clone(),
		opts:     opts,
		notify:   notify,
		control:  make(chan struct{}),
		pool:     make(map[string]*tarantool.Connection),
	}
	somebodyAlive, _ := connMulti.warmUp()
	if !somebodyAlive {
		connMulti.Close()
		return nil, ErrNoConnection
	}
	go connMulti.checker()

	return connMulti, nil
}

// Connect creates and configures new ConnectionMulti.
func Connect(addrs []string, connOpts tarantool.Opts) (connMulti *ConnectionMulti, err error) {
	opts := OptsMulti{
		CheckTimeout: 1 * time.Second,
	}
	return ConnectWithOpts(addrs, connOpts, opts)
}

func (connMulti *ConnectionMulti) warmUp() (somebodyAlive bool, errs []error) {
	errs = make([]error, len(connMulti.addrs))

	for i, addr := range connMulti.addrs {
		conn, err := tarantool.Connect(addr, connMulti.connOpts)
		errs[i] = err
		if conn != nil && err == nil {
			if connMulti.fallback == nil {
				connMulti.fallback = conn
			}
			connMulti.pool[addr] = conn
			if conn.ConnectedNow() {
				somebodyAlive = true
			}
		}
	}
	return
}

func (connMulti *ConnectionMulti) getState() uint32 {
	return atomic.LoadUint32(&connMulti.state)
}

func (connMulti *ConnectionMulti) getConnectionFromPool(addr string) (*tarantool.Connection, bool) {
	connMulti.mutex.RLock()
	defer connMulti.mutex.RUnlock()
	conn, ok := connMulti.pool[addr]
	return conn, ok
}

func (connMulti *ConnectionMulti) setConnectionToPool(addr string, conn *tarantool.Connection) {
	connMulti.mutex.Lock()
	defer connMulti.mutex.Unlock()
	connMulti.pool[addr] = conn
}

func (connMulti *ConnectionMulti) deleteConnectionFromPool(addr string) {
	connMulti.mutex.Lock()
	defer connMulti.mutex.Unlock()
	delete(connMulti.pool, addr)
}

func (connMulti *ConnectionMulti) checker() {

	refreshTimer := time.NewTicker(connMulti.opts.ClusterDiscoveryTime)
	timer := time.NewTicker(connMulti.opts.CheckTimeout)
	defer refreshTimer.Stop()
	defer timer.Stop()

	for connMulti.getState() != connClosed {

		select {
		case <-connMulti.control:
			return
		case e := <-connMulti.notify:
			if connMulti.getState() == connClosed {
				return
			}
			if e.Conn.ClosedNow() {
				addr := e.Conn.Addr()
				if _, ok := connMulti.getConnectionFromPool(addr); !ok {
					continue
				}
				conn, _ := tarantool.Connect(addr, connMulti.connOpts)
				if conn != nil {
					connMulti.setConnectionToPool(addr, conn)
				} else {
					connMulti.deleteConnectionFromPool(addr)
				}
			}
		case <-refreshTimer.C:
			if connMulti.getState() == connClosed || connMulti.opts.NodesGetFunctionName == "" {
				continue
			}
			var resp [][]string
			err := connMulti.Call17Typed(connMulti.opts.NodesGetFunctionName, []interface{}{}, &resp)
			if err != nil {
				continue
			}
			if len(resp) > 0 && len(resp[0]) > 0 {
				addrs := resp[0]
				// Fill pool with new connections.
				for _, v := range addrs {
					if indexOf(v, connMulti.addrs) < 0 {
						conn, _ := tarantool.Connect(v, connMulti.connOpts)
						if conn != nil {
							connMulti.setConnectionToPool(v, conn)
						}
					}
				}
				// Clear pool from obsolete connections.
				for _, v := range connMulti.addrs {
					if indexOf(v, addrs) < 0 {
						con, ok := connMulti.getConnectionFromPool(v)
						if con != nil && ok {
							con.Close()
						}
						connMulti.deleteConnectionFromPool(v)
					}
				}
				connMulti.mutex.Lock()
				connMulti.addrs = addrs
				connMulti.mutex.Unlock()
			}
		case <-timer.C:
			for _, addr := range connMulti.addrs {
				if connMulti.getState() == connClosed {
					return
				}
				if conn, ok := connMulti.getConnectionFromPool(addr); ok {
					if !conn.ClosedNow() {
						continue
					}
				}
				conn, _ := tarantool.Connect(addr, connMulti.connOpts)
				if conn != nil {
					connMulti.setConnectionToPool(addr, conn)
				}
			}
		}
	}
}

func (connMulti *ConnectionMulti) getCurrentConnection() *tarantool.Connection {
	connMulti.mutex.RLock()
	defer connMulti.mutex.RUnlock()

	for _, addr := range connMulti.addrs {
		conn := connMulti.pool[addr]
		if conn != nil {
			if conn.ConnectedNow() {
				return conn
			}
			connMulti.fallback = conn
		}
	}
	return connMulti.fallback
}

// ConnectedNow reports if connection is established at the moment.
func (connMulti *ConnectionMulti) ConnectedNow() bool {
	return connMulti.getState() == connConnected && connMulti.getCurrentConnection().ConnectedNow()
}

// Close closes Connection.
// After this method called, there is no way to reopen this Connection.
func (connMulti *ConnectionMulti) Close() (err error) {
	connMulti.mutex.Lock()
	defer connMulti.mutex.Unlock()

	close(connMulti.control)
	atomic.StoreUint32(&connMulti.state, connClosed)

	for _, conn := range connMulti.pool {
		if err == nil {
			err = conn.Close()
		} else {
			conn.Close()
		}
	}
	if connMulti.fallback != nil {
		connMulti.fallback.Close()
	}

	return
}

// Ping sends empty request to Tarantool to check connection.
func (connMulti *ConnectionMulti) Ping() (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Ping()
}

// ConfiguredTimeout returns a timeout from connection config.
func (connMulti *ConnectionMulti) ConfiguredTimeout() time.Duration {
	return connMulti.getCurrentConnection().ConfiguredTimeout()
}

// Select performs select to box space.
func (connMulti *ConnectionMulti) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Select(space, index, offset, limit, iterator, key)
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connMulti *ConnectionMulti) Insert(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Insert(space, tuple)
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connMulti *ConnectionMulti) Replace(space interface{}, tuple interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Replace(space, tuple)
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
func (connMulti *ConnectionMulti) Delete(space, index interface{}, key interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Delete(space, index, key)
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
func (connMulti *ConnectionMulti) Update(space, index interface{}, key, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Update(space, index, key, ops)
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
func (connMulti *ConnectionMulti) Upsert(space interface{}, tuple, ops interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Upsert(space, tuple, ops)
}

// Call calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connMulti *ConnectionMulti) Call(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Call(functionName, args)
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool 1.6, so result is converted to array of
// arrays.
// Deprecated since Tarantool 1.7.2.
func (connMulti *ConnectionMulti) Call16(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Call16(functionName, args)
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array).
func (connMulti *ConnectionMulti) Call17(functionName string, args interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Call17(functionName, args)
}

// Eval passes Lua expression for evaluation.
func (connMulti *ConnectionMulti) Eval(expr string, args interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Eval(expr, args)
}

// Execute passes sql expression to Tarantool for execution.
//
// Since 1.6.0
func (connMulti *ConnectionMulti) Execute(expr string, args interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().Execute(expr, args)
}

// GetTyped performs select (with limit = 1 and offset = 0) to box space and
// fills typed result.
func (connMulti *ConnectionMulti) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().GetTyped(space, index, key, result)
}

// SelectTyped performs select to box space and fills typed result.
func (connMulti *ConnectionMulti) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().SelectTyped(space, index, offset, limit, iterator, key, result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connMulti *ConnectionMulti) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().InsertTyped(space, tuple, result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
func (connMulti *ConnectionMulti) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().ReplaceTyped(space, tuple, result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with
// deleted tuple.
func (connMulti *ConnectionMulti) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().DeleteTyped(space, index, key, result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated
// tuple.
func (connMulti *ConnectionMulti) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().UpdateTyped(space, index, key, ops, result)
}

// CallTyped calls registered function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connMulti *ConnectionMulti) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().CallTyped(functionName, args, result)
}

// Call16Typed calls registered function.
// It uses request code for Tarantool 1.6, so result is converted to array of
// arrays.
// Deprecated since Tarantool 1.7.2.
func (connMulti *ConnectionMulti) Call16Typed(functionName string, args interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().Call16Typed(functionName, args, result)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool >= 1.7, so result is not converted (though,
// keep in mind, result is always array)
func (connMulti *ConnectionMulti) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().Call17Typed(functionName, args, result)
}

// EvalTyped passes Lua expression for evaluation.
func (connMulti *ConnectionMulti) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().EvalTyped(expr, args, result)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
func (connMulti *ConnectionMulti) ExecuteTyped(expr string, args interface{}, result interface{}) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	return connMulti.getCurrentConnection().ExecuteTyped(expr, args, result)
}

// SelectAsync sends select request to Tarantool and returns Future.
func (connMulti *ConnectionMulti) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().SelectAsync(space, index, offset, limit, iterator, key)
}

// InsertAsync sends insert action to Tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (connMulti *ConnectionMulti) InsertAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().InsertAsync(space, tuple)
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (connMulti *ConnectionMulti) ReplaceAsync(space interface{}, tuple interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().ReplaceAsync(space, tuple)
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (connMulti *ConnectionMulti) DeleteAsync(space, index interface{}, key interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().DeleteAsync(space, index, key)
}

// Update sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (connMulti *ConnectionMulti) UpdateAsync(space, index interface{}, key, ops interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().UpdateAsync(space, index, key, ops)
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (connMulti *ConnectionMulti) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().UpsertAsync(space, tuple, ops)
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (connMulti *ConnectionMulti) CallAsync(functionName string, args interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().CallAsync(functionName, args)
}

// Call16Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is always array
// of arrays.
// Deprecated since Tarantool 1.7.2.
func (connMulti *ConnectionMulti) Call16Async(functionName string, args interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().Call16Async(functionName, args)
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, so future's result will not be converted
// (though, keep in mind, result is always array).
func (connMulti *ConnectionMulti) Call17Async(functionName string, args interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().Call17Async(functionName, args)
}

// EvalAsync passes Lua expression for evaluation.
func (connMulti *ConnectionMulti) EvalAsync(expr string, args interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().EvalAsync(expr, args)
}

// ExecuteAsync passes sql expression to Tarantool for execution.
func (connMulti *ConnectionMulti) ExecuteAsync(expr string, args interface{}) *tarantool.Future {
	return connMulti.getCurrentConnection().ExecuteAsync(expr, args)
}

// NewPrepared passes a sql statement to Tarantool for preparation synchronously.
func (connMulti *ConnectionMulti) NewPrepared(expr string) (*tarantool.Prepared, error) {
	return connMulti.getCurrentConnection().NewPrepared(expr)
}

// NewStream creates new Stream object for connection.
//
// Since v. 2.10.0, Tarantool supports streams and interactive transactions over them.
// To use interactive transactions, memtx_use_mvcc_engine box option should be set to true.
// Since 1.7.0
func (connMulti *ConnectionMulti) NewStream() (*tarantool.Stream, error) {
	return connMulti.getCurrentConnection().NewStream()
}

// NewWatcher does not supported by the ConnectionMulti. The ConnectionMulti is
// deprecated: use ConnectionPool instead.
//
// Since 1.10.0
func (connMulti *ConnectionMulti) NewWatcher(key string,
	callback tarantool.WatchCallback) (tarantool.Watcher, error) {
	return nil, errors.New("ConnectionMulti is deprecated " +
		"use ConnectionPool")
}

// Do sends the request and returns a future.
func (connMulti *ConnectionMulti) Do(req tarantool.Request) *tarantool.Future {
	if connectedReq, ok := req.(tarantool.ConnectedRequest); ok {
		_, belongs := connMulti.getConnectionFromPool(connectedReq.Conn().Addr())
		if !belongs {
			fut := tarantool.NewFuture()
			fut.SetError(fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool"))
			return fut
		}
		return connectedReq.Conn().Do(req)
	}
	return connMulti.getCurrentConnection().Do(req)
}

func (connMulti *ConnectionMulti) PrepareExecute(sql string, args map[string]interface{}) (resp *tarantool.Response, err error) {
	return connMulti.getCurrentConnection().PrepareExecute(sql, args)
}
func (connMulti *ConnectionMulti) PrepareExecuteTyped(sql string, args map[string]interface{}, result interface{}) (err error) {
	return connMulti.getCurrentConnection().PrepareExecuteTyped(sql, args, result)
}
func (connMulti *ConnectionMulti) UpsertTyped(space, tuple, ops, result interface{}) (err error) {
	return connMulti.getCurrentConnection().UpsertTyped(space, tuple, ops, result)
}
