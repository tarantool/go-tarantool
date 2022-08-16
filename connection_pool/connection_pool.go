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
	"sync/atomic"
	"time"

	"github.com/tarantool/go-tarantool"
)

var (
	ErrEmptyAddrs        = errors.New("addrs (first argument) should not be empty")
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
	ErrNoConnection      = errors.New("no active connections")
	ErrTooManyArgs       = errors.New("too many arguments")
	ErrIncorrectResponse = errors.New("Incorrect response format")
	ErrIncorrectStatus   = errors.New("Incorrect instance status: status should be `running`")
	ErrNoRwInstance      = errors.New("Can't find rw instance in pool")
	ErrNoRoInstance      = errors.New("Can't find ro instance in pool")
	ErrNoHealthyInstance = errors.New("Can't find healthy instance in pool")
)

/*
Additional options (configurable via ConnectWithOpts):

- CheckTimeout - time interval to check for connection timeout and try to switch connection.
*/
type OptsPool struct {
	// timeout for timer to reopen connections
	// that have been closed by some events and
	// to relocate connection between subpools
	// if ro/rw role has been updated
	CheckTimeout time.Duration
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

	notify  chan tarantool.ConnEvent
	state   State
	control chan struct{}
	roPool  *RoundRobinStrategy
	rwPool  *RoundRobinStrategy
	anyPool *RoundRobinStrategy
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

	notify := make(chan tarantool.ConnEvent, 10*len(addrs)) // x10 to accept disconnected and closed event (with a margin)
	connOpts.Notify = notify

	size := len(addrs)
	rwPool := NewEmptyRoundRobin(size)
	roPool := NewEmptyRoundRobin(size)
	anyPool := NewEmptyRoundRobin(size)

	connPool = &ConnectionPool{
		addrs:    addrs,
		connOpts: connOpts,
		opts:     opts,
		notify:   notify,
		control:  make(chan struct{}),
		rwPool:   rwPool,
		roPool:   roPool,
		anyPool:  anyPool,
	}

	somebodyAlive := connPool.fillPools()
	if !somebodyAlive {
		connPool.Close()
		return nil, ErrNoConnection
	}

	go connPool.checker()

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
	if connPool.getState() != connConnected {
		return false, nil
	}

	conn, err := connPool.getNextConnection(mode)
	if err != nil || conn == nil {
		return false, err
	}

	return conn.ConnectedNow(), nil
}

// ConfiguredTimeout gets timeout of current connection.
func (connPool *ConnectionPool) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	conn, err := connPool.getNextConnection(mode)
	if err != nil {
		return 0, err
	}

	return conn.ConfiguredTimeout(), nil
}

// Close closes connections in pool.
func (connPool *ConnectionPool) Close() []error {
	close(connPool.control)
	connPool.state = connClosed

	rwErrs := connPool.rwPool.CloseConns()
	roErrs := connPool.roPool.CloseConns()

	allErrs := append(rwErrs, roErrs...)

	return allErrs
}

// GetAddrs gets addresses of connections in pool.
func (connPool *ConnectionPool) GetAddrs() []string {
	return connPool.addrs
}

// GetPoolInfo gets information of connections (connected status, ro/rw role).
func (connPool *ConnectionPool) GetPoolInfo() map[string]*ConnectionInfo {
	info := make(map[string]*ConnectionInfo)

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
		return unknown, err
	}
	if resp == nil {
		return unknown, ErrIncorrectResponse
	}
	if len(resp.Data) < 1 {
		return unknown, ErrIncorrectResponse
	}

	instanceStatus, ok := resp.Data[0].(map[interface{}]interface{})["status"]
	if !ok {
		return unknown, ErrIncorrectResponse
	}
	if instanceStatus != "running" {
		return unknown, ErrIncorrectStatus
	}

	replicaRole, ok := resp.Data[0].(map[interface{}]interface{})["ro"]
	if !ok {
		return unknown, ErrIncorrectResponse
	}

	switch replicaRole {
	case false:
		return master, nil
	case true:
		return replica, nil
	}

	return unknown, nil
}

func (connPool *ConnectionPool) getConnectionFromPool(addr string) (*tarantool.Connection, Role) {
	conn := connPool.rwPool.GetConnByAddr(addr)
	if conn != nil {
		return conn, master
	}

	conn = connPool.roPool.GetConnByAddr(addr)
	if conn != nil {
		return conn, replica
	}

	return connPool.anyPool.GetConnByAddr(addr), unknown
}

func (connPool *ConnectionPool) deleteConnectionFromPool(addr string) {
	_ = connPool.anyPool.DeleteConnByAddr(addr)
	conn := connPool.rwPool.DeleteConnByAddr(addr)
	if conn != nil {
		return
	}

	connPool.roPool.DeleteConnByAddr(addr)
}

func (connPool *ConnectionPool) setConnectionToPool(addr string, conn *tarantool.Connection) error {
	role, err := connPool.getConnectionRole(conn)
	if err != nil {
		return err
	}

	connPool.anyPool.AddConn(addr, conn)

	switch role {
	case master:
		connPool.rwPool.AddConn(addr, conn)
	case replica:
		connPool.roPool.AddConn(addr, conn)
	}

	return nil
}

func (connPool *ConnectionPool) refreshConnection(addr string) {
	if conn, oldRole := connPool.getConnectionFromPool(addr); conn != nil {
		if !conn.ClosedNow() {
			curRole, _ := connPool.getConnectionRole(conn)
			if oldRole != curRole {
				connPool.deleteConnectionFromPool(addr)
				err := connPool.setConnectionToPool(addr, conn)
				if err != nil {
					conn.Close()
					log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err.Error())
				}
			}
		}
	} else {
		conn, _ := tarantool.Connect(addr, connPool.connOpts)
		if conn != nil {
			err := connPool.setConnectionToPool(addr, conn)
			if err != nil {
				conn.Close()
				log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err.Error())
			}
		}
	}
}

func (connPool *ConnectionPool) checker() {

	timer := time.NewTicker(connPool.opts.CheckTimeout)
	defer timer.Stop()

	for connPool.getState() != connClosed {
		select {
		case <-connPool.control:
			return
		case e := <-connPool.notify:
			if connPool.getState() == connClosed {
				return
			}
			if e.Conn.ClosedNow() {
				addr := e.Conn.Addr()
				if conn, _ := connPool.getConnectionFromPool(addr); conn == nil {
					continue
				}
				conn, _ := tarantool.Connect(addr, connPool.connOpts)
				if conn != nil {
					err := connPool.setConnectionToPool(addr, conn)
					if err != nil {
						conn.Close()
						log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err.Error())
					}
				} else {
					connPool.deleteConnectionFromPool(addr)
				}
			}
		case <-timer.C:
			for _, addr := range connPool.addrs {
				if connPool.getState() == connClosed {
					return
				}

				// Reopen connection
				// Relocate connection between subpools
				// if ro/rw was updated
				connPool.refreshConnection(addr)
			}
		}
	}
}

func (connPool *ConnectionPool) fillPools() bool {
	somebodyAlive := false

	for _, addr := range connPool.addrs {
		conn, err := tarantool.Connect(addr, connPool.connOpts)
		if err != nil {
			log.Printf("tarantool: connect to %s failed: %s\n", addr, err.Error())
		} else if conn != nil {
			err = connPool.setConnectionToPool(addr, conn)
			if err != nil {
				conn.Close()
				log.Printf("tarantool: storing connection to %s failed: %s\n", addr, err.Error())
			} else if conn.ConnectedNow() {
				somebodyAlive = true
			}
		}
	}

	return somebodyAlive
}

func (connPool *ConnectionPool) getState() uint32 {
	return atomic.LoadUint32((*uint32)(&connPool.state))
}

func (connPool *ConnectionPool) getNextConnection(mode Mode) (*tarantool.Connection, error) {

	switch mode {
	case ANY:
		if connPool.anyPool.IsEmpty() {
			return nil, ErrNoHealthyInstance
		}

		return connPool.anyPool.GetNextConnection(), nil

	case RW:
		if connPool.rwPool.IsEmpty() {
			return nil, ErrNoRwInstance
		}

		return connPool.rwPool.GetNextConnection(), nil

	case RO:
		if connPool.roPool.IsEmpty() {
			return nil, ErrNoRoInstance
		}

		return connPool.roPool.GetNextConnection(), nil

	case PreferRW:
		if !connPool.rwPool.IsEmpty() {
			return connPool.rwPool.GetNextConnection(), nil
		}

		if !connPool.roPool.IsEmpty() {
			return connPool.roPool.GetNextConnection(), nil
		}

		return nil, ErrNoHealthyInstance

	case PreferRO:
		if !connPool.roPool.IsEmpty() {
			return connPool.roPool.GetNextConnection(), nil
		}

		if !connPool.rwPool.IsEmpty() {
			return connPool.rwPool.GetNextConnection(), nil
		}

		return nil, ErrNoHealthyInstance
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
