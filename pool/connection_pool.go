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
package pool

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v2"
)

var (
	ErrEmptyDialers      = errors.New("dialers (second argument) should not be empty")
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
	ErrUnknownRequest    = errors.New("the passed connected request doesn't belong to " +
		"the current connection pool")
	ErrContextCanceled = errors.New("operation was canceled")
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
	Discovered(id string, conn *tarantool.Connection, role Role) error
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
	Deactivated(id string, conn *tarantool.Connection, role Role) error
}

// Opts provides additional options (configurable via ConnectWithOpts).
type Opts struct {
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
	ends      map[string]*endpoint
	endsMutex sync.RWMutex

	connOpts tarantool.Opts
	opts     Opts

	state            state
	done             chan struct{}
	roPool           *roundRobinStrategy
	rwPool           *roundRobinStrategy
	anyPool          *roundRobinStrategy
	poolsMutex       sync.RWMutex
	watcherContainer watcherContainer
}

var _ Pooler = (*ConnectionPool)(nil)

type endpoint struct {
	id     string
	dialer tarantool.Dialer
	notify chan tarantool.ConnEvent
	conn   *tarantool.Connection
	role   Role
	// This is used to switch a connection states.
	shutdown chan struct{}
	close    chan struct{}
	closed   chan struct{}
	cancel   context.CancelFunc
	closeErr error
}

func newEndpoint(id string, dialer tarantool.Dialer) *endpoint {
	return &endpoint{
		id:       id,
		dialer:   dialer,
		notify:   make(chan tarantool.ConnEvent, 100),
		conn:     nil,
		role:     UnknownRole,
		shutdown: make(chan struct{}),
		close:    make(chan struct{}),
		closed:   make(chan struct{}),
		cancel:   nil,
	}
}

// ConnectWithOpts creates pool for instances with specified dialers and options opts.
// Each dialer corresponds to a certain id by which they will be distinguished.
func ConnectWithOpts(ctx context.Context, dialers map[string]tarantool.Dialer,
	connOpts tarantool.Opts, opts Opts) (*ConnectionPool, error) {
	if len(dialers) == 0 {
		return nil, ErrEmptyDialers
	}
	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}

	size := len(dialers)
	rwPool := newRoundRobinStrategy(size)
	roPool := newRoundRobinStrategy(size)
	anyPool := newRoundRobinStrategy(size)

	connPool := &ConnectionPool{
		ends:     make(map[string]*endpoint),
		connOpts: connOpts,
		opts:     opts,
		state:    unknownState,
		done:     make(chan struct{}),
		rwPool:   rwPool,
		roPool:   roPool,
		anyPool:  anyPool,
	}

	somebodyAlive, ctxCanceled := connPool.fillPools(ctx, dialers)
	if !somebodyAlive {
		connPool.state.set(closedState)
		if ctxCanceled {
			return nil, ErrContextCanceled
		}
		return nil, ErrNoConnection
	}

	connPool.state.set(connectedState)

	for _, s := range connPool.ends {
		endpointCtx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		go connPool.controller(endpointCtx, s)
	}

	return connPool, nil
}

// Connect creates pool for instances with specified dialers.
// Each dialer corresponds to a certain id by which they will be distinguished.
//
// It is useless to set up tarantool.Opts.Reconnect value for a connection.
// The connection pool has its own reconnection logic. See
// Opts.CheckTimeout description.
func Connect(ctx context.Context, dialers map[string]tarantool.Dialer,
	connOpts tarantool.Opts) (*ConnectionPool, error) {
	opts := Opts{
		CheckTimeout: 1 * time.Second,
	}
	return ConnectWithOpts(ctx, dialers, connOpts, opts)
}

// ConnectedNow gets connected status of pool.
func (p *ConnectionPool) ConnectedNow(mode Mode) (bool, error) {
	p.poolsMutex.RLock()
	defer p.poolsMutex.RUnlock()

	if p.state.get() != connectedState {
		return false, nil
	}
	switch mode {
	case ANY:
		return !p.anyPool.IsEmpty(), nil
	case RW:
		return !p.rwPool.IsEmpty(), nil
	case RO:
		return !p.roPool.IsEmpty(), nil
	case PreferRW:
		fallthrough
	case PreferRO:
		return !p.rwPool.IsEmpty() || !p.roPool.IsEmpty(), nil
	default:
		return false, ErrNoHealthyInstance
	}
}

// ConfiguredTimeout gets timeout of current connection.
func (p *ConnectionPool) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	conn, err := p.getNextConnection(mode)
	if err != nil {
		return 0, err
	}

	return conn.ConfiguredTimeout(), nil
}

// Add adds a new endpoint with the id into the pool. This function
// adds the endpoint only after successful connection.
func (p *ConnectionPool) Add(ctx context.Context, id string, dialer tarantool.Dialer) error {
	e := newEndpoint(id, dialer)

	p.endsMutex.Lock()
	// Ensure that Close()/CloseGraceful() not in progress/done.
	if p.state.get() != connectedState {
		p.endsMutex.Unlock()
		return ErrClosed
	}
	if _, ok := p.ends[id]; ok {
		p.endsMutex.Unlock()
		return ErrExists
	}

	endpointCtx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel

	p.ends[id] = e
	p.endsMutex.Unlock()

	if err := p.tryConnect(ctx, e); err != nil {
		p.endsMutex.Lock()
		delete(p.ends, id)
		p.endsMutex.Unlock()
		e.cancel()
		close(e.closed)
		return err
	}

	go p.controller(endpointCtx, e)
	return nil
}

// Remove removes an endpoint with the id from the pool. The call
// closes an active connection gracefully.
func (p *ConnectionPool) Remove(id string) error {
	p.endsMutex.Lock()
	endpoint, ok := p.ends[id]
	if !ok {
		p.endsMutex.Unlock()
		return errors.New("endpoint not exist")
	}

	select {
	case <-endpoint.close:
		// Close() in progress/done.
	case <-endpoint.shutdown:
		// CloseGraceful()/Remove() in progress/done.
	default:
		endpoint.cancel()
		close(endpoint.shutdown)
	}

	delete(p.ends, id)
	p.endsMutex.Unlock()

	<-endpoint.closed
	return nil
}

func (p *ConnectionPool) waitClose() []error {
	p.endsMutex.RLock()
	endpoints := make([]*endpoint, 0, len(p.ends))
	for _, e := range p.ends {
		endpoints = append(endpoints, e)
	}
	p.endsMutex.RUnlock()

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
func (p *ConnectionPool) Close() []error {
	if p.state.cas(connectedState, closedState) ||
		p.state.cas(shutdownState, closedState) {
		p.endsMutex.RLock()
		for _, s := range p.ends {
			s.cancel()
			close(s.close)
		}
		p.endsMutex.RUnlock()
	}

	return p.waitClose()
}

// CloseGraceful closes connections in the ConnectionPool gracefully. It waits
// for all requests to complete.
func (p *ConnectionPool) CloseGraceful() []error {
	if p.state.cas(connectedState, shutdownState) {
		p.endsMutex.RLock()
		for _, s := range p.ends {
			s.cancel()
			close(s.shutdown)
		}
		p.endsMutex.RUnlock()
	}

	return p.waitClose()
}

// GetInfo gets information of connections (connected status, ro/rw role).
func (p *ConnectionPool) GetInfo() map[string]ConnectionInfo {
	info := make(map[string]ConnectionInfo)

	p.endsMutex.RLock()
	defer p.endsMutex.RUnlock()
	p.poolsMutex.RLock()
	defer p.poolsMutex.RUnlock()

	if p.state.get() != connectedState {
		return info
	}

	for id := range p.ends {
		conn, role := p.getConnectionFromPool(id)
		if conn != nil {
			info[id] = ConnectionInfo{ConnectedNow: conn.ConnectedNow(), ConnRole: role}
		} else {
			info[id] = ConnectionInfo{ConnectedNow: false, ConnRole: UnknownRole}
		}
	}

	return info
}

// Ping sends empty request to Tarantool to check connection.
//
// Deprecated: the method will be removed in the next major version,
// use a PingRequest object + Do() instead.
func (p *ConnectionPool) Ping(userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Ping()
}

// Select performs select to box space.
//
// Deprecated: the method will be removed in the next major version,
// use a SelectRequest object + Do() instead.
func (p *ConnectionPool) Select(space, index interface{},
	offset, limit uint32,
	iterator tarantool.Iter, key interface{}, userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(ANY, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Select(space, index, offset, limit, iterator, key)
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// Deprecated: the method will be removed in the next major version,
// use an InsertRequest object + Do() instead.
func (p *ConnectionPool) Insert(space interface{}, tuple interface{},
	userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Insert(space, tuple)
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// Deprecated: the method will be removed in the next major version,
// use a ReplaceRequest object + Do() instead.
func (p *ConnectionPool) Replace(space interface{}, tuple interface{},
	userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Replace(space, tuple)
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a DeleteRequest object + Do() instead.
func (p *ConnectionPool) Delete(space, index interface{}, key interface{},
	userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Delete(space, index, key)
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a UpdateRequest object + Do() instead.
func (p *ConnectionPool) Update(space, index interface{}, key interface{},
	ops *tarantool.Operations, userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Update(space, index, key, ops)
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a UpsertRequest object + Do() instead.
func (p *ConnectionPool) Upsert(space interface{}, tuple interface{},
	ops *tarantool.Operations, userMode ...Mode) ([]interface{}, error) {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return nil, err
	}

	return conn.Upsert(space, tuple, ops)
}

// Call calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a CallRequest object + Do() instead.
func (p *ConnectionPool) Call(functionName string, args interface{},
	userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call(functionName, args)
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool 1.6, result is an array of arrays.
// Deprecated since Tarantool 1.7.2.
//
// Deprecated: the method will be removed in the next major version,
// use a Call16Request object + Do() instead.
func (p *ConnectionPool) Call16(functionName string, args interface{},
	userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call16(functionName, args)
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a Call17Request object + Do() instead.
func (p *ConnectionPool) Call17(functionName string, args interface{},
	userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Call17(functionName, args)
}

// Eval passes lua expression for evaluation.
//
// Deprecated: the method will be removed in the next major version,
// use an EvalRequest object + Do() instead.
func (p *ConnectionPool) Eval(expr string, args interface{},
	userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Eval(expr, args)
}

// Execute passes sql expression to Tarantool for execution.
//
// Deprecated: the method will be removed in the next major version,
// use an ExecuteRequest object + Do() instead.
func (p *ConnectionPool) Execute(expr string, args interface{},
	userMode Mode) ([]interface{}, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}

	return conn.Execute(expr, args)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
//
// Deprecated: the method will be removed in the next major version,
// use a SelectRequest object + Do() instead.
func (p *ConnectionPool) GetTyped(space, index interface{}, key interface{}, result interface{},
	userMode ...Mode) error {
	conn, err := p.getConnByMode(ANY, userMode)
	if err != nil {
		return err
	}

	return conn.GetTyped(space, index, key, result)
}

// SelectTyped performs select to box space and fills typed result.
//
// Deprecated: the method will be removed in the next major version,
// use a SelectRequest object + Do() instead.
func (p *ConnectionPool) SelectTyped(space, index interface{},
	offset, limit uint32,
	iterator tarantool.Iter, key interface{}, result interface{}, userMode ...Mode) error {
	conn, err := p.getConnByMode(ANY, userMode)
	if err != nil {
		return err
	}

	return conn.SelectTyped(space, index, offset, limit, iterator, key, result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// Deprecated: the method will be removed in the next major version,
// use an InsertRequest object + Do() instead.
func (p *ConnectionPool) InsertTyped(space interface{}, tuple interface{}, result interface{},
	userMode ...Mode) error {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.InsertTyped(space, tuple, result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// Deprecated: the method will be removed in the next major version,
// use a ReplaceRequest object + Do() instead.
func (p *ConnectionPool) ReplaceTyped(space interface{}, tuple interface{}, result interface{},
	userMode ...Mode) error {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.ReplaceTyped(space, tuple, result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a DeleteRequest object + Do() instead.
func (p *ConnectionPool) DeleteTyped(space, index interface{}, key interface{}, result interface{},
	userMode ...Mode) error {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.DeleteTyped(space, index, key, result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a UpdateRequest object + Do() instead.
func (p *ConnectionPool) UpdateTyped(space, index interface{}, key interface{},
	ops *tarantool.Operations, result interface{}, userMode ...Mode) error {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return err
	}

	return conn.UpdateTyped(space, index, key, ops, result)
}

// CallTyped calls registered function.
// It uses request code for Tarantool >= 1.7, result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a CallRequest object + Do() instead.
func (p *ConnectionPool) CallTyped(functionName string, args interface{}, result interface{},
	userMode Mode) error {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.CallTyped(functionName, args, result)
}

// Call16Typed calls registered function.
// It uses request code for Tarantool 1.6, result is an array of arrays.
// Deprecated since Tarantool 1.7.2.
//
// Deprecated: the method will be removed in the next major version,
// use a Call16Request object + Do() instead.
func (p *ConnectionPool) Call16Typed(functionName string, args interface{}, result interface{},
	userMode Mode) error {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.Call16Typed(functionName, args, result)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool >= 1.7, result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a Call17Request object + Do() instead.
func (p *ConnectionPool) Call17Typed(functionName string, args interface{}, result interface{},
	userMode Mode) error {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.Call17Typed(functionName, args, result)
}

// EvalTyped passes lua expression for evaluation.
//
// Deprecated: the method will be removed in the next major version,
// use an EvalRequest object + Do() instead.
func (p *ConnectionPool) EvalTyped(expr string, args interface{}, result interface{},
	userMode Mode) error {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return err
	}

	return conn.EvalTyped(expr, args, result)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
//
// Deprecated: the method will be removed in the next major version,
// use an ExecuteRequest object + Do() instead.
func (p *ConnectionPool) ExecuteTyped(expr string, args interface{}, result interface{},
	userMode Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return tarantool.SQLInfo{}, nil, err
	}

	return conn.ExecuteTyped(expr, args, result)
}

// SelectAsync sends select request to Tarantool and returns Future.
//
// Deprecated: the method will be removed in the next major version,
// use a SelectRequest object + Do() instead.
func (p *ConnectionPool) SelectAsync(space, index interface{},
	offset, limit uint32,
	iterator tarantool.Iter, key interface{}, userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(ANY, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.SelectAsync(space, index, offset, limit, iterator, key)
}

// InsertAsync sends insert action to Tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// Deprecated: the method will be removed in the next major version,
// use an InsertRequest object + Do() instead.
func (p *ConnectionPool) InsertAsync(space interface{}, tuple interface{},
	userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.InsertAsync(space, tuple)
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
//
// Deprecated: the method will be removed in the next major version,
// use a ReplaceRequest object + Do() instead.
func (p *ConnectionPool) ReplaceAsync(space interface{}, tuple interface{},
	userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.ReplaceAsync(space, tuple)
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
// Future's result will contain array with deleted tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a DeleteRequest object + Do() instead.
func (p *ConnectionPool) DeleteAsync(space, index interface{}, key interface{},
	userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.DeleteAsync(space, index, key)
}

// UpdateAsync sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a UpdateRequest object + Do() instead.
func (p *ConnectionPool) UpdateAsync(space, index interface{}, key interface{},
	ops *tarantool.Operations, userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.UpdateAsync(space, index, key, ops)
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
// Future's sesult will not contain any tuple.
//
// Deprecated: the method will be removed in the next major version,
// use a UpsertRequest object + Do() instead.
func (p *ConnectionPool) UpsertAsync(space interface{}, tuple interface{},
	ops *tarantool.Operations, userMode ...Mode) *tarantool.Future {
	conn, err := p.getConnByMode(RW, userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.UpsertAsync(space, tuple, ops)
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, future's result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a CallRequest object + Do() instead.
func (p *ConnectionPool) CallAsync(functionName string, args interface{},
	userMode Mode) *tarantool.Future {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.CallAsync(functionName, args)
}

// Call16Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is an array of arrays.
// Deprecated since Tarantool 1.7.2.
//
// Deprecated: the method will be removed in the next major version,
// use a Call16Request object + Do() instead.
func (p *ConnectionPool) Call16Async(functionName string, args interface{},
	userMode Mode) *tarantool.Future {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Call16Async(functionName, args)
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, future's result is an array.
//
// Deprecated: the method will be removed in the next major version,
// use a Call17Request object + Do() instead.
func (p *ConnectionPool) Call17Async(functionName string, args interface{},
	userMode Mode) *tarantool.Future {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Call17Async(functionName, args)
}

// EvalAsync sends a lua expression for evaluation and returns Future.
//
// Deprecated: the method will be removed in the next major version,
// use an EvalRequest object + Do() instead.
func (p *ConnectionPool) EvalAsync(expr string, args interface{},
	userMode Mode) *tarantool.Future {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.EvalAsync(expr, args)
}

// ExecuteAsync sends sql expression to Tarantool for execution and returns
// Future.
//
// Deprecated: the method will be removed in the next major version,
// use an ExecuteRequest object + Do() instead.
func (p *ConnectionPool) ExecuteAsync(expr string, args interface{},
	userMode Mode) *tarantool.Future {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.ExecuteAsync(expr, args)
}

// NewStream creates new Stream object for connection selected
// by userMode from pool.
//
// Since v. 2.10.0, Tarantool supports streams and interactive transactions over them.
// To use interactive transactions, memtx_use_mvcc_engine box option should be set to true.
// Since 1.7.0
func (p *ConnectionPool) NewStream(userMode Mode) (*tarantool.Stream, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}
	return conn.NewStream()
}

// NewPrepared passes a sql statement to Tarantool for preparation synchronously.
func (p *ConnectionPool) NewPrepared(expr string, userMode Mode) (*tarantool.Prepared, error) {
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return nil, err
	}
	return conn.NewPrepared(expr)
}

// NewWatcher creates a new Watcher object for the connection pool.
// A watcher could be created only for instances with the support.
//
// The behavior is same as if Connection.NewWatcher() called for each
// connection with a suitable mode.
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
func (p *ConnectionPool) NewWatcher(key string,
	callback tarantool.WatchCallback, mode Mode) (tarantool.Watcher, error) {

	watcher := &poolWatcher{
		container:    &p.watcherContainer,
		mode:         mode,
		key:          key,
		callback:     callback,
		watchers:     make(map[*tarantool.Connection]tarantool.Watcher),
		unregistered: false,
	}

	watcher.container.add(watcher)

	rr := p.anyPool
	if mode == RW {
		rr = p.rwPool
	} else if mode == RO {
		rr = p.roPool
	}

	conns := rr.GetConnections()
	for _, conn := range conns {
		// Check that connection supports watchers.
		if !isFeatureInSlice(iproto.IPROTO_FEATURE_WATCHERS, conn.ProtocolInfo().Features) {
			continue
		}
		if err := watcher.watch(conn); err != nil {
			conn.Close()
		}
	}

	return watcher, nil
}

// Do sends the request and returns a future.
// For requests that belong to the only one connection (e.g. Unprepare or ExecutePrepared)
// the argument of type Mode is unused.
func (p *ConnectionPool) Do(req tarantool.Request, userMode Mode) *tarantool.Future {
	if connectedReq, ok := req.(tarantool.ConnectedRequest); ok {
		conns := p.anyPool.GetConnections()
		isOurConnection := false
		for _, conn := range conns {
			// Compare raw pointers.
			if conn == connectedReq.Conn() {
				isOurConnection = true
				break
			}
		}
		if !isOurConnection {
			return newErrorFuture(ErrUnknownRequest)
		}
		return connectedReq.Conn().Do(req)
	}
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return newErrorFuture(err)
	}

	return conn.Do(req)
}

//
// private
//

func (p *ConnectionPool) getConnectionRole(conn *tarantool.Connection) (Role, error) {
	data, err := conn.Do(tarantool.NewCallRequest("box.info")).Get()
	if err != nil {
		return UnknownRole, err
	}
	if len(data) < 1 {
		return UnknownRole, ErrIncorrectResponse
	}

	instanceStatus, ok := data[0].(map[interface{}]interface{})["status"]
	if !ok {
		return UnknownRole, ErrIncorrectResponse
	}
	if instanceStatus != "running" {
		return UnknownRole, ErrIncorrectStatus
	}

	replicaRole, ok := data[0].(map[interface{}]interface{})["ro"]
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

func (p *ConnectionPool) getConnectionFromPool(id string) (*tarantool.Connection, Role) {
	if conn := p.rwPool.GetConnById(id); conn != nil {
		return conn, MasterRole
	}

	if conn := p.roPool.GetConnById(id); conn != nil {
		return conn, ReplicaRole
	}

	return p.anyPool.GetConnById(id), UnknownRole
}

func (p *ConnectionPool) deleteConnection(id string) {
	if conn := p.anyPool.DeleteConnById(id); conn != nil {
		if conn := p.rwPool.DeleteConnById(id); conn == nil {
			p.roPool.DeleteConnById(id)
		}
		// The internal connection deinitialization.
		p.watcherContainer.mutex.RLock()
		defer p.watcherContainer.mutex.RUnlock()

		p.watcherContainer.foreach(func(watcher *poolWatcher) error {
			watcher.unwatch(conn)
			return nil
		})
	}
}

func (p *ConnectionPool) addConnection(id string,
	conn *tarantool.Connection, role Role) error {
	// The internal connection initialization.
	p.watcherContainer.mutex.RLock()
	defer p.watcherContainer.mutex.RUnlock()

	if isFeatureInSlice(iproto.IPROTO_FEATURE_WATCHERS, conn.ProtocolInfo().Features) {
		watched := []*poolWatcher{}
		err := p.watcherContainer.foreach(func(watcher *poolWatcher) error {
			watch := false
			switch watcher.mode {
			case RW:
				watch = role == MasterRole
			case RO:
				watch = role == ReplicaRole
			default:
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
			log.Printf("tarantool: failed initialize watchers for %s: %s", id, err)
			return err
		}
	}

	p.anyPool.AddConn(id, conn)

	switch role {
	case MasterRole:
		p.rwPool.AddConn(id, conn)
	case ReplicaRole:
		p.roPool.AddConn(id, conn)
	}
	return nil
}

func (p *ConnectionPool) handlerDiscovered(id string, conn *tarantool.Connection,
	role Role) bool {
	var err error
	if p.opts.ConnectionHandler != nil {
		err = p.opts.ConnectionHandler.Discovered(id, conn, role)
	}

	if err != nil {
		log.Printf("tarantool: storing connection to %s canceled: %s\n", id, err)
		return false
	}
	return true
}

func (p *ConnectionPool) handlerDeactivated(id string, conn *tarantool.Connection,
	role Role) {
	var err error
	if p.opts.ConnectionHandler != nil {
		err = p.opts.ConnectionHandler.Deactivated(id, conn, role)
	}

	if err != nil {
		log.Printf("tarantool: deactivating connection to %s by user failed: %s\n", id, err)
	}
}

func (p *ConnectionPool) deactivateConnection(id string, conn *tarantool.Connection, role Role) {
	p.deleteConnection(id)
	conn.Close()
	p.handlerDeactivated(id, conn, role)
}

func (p *ConnectionPool) deactivateConnections() {
	for id, endpoint := range p.ends {
		if endpoint != nil && endpoint.conn != nil {
			p.deactivateConnection(id, endpoint.conn, endpoint.role)
		}
	}
}

func (p *ConnectionPool) processConnection(conn *tarantool.Connection,
	id string, end *endpoint) bool {
	role, err := p.getConnectionRole(conn)
	if err != nil {
		conn.Close()
		log.Printf("tarantool: storing connection to %s failed: %s\n", id, err)
		return false
	}

	if !p.handlerDiscovered(id, conn, role) {
		conn.Close()
		return false
	}
	if p.addConnection(id, conn, role) != nil {
		conn.Close()
		p.handlerDeactivated(id, conn, role)
		return false
	}

	end.conn = conn
	end.role = role
	return true
}

func (p *ConnectionPool) fillPools(
	ctx context.Context,
	dialers map[string]tarantool.Dialer) (bool, bool) {
	somebodyAlive := false
	ctxCanceled := false

	// It is called before controller() goroutines, so we don't expect
	// concurrency issues here.
	for id, dialer := range dialers {
		end := newEndpoint(id, dialer)
		p.ends[id] = end
		connOpts := p.connOpts
		connOpts.Notify = end.notify
		conn, err := tarantool.Connect(ctx, dialer, connOpts)
		if err != nil {
			log.Printf("tarantool: connect to %s failed: %s\n", id, err.Error())
			select {
			case <-ctx.Done():
				ctxCanceled = true

				p.ends[id] = nil
				log.Printf("tarantool: operation was canceled")

				p.deactivateConnections()

				return false, ctxCanceled
			default:
			}
		} else if p.processConnection(conn, id, end) {
			somebodyAlive = true
		}
	}

	return somebodyAlive, ctxCanceled
}

func (p *ConnectionPool) updateConnection(e *endpoint) {
	p.poolsMutex.Lock()

	if p.state.get() != connectedState {
		p.poolsMutex.Unlock()
		return
	}

	if role, err := p.getConnectionRole(e.conn); err == nil {
		if e.role != role {
			p.deleteConnection(e.id)
			p.poolsMutex.Unlock()

			p.handlerDeactivated(e.id, e.conn, e.role)
			opened := p.handlerDiscovered(e.id, e.conn, role)
			if !opened {
				e.conn.Close()
				e.conn = nil
				e.role = UnknownRole
				return
			}

			p.poolsMutex.Lock()
			if p.state.get() != connectedState {
				p.poolsMutex.Unlock()

				e.conn.Close()
				p.handlerDeactivated(e.id, e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}

			if p.addConnection(e.id, e.conn, role) != nil {
				p.poolsMutex.Unlock()

				e.conn.Close()
				p.handlerDeactivated(e.id, e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}
			e.role = role
		}
		p.poolsMutex.Unlock()
		return
	} else {
		p.deleteConnection(e.id)
		p.poolsMutex.Unlock()

		e.conn.Close()
		p.handlerDeactivated(e.id, e.conn, e.role)
		e.conn = nil
		e.role = UnknownRole
		return
	}
}

func (p *ConnectionPool) tryConnect(ctx context.Context, e *endpoint) error {
	p.poolsMutex.Lock()

	if p.state.get() != connectedState {
		p.poolsMutex.Unlock()
		return ErrClosed
	}

	e.conn = nil
	e.role = UnknownRole

	connOpts := p.connOpts
	connOpts.Notify = e.notify
	conn, err := tarantool.Connect(ctx, e.dialer, connOpts)
	if err == nil {
		role, err := p.getConnectionRole(conn)
		p.poolsMutex.Unlock()

		if err != nil {
			conn.Close()
			log.Printf("tarantool: storing connection to %s failed: %s\n",
				e.id, err)
			return err
		}

		opened := p.handlerDiscovered(e.id, conn, role)
		if !opened {
			conn.Close()
			return errors.New("storing connection canceled")
		}

		p.poolsMutex.Lock()
		if p.state.get() != connectedState {
			p.poolsMutex.Unlock()
			conn.Close()
			p.handlerDeactivated(e.id, conn, role)
			return ErrClosed
		}

		if err = p.addConnection(e.id, conn, role); err != nil {
			p.poolsMutex.Unlock()
			conn.Close()
			p.handlerDeactivated(e.id, conn, role)
			return err
		}
		e.conn = conn
		e.role = role
	}

	p.poolsMutex.Unlock()
	return err
}

func (p *ConnectionPool) reconnect(ctx context.Context, e *endpoint) {
	p.poolsMutex.Lock()

	if p.state.get() != connectedState {
		p.poolsMutex.Unlock()
		return
	}

	p.deleteConnection(e.id)
	p.poolsMutex.Unlock()

	p.handlerDeactivated(e.id, e.conn, e.role)
	e.conn = nil
	e.role = UnknownRole

	p.tryConnect(ctx, e)
}

func (p *ConnectionPool) controller(ctx context.Context, e *endpoint) {
	timer := time.NewTicker(p.opts.CheckTimeout)
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
				p.poolsMutex.Lock()
				p.deleteConnection(e.id)
				p.poolsMutex.Unlock()

				if !shutdown {
					e.closeErr = e.conn.Close()
					p.handlerDeactivated(e.id, e.conn, e.role)
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
					p.poolsMutex.Lock()
					p.deleteConnection(e.id)
					p.poolsMutex.Unlock()

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
						p.poolsMutex.Lock()
						if p.state.get() == connectedState {
							p.deleteConnection(e.id)
							p.poolsMutex.Unlock()
							p.handlerDeactivated(e.id, e.conn, e.role)
							e.conn = nil
							e.role = UnknownRole
						} else {
							p.poolsMutex.Unlock()
						}
					}
				case <-timer.C:
					// Reopen connection.
					// Relocate connection between subpools
					// if ro/rw was updated.
					if e.conn == nil {
						p.tryConnect(ctx, e)
					} else if !e.conn.ClosedNow() {
						p.updateConnection(e)
					} else {
						p.reconnect(ctx, e)
					}
				}
			}
		}
	}
}

func (p *ConnectionPool) getNextConnection(mode Mode) (*tarantool.Connection, error) {

	switch mode {
	case ANY:
		if next := p.anyPool.GetNextConnection(); next != nil {
			return next, nil
		}
	case RW:
		if next := p.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
		return nil, ErrNoRwInstance
	case RO:
		if next := p.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
		return nil, ErrNoRoInstance
	case PreferRW:
		if next := p.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
		if next := p.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
	case PreferRO:
		if next := p.roPool.GetNextConnection(); next != nil {
			return next, nil
		}
		if next := p.rwPool.GetNextConnection(); next != nil {
			return next, nil
		}
	}
	return nil, ErrNoHealthyInstance
}

func (p *ConnectionPool) getConnByMode(defaultMode Mode,
	userMode []Mode) (*tarantool.Connection, error) {
	if len(userMode) > 1 {
		return nil, ErrTooManyArgs
	}

	mode := defaultMode
	if len(userMode) > 0 {
		mode = userMode[0]
	}

	return p.getNextConnection(mode)
}

func newErrorFuture(err error) *tarantool.Future {
	fut := tarantool.NewFuture()
	fut.SetError(err)
	return fut
}

func isFeatureInSlice(expected iproto.Feature, actualSlice []iproto.Feature) bool {
	for _, actual := range actualSlice {
		if expected == actual {
			return true
		}
	}
	return false
}
