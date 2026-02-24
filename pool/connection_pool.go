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
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v3"
)

var (
	ErrWrongCheckTimeout = errors.New("wrong check timeout, must be greater than 0")
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
	Discovered(name string, conn *tarantool.Connection, role Role) error
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
	Deactivated(name string, conn *tarantool.Connection, role Role) error
}

// Instance describes a single instance configuration in the pool.
type Instance struct {
	// Name is an instance name. The name must be unique.
	Name string
	// Dialer will be used to create a connection to the instance.
	Dialer tarantool.Dialer
	// Opts configures a connection to the instance.
	Opts tarantool.Opts
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
	Instance     Instance
}

/*
Main features:

- Return available connection from pool according to round-robin strategy.

- Automatic master discovery by mode parameter.
*/
type ConnectionPool struct {
	ends      map[string]*endpoint
	endsMutex sync.RWMutex

	opts Opts

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
	name   string
	dialer tarantool.Dialer
	opts   tarantool.Opts
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

func newEndpoint(name string, dialer tarantool.Dialer, opts tarantool.Opts) *endpoint {
	return &endpoint{
		name:     name,
		dialer:   dialer,
		opts:     opts,
		notify:   make(chan tarantool.ConnEvent, 100),
		conn:     nil,
		role:     UnknownRole,
		shutdown: make(chan struct{}),
		close:    make(chan struct{}),
		closed:   make(chan struct{}),
		cancel:   nil,
	}
}

// ConnectWithOpts creates pool for instances with specified instances and
// opts. Instances must have unique names.
func ConnectWithOpts(ctx context.Context, instances []Instance,
	opts Opts) (*ConnectionPool, error) {
	unique := make(map[string]bool)
	for _, instance := range instances {
		if _, ok := unique[instance.Name]; ok {
			return nil, fmt.Errorf("duplicate instance name: %q", instance.Name)
		}
		unique[instance.Name] = true
	}

	if opts.CheckTimeout <= 0 {
		return nil, ErrWrongCheckTimeout
	}

	size := len(instances)
	rwPool := newRoundRobinStrategy(size)
	roPool := newRoundRobinStrategy(size)
	anyPool := newRoundRobinStrategy(size)

	p := &ConnectionPool{
		ends:    make(map[string]*endpoint),
		opts:    opts,
		state:   connectedState,
		done:    make(chan struct{}),
		rwPool:  rwPool,
		roPool:  roPool,
		anyPool: anyPool,
	}

	fillCtx, fillCancel := context.WithCancel(ctx)
	defer fillCancel()

	var timeout <-chan time.Time

	timeout = make(chan time.Time)
	filled := p.fillPools(fillCtx, instances)
	done := 0
	success := len(instances) == 0

	for done < len(instances) {
		select {
		case <-timeout:
			fillCancel()
			// To be sure that the branch is called only once.
			timeout = make(chan time.Time)
		case err := <-filled:
			done++

			if err == nil && !success {
				timeout = time.After(opts.CheckTimeout)
				success = true
			}
		}
	}

	if !success && ctx.Err() != nil {
		p.state.set(closedState)
		return nil, ctx.Err()
	}

	for _, endpoint := range p.ends {
		endpointCtx, cancel := context.WithCancel(context.Background())
		endpoint.cancel = cancel
		go p.controller(endpointCtx, endpoint)
	}

	return p, nil
}

// Connect creates pool for instances with specified instances. Instances must
// have unique names.
//
// It is useless to set up tarantool.Opts.Reconnect value for a connection.
// The connection pool has its own reconnection logic. See
// Opts.CheckTimeout description.
func Connect(ctx context.Context, instances []Instance) (*ConnectionPool, error) {
	opts := Opts{
		CheckTimeout: 1 * time.Second,
	}
	return ConnectWithOpts(ctx, instances, opts)
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

// Add adds a new instance into the pool. The pool will try to connect to the
// instance later if it is unable to establish a connection.
//
// The function may return an error and don't add the instance into the pool
// if the context has been cancelled or on concurrent Close()/CloseGraceful()
// call.
func (p *ConnectionPool) Add(ctx context.Context, instance Instance) error {
	e := newEndpoint(instance.Name, instance.Dialer, instance.Opts)

	p.endsMutex.Lock()
	// Ensure that Close()/CloseGraceful() not in progress/done.
	if p.state.get() != connectedState {
		p.endsMutex.Unlock()
		return ErrClosed
	}
	if _, ok := p.ends[instance.Name]; ok {
		p.endsMutex.Unlock()
		return ErrExists
	}

	endpointCtx, endpointCancel := context.WithCancel(context.Background())
	connectCtx, connectCancel := context.WithCancel(ctx)
	e.cancel = func() {
		connectCancel()
		endpointCancel()
	}

	p.ends[instance.Name] = e
	p.endsMutex.Unlock()

	if err := p.tryConnect(connectCtx, e); err != nil {
		var canceled bool
		select {
		case <-connectCtx.Done():
			canceled = true
		case <-endpointCtx.Done():
			canceled = true
		default:
			canceled = false
		}
		if canceled {
			if p.state.get() != connectedState {
				// If it is canceled (or could be canceled) due to a
				// Close()/CloseGraceful() call we overwrite the error
				// to make behavior expected.
				err = ErrClosed
			}

			p.endsMutex.Lock()
			delete(p.ends, instance.Name)
			p.endsMutex.Unlock()
			e.cancel()
			close(e.closed)
			return err
		} else {
			log.Printf("tarantool: connect to %s failed: %s\n", instance.Name, err)
		}
	}

	go p.controller(endpointCtx, e)
	return nil
}

// Remove removes an endpoint with the name from the pool. The call
// closes an active connection gracefully.
func (p *ConnectionPool) Remove(name string) error {
	p.endsMutex.Lock()
	endpoint, ok := p.ends[name]
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

	delete(p.ends, name)
	p.endsMutex.Unlock()

	<-endpoint.closed
	return nil
}

func (p *ConnectionPool) waitClose() error {
	p.endsMutex.RLock()
	endpoints := make([]*endpoint, 0, len(p.ends))
	for _, e := range p.ends {
		endpoints = append(endpoints, e)
	}
	p.endsMutex.RUnlock()

	var errs error
	for _, e := range endpoints {
		<-e.closed
		if e.closeErr != nil {
			errs = errors.Join(errs, e.closeErr)
		}
	}
	return errs
}

// Close closes connections in the ConnectionPool.
func (p *ConnectionPool) Close() error {
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
func (p *ConnectionPool) CloseGraceful() error {
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

	for name, end := range p.ends {
		conn, role := p.getConnectionFromPool(name)

		connInfo := ConnectionInfo{
			ConnectedNow: false,
			ConnRole:     UnknownRole,
			Instance: Instance{
				Name:   name,
				Dialer: end.dialer,
				Opts:   end.opts,
			},
		}

		if conn != nil {
			connInfo.ConnRole = role
			connInfo.ConnectedNow = conn.ConnectedNow()
		}

		info[name] = connInfo
	}

	return info
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
func (p *ConnectionPool) Do(req tarantool.Request, userMode Mode) tarantool.Future {
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
			return tarantool.NewFutureWithErr(nil, ErrUnknownRequest)
		}
		return connectedReq.Conn().Do(req)
	}
	conn, err := p.getNextConnection(userMode)
	if err != nil {
		return tarantool.NewFutureWithErr(nil, err)
	}

	return conn.Do(req)
}

// DoInstance sends the request into a target instance and returns a future.
func (p *ConnectionPool) DoInstance(req tarantool.Request, name string) tarantool.Future {
	conn := p.anyPool.GetConnection(name)
	if conn == nil {
		return tarantool.NewFutureWithErr(nil, ErrNoHealthyInstance)
	}

	return conn.Do(req)
}

//
// private
//

func (p *ConnectionPool) getConnectionRole(conn *tarantool.Connection) (Role, error) {
	var (
		roFieldName string
		data        []interface{}
		err         error
	)

	if isFeatureInSlice(iproto.IPROTO_FEATURE_WATCH_ONCE, conn.ProtocolInfo().Features) {
		roFieldName = "is_ro"
		data, err = conn.Do(tarantool.NewWatchOnceRequest("box.status")).Get()
	} else {
		roFieldName = "ro"
		data, err = conn.Do(tarantool.NewCallRequest("box.info")).Get()
	}

	if err != nil {
		return UnknownRole, err
	}
	if len(data) < 1 {
		return UnknownRole, ErrIncorrectResponse
	}

	respFields, ok := data[0].(map[interface{}]interface{})
	if !ok {
		return UnknownRole, ErrIncorrectResponse
	}

	instanceStatus, ok := respFields["status"]
	if !ok {
		return UnknownRole, ErrIncorrectResponse
	}
	if instanceStatus != "running" {
		return UnknownRole, ErrIncorrectStatus
	}

	replicaRole, ok := respFields[roFieldName]
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

func (p *ConnectionPool) getConnectionFromPool(name string) (*tarantool.Connection, Role) {
	if conn := p.rwPool.GetConnection(name); conn != nil {
		return conn, MasterRole
	}

	if conn := p.roPool.GetConnection(name); conn != nil {
		return conn, ReplicaRole
	}

	return p.anyPool.GetConnection(name), UnknownRole
}

func (p *ConnectionPool) deleteConnection(name string) {
	if conn := p.anyPool.DeleteConnection(name); conn != nil {
		if conn := p.rwPool.DeleteConnection(name); conn == nil {
			p.roPool.DeleteConnection(name)
		}
		// The internal connection deinitialization.
		p.watcherContainer.mutex.RLock()
		defer p.watcherContainer.mutex.RUnlock()

		_ = p.watcherContainer.foreach(func(watcher *poolWatcher) error {
			watcher.unwatch(conn)
			return nil
		})
	}
}

func (p *ConnectionPool) addConnection(name string,
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
			log.Printf("tarantool: failed initialize watchers for %s: %s", name, err)
			return err
		}
	}

	p.anyPool.AddConnection(name, conn)

	switch role {
	case MasterRole:
		p.rwPool.AddConnection(name, conn)
	case ReplicaRole:
		p.roPool.AddConnection(name, conn)
	}
	return nil
}

func (p *ConnectionPool) handlerDiscovered(name string, conn *tarantool.Connection,
	role Role) bool {
	var err error
	if p.opts.ConnectionHandler != nil {
		err = p.opts.ConnectionHandler.Discovered(name, conn, role)
	}

	if err != nil {
		log.Printf("tarantool: storing connection to %s canceled: %s\n", name, err)
		return false
	}
	return true
}

func (p *ConnectionPool) handlerDeactivated(name string, conn *tarantool.Connection,
	role Role) {
	var err error
	if p.opts.ConnectionHandler != nil {
		err = p.opts.ConnectionHandler.Deactivated(name, conn, role)
	}

	if err != nil {
		log.Printf("tarantool: deactivating connection to %s by user failed: %s\n",
			name, err)
	}
}

func (p *ConnectionPool) fillPools(ctx context.Context, instances []Instance) <-chan error {
	done := make(chan error, len(instances))

	// It is called before controller() goroutines, so we don't expect
	// concurrency issues here.
	for _, instance := range instances {
		end := newEndpoint(instance.Name, instance.Dialer, instance.Opts)
		p.ends[instance.Name] = end
	}

	for _, instance := range instances {
		name := instance.Name
		end := p.ends[name]

		go func() {
			if err := p.tryConnect(ctx, end); err != nil {
				log.Printf("tarantool: connect to %s failed: %s\n", name, err)
				done <- fmt.Errorf("failed to connect to %s :%w", name, err)

				return
			}

			done <- nil
		}()
	}

	return done
}

func (p *ConnectionPool) updateConnection(e *endpoint) {
	p.poolsMutex.Lock()

	if p.state.get() != connectedState {
		p.poolsMutex.Unlock()
		return
	}

	if role, err := p.getConnectionRole(e.conn); err == nil {
		if e.role != role {
			p.deleteConnection(e.name)
			p.poolsMutex.Unlock()

			p.handlerDeactivated(e.name, e.conn, e.role)
			opened := p.handlerDiscovered(e.name, e.conn, role)
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
				p.handlerDeactivated(e.name, e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}

			if p.addConnection(e.name, e.conn, role) != nil {
				p.poolsMutex.Unlock()

				e.conn.Close()
				p.handlerDeactivated(e.name, e.conn, role)
				e.conn = nil
				e.role = UnknownRole
				return
			}
			e.role = role
		}
		p.poolsMutex.Unlock()
		return
	} else {
		p.deleteConnection(e.name)
		p.poolsMutex.Unlock()

		e.conn.Close()
		p.handlerDeactivated(e.name, e.conn, e.role)
		e.conn = nil
		e.role = UnknownRole
		return
	}
}

func (p *ConnectionPool) tryConnect(ctx context.Context, e *endpoint) error {
	e.conn = nil
	e.role = UnknownRole

	connOpts := e.opts
	connOpts.Notify = e.notify
	conn, err := tarantool.Connect(ctx, e.dialer, connOpts)

	p.poolsMutex.Lock()

	if p.state.get() != connectedState {
		if err == nil {
			conn.Close()
		}

		p.poolsMutex.Unlock()
		return ErrClosed
	}

	if err == nil {
		role, err := p.getConnectionRole(conn)
		p.poolsMutex.Unlock()

		if err != nil {
			conn.Close()
			log.Printf("tarantool: storing connection to %s failed: %s\n",
				e.name, err)
			return err
		}

		opened := p.handlerDiscovered(e.name, conn, role)
		if !opened {
			conn.Close()
			return errors.New("storing connection canceled")
		}

		p.poolsMutex.Lock()
		if p.state.get() != connectedState {
			p.poolsMutex.Unlock()
			conn.Close()
			p.handlerDeactivated(e.name, conn, role)
			return ErrClosed
		}

		if err = p.addConnection(e.name, conn, role); err != nil {
			p.poolsMutex.Unlock()
			conn.Close()
			p.handlerDeactivated(e.name, conn, role)
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

	p.deleteConnection(e.name)
	p.poolsMutex.Unlock()

	p.handlerDeactivated(e.name, e.conn, e.role)
	e.conn = nil
	e.role = UnknownRole

	if err := p.tryConnect(ctx, e); err != nil {
		log.Printf("tarantool: reconnect to %s failed: %s\n", e.name, err)
	}
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
				p.deleteConnection(e.name)
				p.poolsMutex.Unlock()

				if !shutdown {
					e.closeErr = e.conn.Close()
					p.handlerDeactivated(e.name, e.conn, e.role)
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
					p.deleteConnection(e.name)
					p.poolsMutex.Unlock()

					// We need to catch s.close in the current goroutine, so
					// we need to start an another one for the shutdown.
					go func() {
						e.closeErr = e.conn.CloseGraceful()
						p.handlerDeactivated(e.name, e.conn, e.role)
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
							p.deleteConnection(e.name)
							p.poolsMutex.Unlock()
							p.handlerDeactivated(e.name, e.conn, e.role)
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
						if err := p.tryConnect(ctx, e); err != nil {
							log.Printf("tarantool: reopen connection to %s failed: %s\n",
								e.name, err)
						}
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

func isFeatureInSlice(expected iproto.Feature, actualSlice []iproto.Feature) bool {
	for _, actual := range actualSlice {
		if expected == actual {
			return true
		}
	}
	return false
}
