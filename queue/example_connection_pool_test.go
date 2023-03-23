package queue_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/connection_pool"
	"github.com/ice-blockchain/go-tarantool/queue"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

// QueueConnectionHandler handles new connections in a ConnectionPool.
type QueueConnectionHandler struct {
	name string
	cfg  queue.Cfg

	uuid          uuid.UUID
	registered    bool
	err           error
	mutex         sync.Mutex
	masterUpdated chan struct{}
	masterCnt     int32
}

// QueueConnectionHandler implements the ConnectionHandler interface.
var _ connection_pool.ConnectionHandler = &QueueConnectionHandler{}

// NewQueueConnectionHandler creates a QueueConnectionHandler object.
func NewQueueConnectionHandler(name string, cfg queue.Cfg) *QueueConnectionHandler {
	return &QueueConnectionHandler{
		name:          name,
		cfg:           cfg,
		masterUpdated: make(chan struct{}, 10),
	}
}

// Discovered configures a queue for an instance and identifies a shared queue
// session on master instances.
//
// NOTE: the Queue supports only a master-replica cluster configuration. It
// does not support a master-master configuration.
func (h *QueueConnectionHandler) Discovered(conn *tarantool.Connection,
	role connection_pool.Role) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.err != nil {
		return h.err
	}

	master := role == connection_pool.MasterRole
	if master {
		defer func() {
			h.masterUpdated <- struct{}{}
		}()
	}

	// Set up a queue module configuration for an instance.
	q := queue.New(conn, h.name)
	opts := queue.CfgOpts{InReplicaset: true, Ttr: 60 * time.Second}

	if h.err = q.Cfg(opts); h.err != nil {
		return fmt.Errorf("unable to configure queue: %w", h.err)
	}

	// The queue only works with a master instance.
	if !master {
		return nil
	}

	if h.err = q.Create(h.cfg); h.err != nil {
		return h.err
	}

	if !h.registered {
		// We register a shared session at the first time.
		if h.uuid, h.err = q.Identify(nil); h.err != nil {
			return h.err
		}
		h.registered = true
	} else {
		// We re-identify as the shared session.
		if _, h.err = q.Identify(&h.uuid); h.err != nil {
			return h.err
		}
	}

	fmt.Printf("Master %s is ready to work!\n", conn.Addr())
	atomic.AddInt32(&h.masterCnt, 1)

	return nil
}

// Deactivated doesn't do anything useful for the example.
func (h *QueueConnectionHandler) Deactivated(conn *tarantool.Connection,
	role connection_pool.Role) error {
	if role == connection_pool.MasterRole {
		atomic.AddInt32(&h.masterCnt, -1)
	}
	return nil
}

// Closes closes a QueueConnectionHandler object.
func (h *QueueConnectionHandler) Close() {
	close(h.masterUpdated)
}

// Example demonstrates how to use the queue package with the connection_pool
// package. First of all, you need to create a ConnectionHandler implementation
// for the a ConnectionPool object to process new connections from
// RW-instances.
//
// You need to register a shared session UUID at a first master connection.
// It needs to be used to re-identify as the shared session on new
// RW-instances. See QueueConnectionHandler.Discovered() implementation.
//
// After that, you need to create a ConnectorAdapter object with RW mode for
// the ConnectionPool to send requests into RW-instances. This adapter can
// be used to create a ready-to-work queue object.
func Example_connectionPool() {
	// Create a ConnectionHandler object.
	cfg := queue.Cfg{
		Temporary:   false,
		IfNotExists: true,
		Kind:        queue.FIFO,
		Opts: queue.Opts{
			Ttl: 10 * time.Second,
		},
	}
	h := NewQueueConnectionHandler("test_queue", cfg)
	defer h.Close()

	// Create a ConnectionPool object.
	servers := []string{
		"127.0.0.1:3014",
		"127.0.0.1:3015",
	}
	connOpts := tarantool.Opts{
		Timeout: 1 * time.Second,
		User:    "test",
		Pass:    "test",
	}
	poolOpts := connection_pool.OptsPool{
		CheckTimeout:      1 * time.Second,
		ConnectionHandler: h,
	}
	connPool, err := connection_pool.ConnectWithOpts(servers, connOpts, poolOpts)
	if err != nil {
		fmt.Printf("Unable to connect to the pool: %s", err)
		return
	}
	defer connPool.Close()

	// Wait for a master instance identification in the queue.
	<-h.masterUpdated
	if h.err != nil {
		fmt.Printf("Unable to identify in the pool: %s", h.err)
		return
	}

	// Create a Queue object from the ConnectionPool object via
	// a ConnectorAdapter.
	rw := connection_pool.NewConnectorAdapter(connPool, connection_pool.RW)
	q := queue.New(rw, "test_queue")
	fmt.Println("A Queue object is ready to work.")

	testData := "test_data"
	fmt.Println("Send data:", testData)
	if _, err = q.Put(testData); err != nil {
		fmt.Printf("Unable to put data into the queue: %s", err)
		return
	}

	// Switch a master instance in the pool.
	roles := []bool{true, false}
	err = test_helpers.SetClusterRO(servers, connOpts, roles)
	if err != nil {
		fmt.Printf("Unable to set cluster roles: %s", err)
		return
	}

	// Wait for a new master instance re-identification.
	<-h.masterUpdated
	h.mutex.Lock()
	err = h.err
	h.mutex.Unlock()

	if err != nil {
		fmt.Printf("Unable to re-identify in the pool: %s", err)
		return
	}

	for i := 0; i < 2 && atomic.LoadInt32(&h.masterCnt) != 1; i++ {
		// The pool does not immediately detect role switching. It may happen
		// that requests will be sent to RO instances. In that case q.Take()
		// method will return a nil value.
		//
		// We need to make the example test output deterministic so we need to
		// avoid it here. But in real life, you need to take this into account.
		time.Sleep(poolOpts.CheckTimeout)
	}

	// Take a data from the new master instance.
	task, err := q.Take()
	if err != nil {
		fmt.Println("Unable to got task:", err)
	} else if task == nil {
		fmt.Println("task == nil")
	} else if task.Data() == nil {
		fmt.Println("task.Data() == nil")
	} else {
		task.Ack()
		fmt.Println("Got data:", task.Data())
	}

	// Output:
	// Master 127.0.0.1:3014 is ready to work!
	// A Queue object is ready to work.
	// Send data: test_data
	// Master 127.0.0.1:3015 is ready to work!
	// Got data: test_data
}
