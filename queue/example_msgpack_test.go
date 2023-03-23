// Setup queue module and start Tarantool instance before execution:
// Terminal 1:
// $ make deps
// $ TEST_TNT_LISTEN=3013 tarantool queue/config.lua
//
// Terminal 2:
// $ cd queue
// $ go test -v example_msgpack_test.go
package queue_test

import (
	"fmt"
	"log"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/queue"
)

type dummyData struct {
	Dummy bool
}

func (c *dummyData) DecodeMsgpack(d *decoder) error {
	var err error
	if c.Dummy, err = d.DecodeBool(); err != nil {
		return err
	}
	return nil
}

func (c *dummyData) EncodeMsgpack(e *encoder) error {
	return e.EncodeBool(c.Dummy)
}

// Example demonstrates an operations like Put and Take with queue and custom
// MsgPack structure.
//
// Features of the implementation:
//
// - If you use the connection timeout and call TakeWithTimeout with a
// parameter greater than the connection timeout, the parameter is reduced to
// it.
//
// - If you use the connection timeout and call Take, we return an error if we
// cannot take the task out of the queue within the time corresponding to the
// connection timeout.
func Example_simpleQueueCustomMsgPack() {
	opts := tarantool.Opts{
		Reconnect:     time.Second,
		Timeout:       2500 * time.Millisecond,
		MaxReconnects: 5,
		User:          "test",
		Pass:          "test",
	}
	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		log.Fatalf("connection: %s", err)
		return
	}
	defer conn.Close()

	cfg := queue.Cfg{
		Temporary:   true,
		IfNotExists: true,
		Kind:        queue.FIFO,
		Opts: queue.Opts{
			Ttl:   10 * time.Second,
			Ttr:   5 * time.Second,
			Delay: 3 * time.Second,
			Pri:   1,
		},
	}

	que := queue.New(conn, "test_queue_msgpack")
	if err = que.Create(cfg); err != nil {
		log.Fatalf("queue create: %s", err)
		return
	}

	// Put data.
	task, err := que.Put("test_data")
	if err != nil {
		log.Fatalf("put task: %s", err)
	}
	fmt.Println("Task id is", task.Id())

	// Take data.
	task, err = que.Take() // Blocking operation.
	if err != nil {
		log.Fatalf("take task: %s", err)
	}
	fmt.Println("Data is", task.Data())
	task.Ack()

	// Take typed example.
	putData := dummyData{}
	// Put data.
	task, err = que.Put(&putData)
	if err != nil {
		log.Fatalf("put typed task: %s", err)
	}
	fmt.Println("Task id is ", task.Id())

	takeData := dummyData{}
	// Take data.
	task, err = que.TakeTyped(&takeData) // Blocking operation.
	if err != nil {
		log.Fatalf("take take typed: %s", err)
	}
	fmt.Println("Data is ", takeData)
	// Same data.
	fmt.Println("Data is ", task.Data())

	task, err = que.Put([]int{1, 2, 3})
	if err != nil {
		log.Fatalf("Put failed: %s", err)
	}
	task.Bury()

	task, err = que.TakeTimeout(2 * time.Second)
	if err != nil {
		log.Fatalf("Take with timeout failed: %s", err)
	}
	if task == nil {
		fmt.Println("Task is nil")
	}

	que.Drop()

	// Unordered output:
	// Task id is 0
	// Data is test_data
	// Task id is  0
	// Data is  {false}
	// Data is  &{false}
	// Task is nil
}
