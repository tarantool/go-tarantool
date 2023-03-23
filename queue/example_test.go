// Setup queue module and start Tarantool instance before execution:
// Terminal 1:
// $ make deps
// $ TEST_TNT_LISTEN=3013 tarantool queue/config.lua
//
// Terminal 2:
// $ cd queue
// $ go test -v example_test.go
package queue_test

import (
	"fmt"
	"time"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/queue"
)

// Example demonstrates an operations like Put and Take with queue.
func Example_simpleQueue() {
	cfg := queue.Cfg{
		Temporary: false,
		Kind:      queue.FIFO,
		Opts: queue.Opts{
			Ttl: 10 * time.Second,
		},
	}
	opts := tarantool.Opts{
		Timeout: 2500 * time.Millisecond,
		User:    "test",
		Pass:    "test",
	}

	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		fmt.Printf("error in prepare is %v", err)
		return
	}
	defer conn.Close()

	q := queue.New(conn, "test_queue")
	if err := q.Create(cfg); err != nil {
		fmt.Printf("error in queue is %v", err)
		return
	}

	defer q.Drop()

	testData_1 := "test_data_1"
	if _, err = q.Put(testData_1); err != nil {
		fmt.Printf("error in put is %v", err)
		return
	}

	testData_2 := "test_data_2"
	task_2, err := q.PutWithOpts(testData_2, queue.Opts{Ttl: 2 * time.Second})
	if err != nil {
		fmt.Printf("error in put with config is %v", err)
		return
	}

	task, err := q.Take()
	if err != nil {
		fmt.Printf("error in take with is %v", err)
		return
	}
	task.Ack()
	fmt.Println("data_1: ", task.Data())

	err = task_2.Bury()
	if err != nil {
		fmt.Printf("error in bury with is %v", err)
		return
	}

	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		fmt.Printf("error in take with timeout")
	}
	if task != nil {
		fmt.Printf("Task should be nil, but %d", task.Id())
		return
	}

	// Output: data_1:  test_data_1
}
