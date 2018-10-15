package queue_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/queue"
)

func ExampleConnection_Queue() {
	cfg := queue.Cfg{
		Temporary: false,
		Kind:      queue.FIFO,
		Opts:      queue.Opts{
			Ttl: 10 * time.Second,
		},
	}

	conn, err := tarantool.Connect(server, opts)
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
	if task != nil {
		fmt.Printf("Task should be nil, but %s", task)
		return
	}
}
