package queue_test

import (
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool"
	"github.com/tarantool/go-tarantool/queue"
	"github.com/tarantool/go-tarantool/test_helpers"
	"gopkg.in/vmihailenco/msgpack.v2"
)

var server = "127.0.0.1:3013"
var opts = Opts{
	Timeout: 2500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
	//Concurrency: 32,
	//RateLimit: 4*1024,
}

/////////QUEUE/////////

func TestFifoQueue(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	//Drop
	if err = q.Drop(); err != nil {
		t.Errorf("Failed drop queue: %s", err.Error())
	}
}

func TestFifoQueue_GetExist_Statistic(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}
	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	ok, err := q.Exists()
	if err != nil {
		t.Errorf("Failed to get exist queue: %s", err.Error())
		return
	}
	if !ok {
		t.Error("Queue is not found")
		return
	}

	putData := "put_data"
	_, err = q.Put(putData)
	if err != nil {
		t.Errorf("Failed to put queue: %s", err.Error())
		return
	}

	stat, err := q.Statistic()
	if err != nil {
		t.Errorf("Failed to get statistic queue: %s", err.Error())
	} else if stat == nil {
		t.Error("Statistic is nil")
	}
}

func TestFifoQueue_Put(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err := q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}
}

func TestFifoQueue_Take(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
	} else if task == nil {
		t.Errorf("Task is nil after take")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())

		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err.Error())
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

type customData struct {
	customField string
}

func (c *customData) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l != 1 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if c.customField, err = d.DecodeString(); err != nil {
		return err
	}
	return nil
}

func (c *customData) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeSliceLen(1); err != nil {
		return err
	}
	if err := e.EncodeString(c.customField); err != nil {
		return err
	}
	return nil
}

func TestFifoQueue_TakeTyped(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	putData := &customData{customField: "put_data"}
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		typedData, ok := task.Data().(*customData)
		if !ok {
			t.Errorf("Task data after put has diferent type. %#v != %#v", task.Data(), putData)
		}
		if *typedData != *putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	takeData := &customData{}
	task, err = q.TakeTypedTimeout(2*time.Second, takeData)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
	} else if task == nil {
		t.Errorf("Task is nil after take")
	} else {
		typedData, ok := task.Data().(*customData)
		if !ok {
			t.Errorf("Task data after put has diferent type. %#v != %#v", task.Data(), putData)
		}
		if *typedData != *putData {
			t.Errorf("Task data after take not equal with example. %#v != %#v", task.Data(), putData)
		}
		if *takeData != *putData {
			t.Errorf("Task data after take not equal with example. %#v != %#v", task.Data(), putData)
		}
		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err.Error())
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestFifoQueue_Peek(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Peek
	task, err = q.Peek(task.Id())
	if err != nil {
		t.Errorf("Failed peek from queue: %s", err.Error())
	} else if task == nil {
		t.Errorf("Task is nil after peek")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after peek not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsReady() {
			t.Errorf("Task status after peek is not ready. Status = %s", task.Status())
		}
	}
}

func TestFifoQueue_Bury_Kick(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Bury
	err = task.Bury()
	if err != nil {
		t.Errorf("Failed bury task %s", err.Error())
		return
	} else if !task.IsBuried() {
		t.Errorf("Task status after bury is not buried. Status = %s", task.Status())
	}

	//Kick
	count, err := q.Kick(1)
	if err != nil {
		t.Errorf("Failed kick task %s", err.Error())
		return
	} else if count != 1 {
		t.Errorf("Kick result != 1")
		return
	}

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
	} else if task == nil {
		t.Errorf("Task is nil after take")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err.Error())
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestFifoQueue_Delete(t *testing.T) {
	var err error

	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	//Put
	var putData = "put_data"
	var tasks = [2]*queue.Task{}

	for i := 0; i < 2; i++ {
		tasks[i], err = q.Put(putData)
		if err != nil {
			t.Errorf("Failed put to queue: %s", err.Error())
			return
		} else if err == nil && tasks[i] == nil {
			t.Errorf("Task is nil after put")
			return
		} else {
			if tasks[i].Data() != putData {
				t.Errorf("Task data after put not equal with example. %s != %s", tasks[i].Data(), putData)
			}
		}
	}

	//Delete by task method
	err = tasks[0].Delete()
	if err != nil {
		t.Errorf("Failed bury task %s", err.Error())
		return
	} else if !tasks[0].IsDone() {
		t.Errorf("Task status after delete is not done. Status = %s", tasks[0].Status())
	}

	//Delete by task ID
	err = q.Delete(tasks[1].Id())
	if err != nil {
		t.Errorf("Failed bury task %s", err.Error())
		return
	} else if !tasks[0].IsDone() {
		t.Errorf("Task status after delete is not done. Status = %s", tasks[0].Status())
	}

	//Take
	for i := 0; i < 2; i++ {
		tasks[i], err = q.TakeTimeout(2 * time.Second)
		if err != nil {
			t.Errorf("Failed take from queue: %s", err.Error())
		} else if tasks[i] != nil {
			t.Errorf("Task is not nil after take. Task is %d", tasks[i].Id())
		}
	}
}

func TestFifoQueue_Release(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	q := queue.New(conn, name)
	if err = q.Create(queue.Cfg{Temporary: true, Kind: queue.FIFO}); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
		return
	} else if task == nil {
		t.Error("Task is nil after take")
		return
	}

	//Release
	err = task.Release()
	if err != nil {
		t.Errorf("Failed release task %s", err.Error())
		return
	}

	if !task.IsReady() {
		t.Errorf("Task status is not ready, but %s", task.Status())
		return
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
		return
	} else if task == nil {
		t.Error("Task is nil after take")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err.Error())
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestTtlQueue(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	defer conn.Close()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := queue.New(conn, name)
	if err = q.Create(cfg); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	time.Sleep(5 * time.Second)

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
	} else if task != nil {
		t.Errorf("Task is not nil after sleep")
	}
}

func TestTtlQueue_Put(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := queue.New(conn, name)
	if err = q.Create(cfg); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}

	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	putData := "put_data"
	task, err := q.PutWithOpts(putData, queue.Opts{Ttl: 10 * time.Second})
	if err != nil {
		t.Errorf("Failed put to queue: %s", err.Error())
		return
	} else if err == nil && task == nil {
		t.Errorf("Task is nil after put")
		return
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	time.Sleep(5 * time.Second)

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err.Error())
	} else if task == nil {
		t.Errorf("Task is nil after sleep")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err.Error())
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestUtube_Put(t *testing.T) {
	conn, err := Connect(server, opts)
	if err != nil {
		t.Errorf("Failed to connect: %s", err.Error())
		return
	}
	if conn == nil {
		t.Errorf("conn is nil after Connect")
		return
	}
	defer conn.Close()

	name := "test_utube"
	cfg := queue.Cfg{
		Temporary:   true,
		Kind:        queue.UTUBE,
		IfNotExists: true,
	}
	q := queue.New(conn, name)
	if err = q.Create(cfg); err != nil {
		t.Errorf("Failed to create queue: %s", err.Error())
		return
	}
	defer func() {
		//Drop
		err := q.Drop()
		if err != nil {
			t.Errorf("Failed drop queue: %s", err.Error())
		}
	}()

	data1 := &customData{"test-data-0"}
	_, err = q.PutWithOpts(data1, queue.Opts{Utube: "test-utube-consumer-key"})
	if err != nil {
		t.Fatalf("Failed put task to queue: %s", err.Error())
	}
	data2 := &customData{"test-data-1"}
	_, err = q.PutWithOpts(data2, queue.Opts{Utube: "test-utube-consumer-key"})
	if err != nil {
		t.Fatalf("Failed put task to queue: %s", err.Error())
	}

	go func() {
		t1, err := q.TakeTimeout(2 * time.Second)
		if err != nil {
			t.Fatalf("Failed to take task from utube: %s", err.Error())
		}

		time.Sleep(2 * time.Second)
		if err := t1.Ack(); err != nil {
			t.Fatalf("Failed to ack task: %s", err.Error())
		}
	}()

	time.Sleep(100 * time.Millisecond)
	// the queue should be blocked for ~2 seconds
	start := time.Now()
	t2, err := q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to take task from utube: %s", err.Error())
	}
	if err := t2.Ack(); err != nil {
		t.Fatalf("Failed to ack task: %s", err.Error())
	}
	end := time.Now()
	if math.Abs(float64(end.Sub(start)-2*time.Second)) > float64(200*time.Millisecond) {
		t.Fatalf("Blocking time is less than expected: actual = %.2fs, expected = 1s", end.Sub(start).Seconds())
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "config.lua",
		Listen:       server,
		WorkDir:      "work_dir",
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(inst)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
