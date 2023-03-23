package queue_test

import (
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/queue"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

var server = "127.0.0.1:3013"
var serversPool = []string{
	"127.0.0.1:3014",
	"127.0.0.1:3015",
}

var instances []test_helpers.TarantoolInstance

var opts = Opts{
	Timeout: 2500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
	//Concurrency: 32,
	//RateLimit: 4*1024,
}

func createQueue(t *testing.T, conn *Connection, name string, cfg queue.Cfg) queue.Queue {
	t.Helper()

	q := queue.New(conn, name)
	if err := q.Create(cfg); err != nil {
		t.Fatalf("Failed to create queue: %s", err)
	}

	return q
}

func dropQueue(t *testing.T, q queue.Queue) {
	t.Helper()

	if err := q.Drop(); err != nil {
		t.Fatalf("Failed to drop queue: %s", err)
	}
}

/////////QUEUE/////////

func TestFifoQueue(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)
}

func TestQueue_Cfg(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	err := q.Cfg(queue.CfgOpts{InReplicaset: false, Ttr: 5 * time.Second})
	if err != nil {
		t.Fatalf("Unexpected q.Cfg() error: %s", err)
	}
}

func TestQueue_Identify(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	uuid, err := q.Identify(nil)
	if err != nil {
		t.Fatalf("Failed to identify: %s", err)
	}
	cpy := uuid

	uuid, err = q.Identify(&cpy)
	if err != nil {
		t.Fatalf("Failed to identify with uuid %s: %s", cpy, err)
	}
	if cpy.String() != uuid.String() {
		t.Fatalf("Unequal UUIDs after re-identify: %s, expected %s", uuid, cpy)
	}
}

func TestQueue_ReIdentify(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := createQueue(t, conn, name, cfg)
	q.Cfg(queue.CfgOpts{InReplicaset: false, Ttr: 5 * time.Second})
	defer func() {
		dropQueue(t, q)
	}()

	uuid, err := q.Identify(nil)
	if err != nil {
		t.Fatalf("Failed to identify: %s", err)
	}
	newuuid, err := q.Identify(&uuid)
	if err != nil {
		t.Fatalf("Failed to identify: %s", err)
	}
	if newuuid.String() != uuid.String() {
		t.Fatalf("Unequal UUIDs after re-identify: %s, expected %s", newuuid, uuid)
	}
	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		conn.Close()
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatalf("Task is nil after take")
	}

	conn.Close()
	conn = nil

	conn = test_helpers.ConnectWithValidation(t, server, opts)
	q = queue.New(conn, name)

	//Identify in another connection
	newuuid, err = q.Identify(&uuid)
	if err != nil {
		t.Fatalf("Failed to identify: %s", err)
	}
	if newuuid.String() != uuid.String() {
		t.Fatalf("Unequal UUIDs after re-identify: %s, expected %s", newuuid, uuid)
	}

	//Peek in another connection
	task, err = q.Peek(task.Id())
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatalf("Task is nil after take")
	}

	//Ack in another connection
	err = task.Ack()
	if err != nil {
		t.Errorf("Failed ack %s", err)
	} else if !task.IsDone() {
		t.Errorf("Task status after take is not done. Status = %s", task.Status())
	}
}

func TestQueue_State(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	state, err := q.State()
	if err != nil {
		t.Fatalf("Failed to get queue state: %s", err)
	}
	if state != queue.InitState && state != queue.RunningState {
		t.Fatalf("Unexpected state: %d", state)
	}
}

func TestFifoQueue_GetExist_Statistic(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	ok, err := q.Exists()
	if err != nil {
		t.Fatalf("Failed to get exist queue: %s", err)
	}
	if !ok {
		t.Fatal("Queue is not found")
	}

	putData := "put_data"
	_, err = q.Put(putData)
	if err != nil {
		t.Fatalf("Failed to put queue: %s", err)
	}

	stat, err := q.Statistic()
	if err != nil {
		t.Errorf("Failed to get statistic queue: %s", err)
	} else if stat == nil {
		t.Error("Statistic is nil")
	}
}

func TestFifoQueue_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}
}

func TestFifoQueue_Take(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err)
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
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

type customData struct {
	customField string
}

func (c *customData) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
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

func (c *customData) EncodeMsgpack(e *encoder) error {
	if err := e.EncodeArrayLen(1); err != nil {
		return err
	}
	if err := e.EncodeString(c.customField); err != nil {
		return err
	}
	return nil
}

func TestFifoQueue_TakeTyped(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	putData := &customData{customField: "put_data"}
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		typedData, ok := task.Data().(*customData)
		if !ok {
			t.Errorf("Task data after put has different type. %#v != %#v", task.Data(), putData)
		}
		if *typedData != *putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	takeData := &customData{}
	task, err = q.TakeTypedTimeout(2*time.Second, takeData)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Errorf("Task is nil after take")
	} else {
		typedData, ok := task.Data().(*customData)
		if !ok {
			t.Errorf("Task data after put has different type. %#v != %#v", task.Data(), putData)
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
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestFifoQueue_Peek(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Peek
	task, err = q.Peek(task.Id())
	if err != nil {
		t.Errorf("Failed peek from queue: %s", err)
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
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Bury
	err = task.Bury()
	if err != nil {
		t.Fatalf("Failed bury task %s", err)
	} else if !task.IsBuried() {
		t.Errorf("Task status after bury is not buried. Status = %s", task.Status())
	}

	//Kick
	count, err := q.Kick(1)
	if err != nil {
		t.Fatalf("Failed kick task %s", err)
	} else if count != 1 {
		t.Fatalf("Kick result != 1")
	}

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err)
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
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestFifoQueue_Delete(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	//Put
	var putData = "put_data"
	var tasks = [2]*queue.Task{}

	for i := 0; i < 2; i++ {
		tasks[i], err = q.Put(putData)
		if err != nil {
			t.Fatalf("Failed put to queue: %s", err)
		} else if err == nil && tasks[i] == nil {
			t.Fatalf("Task is nil after put")
		} else {
			if tasks[i].Data() != putData {
				t.Errorf("Task data after put not equal with example. %s != %s", tasks[i].Data(), putData)
			}
		}
	}

	//Delete by task method
	err = tasks[0].Delete()
	if err != nil {
		t.Fatalf("Failed bury task %s", err)
	} else if !tasks[0].IsDone() {
		t.Errorf("Task status after delete is not done. Status = %s", tasks[0].Status())
	}

	//Delete by task ID
	err = q.Delete(tasks[1].Id())
	if err != nil {
		t.Fatalf("Failed bury task %s", err)
	} else if !tasks[0].IsDone() {
		t.Errorf("Task status after delete is not done. Status = %s", tasks[0].Status())
	}

	//Take
	for i := 0; i < 2; i++ {
		tasks[i], err = q.TakeTimeout(2 * time.Second)
		if err != nil {
			t.Errorf("Failed take from queue: %s", err)
		} else if tasks[i] != nil {
			t.Errorf("Task is not nil after take. Task is %d", tasks[i].Id())
		}
	}
}

func TestFifoQueue_Release(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatal("Task is nil after take")
	}

	//Release
	err = task.Release()
	if err != nil {
		t.Fatalf("Failed release task %s", err)
	}

	if !task.IsReady() {
		t.Fatalf("Task status is not ready, but %s", task.Status())
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatal("Task is nil after take")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestQueue_ReleaseAll(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatal("Task is nil after take")
	}

	//ReleaseAll
	err = q.ReleaseAll()
	if err != nil {
		t.Fatalf("Failed release task %s", err)
	}

	task, err = q.Peek(task.Id())
	if err != nil {
		t.Fatalf("Failed to peek task %s", err)
	}
	if !task.IsReady() {
		t.Fatalf("Task status is not ready, but %s", task.Status())
	}

	//Take
	task, err = q.Take()
	if err != nil {
		t.Fatalf("Failed take from queue: %s", err)
	} else if task == nil {
		t.Fatal("Task is nil after take")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after take not equal with example. %s != %s", task.Data(), putData)
		}

		if !task.IsTaken() {
			t.Errorf("Task status after take is not taken. Status = %s", task.Status())
		}

		err = task.Ack()
		if err != nil {
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestTtlQueue(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := createQueue(t, conn, name, cfg)
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.Put(putData)
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	time.Sleep(5 * time.Second)

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err)
	} else if task != nil {
		t.Errorf("Task is not nil after sleep")
	}
}

func TestTtlQueue_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := createQueue(t, conn, name, cfg)
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.PutWithOpts(putData, queue.Opts{Ttl: 10 * time.Second})
	if err != nil {
		t.Fatalf("Failed put to queue: %s", err)
	} else if err == nil && task == nil {
		t.Fatalf("Task is nil after put")
	} else {
		if task.Data() != putData {
			t.Errorf("Task data after put not equal with example. %s != %s", task.Data(), putData)
		}
	}

	time.Sleep(5 * time.Second)

	//Take
	task, err = q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Errorf("Failed take from queue: %s", err)
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
			t.Errorf("Failed ack %s", err)
		} else if !task.IsDone() {
			t.Errorf("Task status after take is not done. Status = %s", task.Status())
		}
	}
}

func TestUtube_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	name := "test_utube"
	cfg := queue.Cfg{
		Temporary:   true,
		Kind:        queue.UTUBE,
		IfNotExists: true,
	}
	q := createQueue(t, conn, name, cfg)
	defer dropQueue(t, q)

	data1 := &customData{"test-data-0"}
	_, err := q.PutWithOpts(data1, queue.Opts{Utube: "test-utube-consumer-key"})
	if err != nil {
		t.Fatalf("Failed put task to queue: %s", err)
	}
	data2 := &customData{"test-data-1"}
	_, err = q.PutWithOpts(data2, queue.Opts{Utube: "test-utube-consumer-key"})
	if err != nil {
		t.Fatalf("Failed put task to queue: %s", err)
	}

	errChan := make(chan struct{})
	go func() {
		t1, err := q.TakeTimeout(2 * time.Second)
		if err != nil {
			t.Errorf("Failed to take task from utube: %s", err)
			errChan <- struct{}{}
			return
		}

		time.Sleep(2 * time.Second)
		if err := t1.Ack(); err != nil {
			t.Errorf("Failed to ack task: %s", err)
			errChan <- struct{}{}
			return
		}
		close(errChan)
	}()

	time.Sleep(100 * time.Millisecond)
	// the queue should be blocked for ~2 seconds
	start := time.Now()
	t2, err := q.TakeTimeout(2 * time.Second)
	if err != nil {
		t.Fatalf("Failed to take task from utube: %s", err)
	}
	if err := t2.Ack(); err != nil {
		t.Fatalf("Failed to ack task: %s", err)
	}
	end := time.Now()
	if _, ok := <-errChan; ok {
		t.Fatalf("One of tasks failed")
	}
	if math.Abs(float64(end.Sub(start)-2*time.Second)) > float64(200*time.Millisecond) {
		t.Fatalf("Blocking time is less than expected: actual = %.2fs, expected = 1s", end.Sub(start).Seconds())
	}
}

func TestTask_Touch(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	tests := []struct {
		name string
		cfg  queue.Cfg
		ok   bool
	}{
		{"test_queue",
			queue.Cfg{
				Temporary: true,
				Kind:      queue.FIFO,
			},
			false,
		},
		{"test_queue_ttl",
			queue.Cfg{
				Temporary: true,
				Kind:      queue.FIFO_TTL,
				Opts:      queue.Opts{Ttl: 5 * time.Second},
			},
			true,
		},
		{"test_utube",
			queue.Cfg{
				Temporary: true,
				Kind:      queue.UTUBE,
			},
			false,
		},
		{"test_utube_ttl",
			queue.Cfg{
				Temporary: true,
				Kind:      queue.UTUBE_TTL,
				Opts:      queue.Opts{Ttl: 5 * time.Second},
			},
			true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var task *queue.Task

			q := createQueue(t, conn, tc.name, tc.cfg)
			defer func() {
				if task != nil {
					if err := task.Ack(); err != nil {
						t.Fatalf("Failed to Ack: %s", err)
					}
				}
				dropQueue(t, q)
			}()

			putData := "put_data"
			_, err := q.PutWithOpts(putData,
				queue.Opts{
					Ttl:   10 * time.Second,
					Utube: "test_utube",
				})
			if err != nil {
				t.Fatalf("Failed put a task: %s", err)
			}

			task, err = q.TakeTimeout(2 * time.Second)
			if err != nil {
				t.Fatalf("Failed to take task from utube: %s", err)
			}

			err = task.Touch(1 * time.Second)
			if tc.ok && err != nil {
				t.Fatalf("Failed to touch: %s", err)
			} else if !tc.ok && err == nil {
				t.Fatalf("Unexpected success")
			}
		})
	}
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		InitScript:   "testdata/config.lua",
		Listen:       server,
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
		RetryTimeout: 500 * time.Millisecond,
	})

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	defer test_helpers.StopTarantoolWithCleanup(inst)

	poolOpts := test_helpers.StartOpts{
		InitScript:   "testdata/pool.lua",
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    3 * time.Second, // replication_timeout * 3
		ConnectRetry: -1,
	}
	instances, err = test_helpers.StartTarantoolInstances(serversPool, nil, poolOpts)

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool pool: %s", err)
	}

	defer test_helpers.StopTarantoolInstances(instances)

	roles := []bool{false, true}
	connOpts := Opts{
		Timeout: 500 * time.Millisecond,
		User:    "test",
		Pass:    "test",
	}
	err = test_helpers.SetClusterRO(serversPool, connOpts, roles)

	if err != nil {
		log.Fatalf("Failed to set roles in tarantool pool: %s", err)
	}
	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
