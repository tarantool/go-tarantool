package queue_test

import (
	"fmt"
	"log"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	. "github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/queue"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

const (
	user = "test"
	pass = "test"
)

var servers = []string{"127.0.0.1:3014", "127.0.0.1:3015"}
var server = "127.0.0.1:3013"

var dialer = NetDialer{
	Address:  server,
	User:     user,
	Password: pass,
}

var opts = Opts{
	Timeout: 5 * time.Second,
	// Concurrency: 32,
	// RateLimit: 4*1024,
}

func createQueue(t *testing.T, conn *Connection, name string, cfg queue.Cfg) queue.Queue {
	t.Helper()

	q := queue.New(conn, name)
	require.NoError(t, q.Create(cfg), "Failed to create queue")

	return q
}

func dropQueue(t *testing.T, q queue.Queue) {
	t.Helper()

	require.NoError(t, q.Drop(), "Failed to drop queue")
}

// ///////QUEUE/////////

func TestFifoQueue(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)
}

func TestQueue_Cfg(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	require.NoError(t, q.Cfg(queue.CfgOpts{
		InReplicaset: false,
		Ttr:          5 * time.Second,
	}), "Unexpected q.Cfg() error")
}

func TestQueue_Identify(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	uuid, err := q.Identify(nil)
	require.NoError(t, err, "Failed to identify")
	cpy := uuid

	uuid, err = q.Identify(&cpy)
	require.NoError(t, err, "Failed to identify with uuid %s", cpy)
	require.Equal(t, cpy.String(), uuid.String(), "Unequal UUIDs after re-identify")
}

func TestQueue_ReIdentify(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	name := "test_queue"
	cfg := queue.Cfg{
		Temporary: true,
		Kind:      queue.FIFO_TTL,
		Opts:      queue.Opts{Ttl: 5 * time.Second},
	}
	q := createQueue(t, conn, name, cfg)
	require.NoError(t, q.Cfg(queue.CfgOpts{InReplicaset: false, Ttr: 5 * time.Second}), "Cfg failed")
	defer func() {
		dropQueue(t, q)
	}()

	uuid, err := q.Identify(nil)
	require.NoError(t, err, "Failed to identify")
	newuuid, err := q.Identify(&uuid)
	require.NoError(t, err, "Failed to identify")
	require.Equal(t, uuid.String(), newuuid.String(), "Unequal UUIDs after re-identify")
	// Put.
	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Take.
	task, err = q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")

	_ = conn.Close()
	conn = nil

	conn = test_helpers.ConnectWithValidation(t, dialer, opts)
	q = queue.New(conn, name)

	// Identify in another connection.
	newuuid, err = q.Identify(&uuid)
	require.NoError(t, err, "Failed to identify")
	require.Equal(t, uuid.String(), newuuid.String(), "Unequal UUIDs after re-identify")

	// Peek in another connection.
	task, err = q.Peek(task.Id())
	require.NoError(t, err, "Failed peek from queue")
	require.NotNil(t, task, "Task is nil after peek")

	// Ack in another connection.
	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestQueue_State(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	state, err := q.State()
	require.NoError(t, err, "Failed to get queue state")
	require.True(t, state == queue.InitState || state == queue.RunningState,
		"Unexpected state: %d", state)
}

func TestFifoQueue_GetExist_Statistic(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	ok, err := q.Exists()
	require.NoError(t, err, "Failed to get exist queue")
	require.True(t, ok, "Queue is not found")

	putData := "put_data"
	_, err = q.Put(putData)
	require.NoError(t, err, "Failed to put queue")

	stat, err := q.Statistic()
	require.NoError(t, err, "Failed to get statistic queue")
	assert.NotNil(t, stat, "Statistic is nil")
}

func TestFifoQueue_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")
}

func TestFifoQueue_Take(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Take.
	task, err = q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")
	assert.Equal(t, putData, task.Data(), "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

type customData struct {
	customField string
}

func (c *customData) DecodeMsgpack(d *msgpack.Decoder) error {
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

func (c *customData) EncodeMsgpack(e *msgpack.Encoder) error {
	if err := e.EncodeArrayLen(1); err != nil {
		return err
	}
	if err := e.EncodeString(c.customField); err != nil {
		return err
	}
	return nil
}

func TestFifoQueue_TakeTyped(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	putData := &customData{customField: "put_data"}
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	typedData, ok := task.Data().(*customData)
	require.True(t, ok, "Task data after put has different type. %#v != %#v", task.Data(), putData)
	assert.Equal(t, *putData, *typedData, "Task data after put not equal with example")

	// Take.
	takeData := &customData{}
	task, err = q.TakeTypedTimeout(2*time.Second, takeData)
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")
	typedData, ok = task.Data().(*customData)
	require.True(t, ok, "Task data after take has different type. %#v != %#v", task.Data(), putData)
	assert.Equal(t, *putData, *typedData, "Task data after take not equal with example")
	assert.Equal(t, *putData, *takeData, "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestFifoQueue_Peek(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Peek.
	task, err = q.Peek(task.Id())
	require.NoError(t, err, "Failed peek from queue")
	require.NotNil(t, task, "Task is nil after peek")
	assert.Equal(t, putData, task.Data(), "Task data after peek not equal with example")
	assert.True(t, task.IsReady(), "Task status after peek is not ready. Status = %s", task.Status())
}

func TestFifoQueue_Bury_Kick(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Bury.
	err = task.Bury()
	require.NoError(t, err, "Failed bury task")
	assert.True(t, task.IsBuried(), "Task status after bury is not buried. Status = %s", task.Status())

	// Kick.
	count, err := q.Kick(1)
	require.NoError(t, err, "Failed kick task")
	require.Equal(t, uint64(1), count, "Kick result != 1")

	// Take.
	task, err = q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")
	assert.Equal(t, putData, task.Data(), "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestFifoQueue_Delete(t *testing.T) {
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	// Put.
	var putData = "put_data"
	var tasks = [2]*queue.Task{}

	for i := 0; i < 2; i++ {
		tasks[i], err = q.Put(putData)
		require.NoError(t, err, "Failed put to queue")
		require.NotNil(t, tasks[i], "Task is nil after put")
		assert.Equal(t, putData, tasks[i].Data(), "Task data after put not equal with example")
	}

	// Delete by task method.
	err = tasks[0].Delete()
	require.NoError(t, err, "Failed delete task")
	assert.True(t, tasks[0].IsDone(),
		"Task status after delete is not done. Status = %s", tasks[0].Status())

	// Delete by task ID.
	err = q.Delete(tasks[1].Id())
	require.NoError(t, err, "Failed delete task")

	// Take.
	for i := 0; i < 2; i++ {
		tasks[i], err = q.TakeTimeout(2 * time.Second)
		require.NoError(t, err, "Failed take from queue")
		assert.Nil(t, tasks[i], "Task is not nil after take")
	}
}

func TestFifoQueue_Release(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Take.
	task, err = q.Take()
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")

	// Release.
	require.NoError(t, task.Release(), "Failed release task")
	require.True(t, task.IsReady(), "Task status is not ready, but %s", task.Status())

	// Take.
	task, err = q.Take()
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")
	assert.Equal(t, putData, task.Data(), "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestQueue_ReleaseAll(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

	name := "test_queue"
	q := createQueue(t, conn, name, queue.Cfg{Temporary: true, Kind: queue.FIFO})
	defer dropQueue(t, q)

	putData := "put_data"
	task, err := q.Put(putData)
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	// Take.
	task, err = q.Take()
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")

	// ReleaseAll.
	require.NoError(t, q.ReleaseAll(), "Failed release task")

	task, err = q.Peek(task.Id())
	require.NoError(t, err, "Failed to peek task")
	require.True(t, task.IsReady(), "Task status is not ready, but %s", task.Status())

	// Take.
	task, err = q.Take()
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after take")
	assert.Equal(t, putData, task.Data(), "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestTtlQueue(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	time.Sleep(10 * time.Second)

	// Take.
	task, err = q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed take from queue")
	assert.Nil(t, task, "Task is not nil after sleep")
}

func TestTtlQueue_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
	require.NoError(t, err, "Failed put to queue")
	require.NotNil(t, task, "Task is nil after put")
	assert.Equal(t, putData, task.Data(), "Task data after put not equal with example")

	time.Sleep(2 * time.Second)

	// Take.
	task, err = q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed take from queue")
	require.NotNil(t, task, "Task is nil after sleep")
	assert.Equal(t, putData, task.Data(), "Task data after take not equal with example")
	assert.True(t, task.IsTaken(), "Task status after take is not taken. Status = %s", task.Status())

	err = task.Ack()
	require.NoError(t, err, "Failed ack")
	assert.True(t, task.IsDone(), "Task status after take is not done. Status = %s", task.Status())
}

func TestUtube_Put(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
	require.NoError(t, err, "Failed put task to queue")
	data2 := &customData{"test-data-1"}
	_, err = q.PutWithOpts(data2, queue.Opts{Utube: "test-utube-consumer-key"})
	require.NoError(t, err, "Failed put task to queue")

	errChan := make(chan error, 2)
	go func() {
		t1, gerr := q.TakeTimeout(2 * time.Second)
		if gerr != nil {
			errChan <- fmt.Errorf("failed to take task from utube: %w", gerr)
			return
		}

		time.Sleep(2 * time.Second)
		if gerr := t1.Ack(); gerr != nil {
			errChan <- fmt.Errorf("failed to ack task: %w", gerr)
			return
		}
		close(errChan)
	}()

	time.Sleep(500 * time.Millisecond)
	// The queue should be blocked for ~2 seconds.
	defer func() {
		select {
		case gerr := <-errChan:
			require.NoError(t, gerr)
		default:
		}
	}()
	start := time.Now()
	t2, err := q.TakeTimeout(2 * time.Second)
	require.NoError(t, err, "Failed to take task from utube")
	require.NotNil(t, t2, "Got nil task")

	err = t2.Ack()
	require.NoError(t, err, "Failed to ack task")
	end := time.Now()

	timeSpent := math.Abs(float64(end.Sub(start) - 2*time.Second))

	assert.LessOrEqual(t, timeSpent, float64(700*time.Millisecond),
		"Blocking time is less than expected: actual = %.2fs, expected = 1s",
		end.Sub(start).Seconds())
}

func TestTask_Touch(t *testing.T) {
	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer func() { _ = conn.Close() }()

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
					require.NoError(t, task.Ack(), "Failed to Ack")
				}
				dropQueue(t, q)
			}()

			putData := "put_data"
			_, err := q.PutWithOpts(putData,
				queue.Opts{
					Ttl:   10 * time.Second,
					Utube: "test_utube",
				})
			require.NoError(t, err, "Failed put a task")

			task, err = q.TakeTimeout(2 * time.Second)
			require.NoError(t, err, "Failed to take task from utube")

			err = task.Touch(1 * time.Second)
			if tc.ok {
				require.NoError(t, err, "Failed to touch")
			} else {
				require.Error(t, err, "Unexpected success")
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
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	defer test_helpers.StopTarantoolWithCleanup(inst)

	poolInstsOpts := make([]test_helpers.StartOpts, 0, len(servers))
	for _, serv := range servers {
		poolInstsOpts = append(poolInstsOpts, test_helpers.StartOpts{
			Listen: serv,
			Dialer: NetDialer{
				Address:  serv,
				User:     user,
				Password: pass,
			},
			InitScript:   "testdata/pool.lua",
			WaitStart:    3 * time.Second, // replication_timeout * 3
			ConnectRetry: -1,
		})
	}

	instances, err := test_helpers.StartTarantoolInstances(poolInstsOpts)

	if err != nil {
		log.Printf("Failed to prepare test tarantool pool: %s", err)
		return 1
	}

	defer test_helpers.StopTarantoolInstances(instances)

	for i := 0; i < 10; i++ {
		// We need to skip bootstrap errors and to make sure that cluster is
		// configured.
		roles := []bool{false, true}
		connOpts := Opts{
			Timeout: 500 * time.Millisecond,
		}
		dialers := make([]Dialer, 0, len(servers))
		for _, serv := range servers {
			dialers = append(dialers, NetDialer{
				Address:  serv,
				User:     user,
				Password: pass,
			})
		}

		ctx, cancel := test_helpers.GetPoolConnectContext()
		err = test_helpers.SetClusterRO(ctx, dialers, connOpts, roles)
		cancel()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		log.Printf("Failed to set roles in tarantool pool: %s", err)
		return 1
	}
	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
