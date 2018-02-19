package queue

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

// Queue is a handle to tarantool queue's tube
type Queue interface {
	// Exists checks tube for existence
	// Note: it uses Eval, so user needs 'execute universe' privilege
	Exists() (bool, error)
	// Create creates new tube with configuration
	// Note: it uses Eval, so user needs 'execute universe' privilege
	// Note: you'd better not use this function in your application, cause it is
	// administrative task to create or delete queue.
	Create(cfg Cfg) error
	// Drop destroys tube.
	// Note: you'd better not use this function in your application, cause it is
	// administrative task to create or delete queue.
	Drop() error
	// Put creates new task in a tube
	Put(data interface{}) (*Task, error)
	// PutWithOpts creates new task with options different from tube's defaults
	PutWithOpts(data interface{}, cfg Opts) (*Task, error)
	// Take takes 'ready' task from a tube and marks it as 'in progress'
	// Note: if connection has a request Timeout, then 0.9 * connection.Timeout is
	// used as a timeout.
	Take() (*Task, error)
	// TakeWithTimout takes 'ready' task from a tube and marks it as "in progress",
	// or it is timeouted after "timeout" period.
	// Note: if connection has a request Timeout, and conn.Timeout * 0.9 < timeout
	// then timeout = conn.Timeout*0.9
	TakeTimeout(timeout time.Duration) (*Task, error)
	// Take takes 'ready' task from a tube and marks it as 'in progress'
	// Note: if connection has a request Timeout, then 0.9 * connection.Timeout is
	// used as a timeout.
	// Data will be unpacked to result
	TakeTyped(interface{}) (*Task, error)
	// TakeWithTimout takes 'ready' task from a tube and marks it as "in progress",
	// or it is timeouted after "timeout" period.
	// Note: if connection has a request Timeout, and conn.Timeout * 0.9 < timeout
	// then timeout = conn.Timeout*0.9
	// data will be unpacked to result
	TakeTypedTimeout(timeout time.Duration, result interface{}) (*Task, error)
	// Peek returns task by its id.
	Peek(taskId uint64) (*Task, error)
	// Kick reverts effect of Task.Bury() for `count` tasks.
	Kick(count uint64) (uint64, error)
	// Statistic returns some statistic about queue.
	Statistic() (interface{}, error)
}

type queue struct {
	name string
	conn *tarantool.Connection
	cmds cmd
}

type cmd struct {
	put        string
	take       string
	drop       string
	peek       string
	ack        string
	delete     string
	bury       string
	kick       string
	release    string
	statistics string
}

type Cfg struct {
	Temporary   bool // if true, the contents do not persist on disk
	IfNotExists bool // if true, no error will be returned if the tube already exists
	Kind        queueType
	Opts
}

func (cfg Cfg) toMap() map[string]interface{} {
	res := cfg.Opts.toMap()
	res["temporary"] = cfg.Temporary
	res["if_not_exists"] = cfg.IfNotExists
	return res
}

func (cfg Cfg) getType() string {
	kind := string(cfg.Kind)
	if kind == "" {
		kind = string(FIFO)
	}

	return kind
}

type Opts struct {
	Pri   int           // task priorities
	Ttl   time.Duration // task time to live
	Ttr   time.Duration // task time to execute
	Delay time.Duration // delayed execution
}

func (opts Opts) toMap() map[string]interface{} {
	ret := make(map[string]interface{})

	if opts.Ttl.Seconds() != 0 {
		ret["ttl"] = opts.Ttl.Seconds()
	}

	if opts.Ttr.Seconds() != 0 {
		ret["ttr"] = opts.Ttr.Seconds()
	}

	if opts.Delay.Seconds() != 0 {
		ret["delay"] = opts.Delay.Seconds()
	}

	if opts.Pri != 0 {
		ret["pri"] = opts.Pri
	}

	return ret
}

// New creates a queue handle
func New(conn *tarantool.Connection, name string) Queue {
	q := &queue{
		name: name,
		conn: conn,
	}
	makeCmd(q)
	return q
}

// Create creates a new queue with config
func (q *queue) Create(cfg Cfg) error {
	cmd := "local name, type, cfg = ... ; queue.create_tube(name, type, cfg)"
	_, err := q.conn.Eval(cmd, []interface{}{q.name, cfg.getType(), cfg.toMap()})
	return err
}

// Exists checks existance of a tube
func (q *queue) Exists() (bool, error) {
	cmd := "local name = ... ; return queue.tube[name] ~= null"
	resp, err := q.conn.Eval(cmd, []string{q.name})
	if err != nil {
		return false, err
	}

	exist := len(resp.Data) != 0 && resp.Data[0].(bool)
	return exist, nil
}

// Put data to queue. Returns task.
func (q *queue) Put(data interface{}) (*Task, error) {
	return q.put(data)
}

// Put data with options (ttl/ttr/pri/delay) to queue. Returns task.
func (q *queue) PutWithOpts(data interface{}, cfg Opts) (*Task, error) {
	return q.put(data, cfg.toMap())
}

func (q *queue) put(params ...interface{}) (*Task, error) {
	qd := queueData{
		result: params[0],
		q:      q,
	}
	if err := q.conn.CallTyped(q.cmds.put, params, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

// The take request searches for a task in the queue.
func (q *queue) Take() (*Task, error) {
	var params interface{}
	if q.conn.ConfiguredTimeout() > 0 {
		params = (q.conn.ConfiguredTimeout() * 9 / 10).Seconds()
	}
	return q.take(params)
}

// The take request searches for a task in the queue. Waits until a task becomes ready or the timeout expires.
func (q *queue) TakeTimeout(timeout time.Duration) (*Task, error) {
	t := q.conn.ConfiguredTimeout() * 9 / 10
	if t > 0 && timeout > t {
		timeout = t
	}
	return q.take(timeout.Seconds())
}

// The take request searches for a task in the queue.
func (q *queue) TakeTyped(result interface{}) (*Task, error) {
	var params interface{}
	if q.conn.ConfiguredTimeout() > 0 {
		params = (q.conn.ConfiguredTimeout() * 9 / 10).Seconds()
	}
	return q.take(params, result)
}

// The take request searches for a task in the queue. Waits until a task becomes ready or the timeout expires.
func (q *queue) TakeTypedTimeout(timeout time.Duration, result interface{}) (*Task, error) {
	t := q.conn.ConfiguredTimeout() * 9 / 10
	if t > 0 && timeout > t {
		timeout = t
	}
	return q.take(timeout.Seconds(), result)
}

func (q *queue) take(params interface{}, result ...interface{}) (*Task, error) {
	qd := queueData{q: q}
	if len(result) > 0 {
		qd.result = result[0]
	}
	if err := q.conn.CallTyped(q.cmds.take, []interface{}{params}, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

// Drop queue.
func (q *queue) Drop() error {
	_, err := q.conn.Call(q.cmds.drop, []interface{}{})
	return err
}

// Look at a task without changing its state.
func (q *queue) Peek(taskId uint64) (*Task, error) {
	qd := queueData{q: q}
	if err := q.conn.CallTyped(q.cmds.peek, []interface{}{taskId}, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

func (q *queue) _ack(taskId uint64) (string, error) {
	return q.produce(q.cmds.ack, taskId)
}

func (q *queue) _delete(taskId uint64) (string, error) {
	return q.produce(q.cmds.delete, taskId)
}

func (q *queue) _bury(taskId uint64) (string, error) {
	return q.produce(q.cmds.bury, taskId)
}

func (q *queue) _release(taskId uint64, cfg Opts) (string, error) {
	return q.produce(q.cmds.release, taskId, cfg.toMap())
}
func (q *queue) produce(cmd string, params ...interface{}) (string, error) {
	qd := queueData{q: q}
	if err := q.conn.CallTyped(cmd, params, &qd); err != nil || qd.task == nil {
		return "", err
	}
	return qd.task.status, nil
}

// Reverse the effect of a bury request on one or more tasks.
func (q *queue) Kick(count uint64) (uint64, error) {
	resp, err := q.conn.Call(q.cmds.kick, []interface{}{count})
	var id uint64
	if err == nil {
		id = resp.Data[0].([]interface{})[0].(uint64)
	}
	return id, err
}

// Return the number of tasks in a queue broken down by task_state, and the number of requests broken down by the type of request.
func (q *queue) Statistic() (interface{}, error) {
	resp, err := q.conn.Call(q.cmds.statistics, []interface{}{q.name})
	if err != nil {
		return nil, err
	}

	if len(resp.Data) != 0 {
		data, ok := resp.Data[0].([]interface{})
		if ok && len(data) != 0 {
			return data[0], nil
		}
	}

	return nil, nil
}

func makeCmd(q *queue) {
	q.cmds = cmd{
		put:        "queue.tube." + q.name + ":put",
		take:       "queue.tube." + q.name + ":take",
		drop:       "queue.tube." + q.name + ":drop",
		peek:       "queue.tube." + q.name + ":peek",
		ack:        "queue.tube." + q.name + ":ack",
		delete:     "queue.tube." + q.name + ":delete",
		bury:       "queue.tube." + q.name + ":bury",
		kick:       "queue.tube." + q.name + ":kick",
		release:    "queue.tube." + q.name + ":release",
		statistics: "queue.statistics",
	}
}

type queueData struct {
	q      *queue
	task   *Task
	result interface{}
}

func (qd *queueData) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l > 1 {
		return fmt.Errorf("array len doesn't match for queue data: %d", l)
	}
	if l == 0 {
		return nil
	}

	qd.task = &Task{data: qd.result, q: qd.q}
	d.Decode(&qd.task)
	return nil
}
