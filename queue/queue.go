// Package with implementation of methods for work with a Tarantool's queue
// implementations.
//
// Since: 1.5.
//
// # See also
//
// * Tarantool queue module https://github.com/tarantool/queue
package queue

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/ice-blockchain/go-tarantool"
)

// Queue is a handle to Tarantool queue's tube.
type Queue interface {
	// Set queue settings.
	Cfg(opts CfgOpts) error
	// Exists checks tube for existence.
	// Note: it uses Eval, so user needs 'execute universe' privilege.
	Exists() (bool, error)
	// Identify to a shared session.
	// In the queue the session has a unique UUID and many connections may
	// share one logical session. Also, the consumer can reconnect to the
	// existing session during the ttr time.
	// To get the UUID of the current session, call the Queue.Identify(nil).
	Identify(u *uuid.UUID) (uuid.UUID, error)
	// Create creates new tube with configuration.
	// Note: it uses Eval, so user needs 'execute universe' privilege
	// Note: you'd better not use this function in your application, cause it is
	// administrative task to create or delete queue.
	Create(cfg Cfg) error
	// Drop destroys tube.
	// Note: you'd better not use this function in your application, cause it is
	// administrative task to create or delete queue.
	Drop() error
	// ReleaseAll forcibly returns all taken tasks to a ready state.
	ReleaseAll() error
	// Put creates new task in a tube.
	Put(data interface{}) (*Task, error)
	// PutWithOpts creates new task with options different from tube's defaults.
	PutWithOpts(data interface{}, cfg Opts) (*Task, error)
	// Take takes 'ready' task from a tube and marks it as 'in progress'.
	// Note: if connection has a request Timeout, then 0.9 * connection.Timeout is
	// used as a timeout.
	// If you use a connection timeout and we can not take task from queue in
	// a time equal to the connection timeout after calling `Take` then we
	// return an error.
	Take() (*Task, error)
	// TakeTimeout takes 'ready' task from a tube and marks it as "in progress",
	// or it is timeouted after "timeout" period.
	// Note: if connection has a request Timeout, and conn.Timeout * 0.9 < timeout
	// then timeout = conn.Timeout*0.9.
	// If you use connection timeout and call `TakeTimeout` with parameter
	// greater than the connection timeout then parameter reduced to it.
	TakeTimeout(timeout time.Duration) (*Task, error)
	// TakeTyped takes 'ready' task from a tube and marks it as 'in progress'
	// Note: if connection has a request Timeout, then 0.9 * connection.Timeout is
	// used as a timeout.
	// Data will be unpacked to result.
	TakeTyped(interface{}) (*Task, error)
	// TakeTypedTimeout takes 'ready' task from a tube and marks it as "in progress",
	// or it is timeouted after "timeout" period.
	// Note: if connection has a request Timeout, and conn.Timeout * 0.9 < timeout
	// then timeout = conn.Timeout*0.9.
	// Data will be unpacked to result.
	TakeTypedTimeout(timeout time.Duration, result interface{}) (*Task, error)
	// Peek returns task by its id.
	Peek(taskId uint64) (*Task, error)
	// Kick reverts effect of Task.Bury() for count tasks.
	Kick(count uint64) (uint64, error)
	// Delete the task identified by its id.
	Delete(taskId uint64) error
	// State returns a current queue state.
	State() (State, error)
	// Statistic returns some statistic about queue.
	Statistic() (interface{}, error)
}

type queue struct {
	name string
	conn tarantool.Connector
	cmds cmd
}

type cmd struct {
	put        string
	take       string
	drop       string
	peek       string
	touch      string
	ack        string
	delete     string
	bury       string
	kick       string
	release    string
	releaseAll string
	cfg        string
	identify   string
	state      string
	statistics string
}

type Cfg struct {
	Temporary   bool // If true, the contents do not persist on disk.
	IfNotExists bool // If true, no error will be returned if the tube already exists.
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

// CfgOpts is argument type for the Queue.Cfg() call.
type CfgOpts struct {
	// Enable replication mode. Must be true if the queue is used in master and
	// replica mode. With replication mode enabled, the potential loss of
	// performance can be ~20% compared to single mode. Default value is false.
	InReplicaset bool
	// Time to release in seconds. The time after which, if there is no active
	// connection in the session, it will be released with all its tasks.
	Ttr time.Duration
}

func (opts CfgOpts) toMap() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["in_replicaset"] = opts.InReplicaset
	if opts.Ttr != 0 {
		ret["ttr"] = opts.Ttr
	}
	return ret
}

type Opts struct {
	Pri   int           // Task priorities.
	Ttl   time.Duration // Task time to live.
	Ttr   time.Duration // Task time to execute.
	Delay time.Duration // Delayed execution.
	Utube string
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

	if opts.Utube != "" {
		ret["utube"] = opts.Utube
	}

	return ret
}

// New creates a queue handle.
func New(conn tarantool.Connector, name string) Queue {
	q := &queue{
		name: name,
		conn: conn,
	}
	makeCmd(q)
	return q
}

// Create creates a new queue with config.
func (q *queue) Create(cfg Cfg) error {
	cmd := "local name, type, cfg = ... ; queue.create_tube(name, type, cfg)"
	_, err := q.conn.Eval(cmd, []interface{}{q.name, cfg.getType(), cfg.toMap()})
	return err
}

// Set queue settings.
func (q *queue) Cfg(opts CfgOpts) error {
	_, err := q.conn.Call17(q.cmds.cfg, []interface{}{opts.toMap()})
	return err
}

// Exists checks existence of a tube.
func (q *queue) Exists() (bool, error) {
	cmd := "local name = ... ; return queue.tube[name] ~= nil"
	resp, err := q.conn.Eval(cmd, []string{q.name})
	if err != nil {
		return false, err
	}

	exist := len(resp.Data) != 0 && resp.Data[0].(bool)
	return exist, nil
}

// Identify to a shared session.
// In the queue the session has a unique UUID and many connections may share
// one logical session. Also, the consumer can reconnect to the existing
// session during the ttr time.
// To get the UUID of the current session, call the Queue.Identify(nil).
func (q *queue) Identify(u *uuid.UUID) (uuid.UUID, error) {
	// Unfortunately we can't use go-tarantool/uuid here:
	// https://github.com/tarantool/queue/issues/182
	var args []interface{}
	if u == nil {
		args = []interface{}{}
	} else {
		if bytes, err := u.MarshalBinary(); err != nil {
			return uuid.UUID{}, err
		} else {
			args = []interface{}{bytes}
		}
	}

	if resp, err := q.conn.Call17(q.cmds.identify, args); err == nil {
		if us, ok := resp.Data[0].(string); ok {
			return uuid.FromBytes([]byte(us))
		} else {
			return uuid.UUID{}, fmt.Errorf("unexpected response: %v", resp.Data)
		}
	} else {
		return uuid.UUID{}, err
	}
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
	if err := q.conn.Call17Typed(q.cmds.put, params, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

// The take request searches for a task in the queue.
func (q *queue) Take() (*Task, error) {
	var params interface{}
	timeout := q.conn.ConfiguredTimeout()
	if timeout > 0 {
		params = (timeout * 9 / 10).Seconds()
	}
	return q.take(params)
}

// The take request searches for a task in the queue. Waits until a task
// becomes ready or the timeout expires.
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
	timeout := q.conn.ConfiguredTimeout()
	if timeout > 0 {
		params = (timeout * 9 / 10).Seconds()
	}
	return q.take(params, result)
}

// The take request searches for a task in the queue. Waits until a task
// becomes ready or the timeout expires.
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
	if err := q.conn.Call17Typed(q.cmds.take, []interface{}{params}, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

// Drop queue.
func (q *queue) Drop() error {
	_, err := q.conn.Call17(q.cmds.drop, []interface{}{})
	return err
}

// ReleaseAll forcibly returns all taken tasks to a ready state.
func (q *queue) ReleaseAll() error {
	_, err := q.conn.Call17(q.cmds.releaseAll, []interface{}{})
	return err
}

// Look at a task without changing its state.
func (q *queue) Peek(taskId uint64) (*Task, error) {
	qd := queueData{q: q}
	if err := q.conn.Call17Typed(q.cmds.peek, []interface{}{taskId}, &qd); err != nil {
		return nil, err
	}
	return qd.task, nil
}

func (q *queue) _touch(taskId uint64, increment time.Duration) (string, error) {
	return q.produce(q.cmds.touch, taskId, increment.Seconds())
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
	if err := q.conn.Call17Typed(cmd, params, &qd); err != nil || qd.task == nil {
		return "", err
	}
	return qd.task.status, nil
}

type kickResult struct {
	id uint64
}

func (r *kickResult) DecodeMsgpack(d *decoder) (err error) {
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l > 1 {
		return fmt.Errorf("array len doesn't match for queue kick data: %d", l)
	}
	r.id, err = d.DecodeUint64()
	return
}

// Reverse the effect of a bury request on one or more tasks.
func (q *queue) Kick(count uint64) (uint64, error) {
	var r kickResult
	err := q.conn.Call17Typed(q.cmds.kick, []interface{}{count}, &r)
	return r.id, err
}

// Delete the task identified by its id.
func (q *queue) Delete(taskId uint64) error {
	_, err := q._delete(taskId)
	return err
}

// State returns a current queue state.
func (q *queue) State() (State, error) {
	resp, err := q.conn.Call17(q.cmds.state, []interface{}{})
	if err != nil {
		return UnknownState, err
	}

	if respState, ok := resp.Data[0].(string); ok {
		if state, ok := strToState[respState]; ok {
			return state, nil
		}
		return UnknownState, fmt.Errorf("unknown state: %v", resp.Data[0])
	}
	return UnknownState, fmt.Errorf("unexpected response: %v", resp.Data)
}

// Return the number of tasks in a queue broken down by task_state, and the
// number of requests broken down by the type of request.
func (q *queue) Statistic() (interface{}, error) {
	resp, err := q.conn.Call17(q.cmds.statistics, []interface{}{q.name})
	if err != nil {
		return nil, err
	}

	if len(resp.Data) != 0 {
		return resp.Data[0], nil
	}

	return nil, nil
}

func makeCmd(q *queue) {
	q.cmds = cmd{
		put:        "queue.tube." + q.name + ":put",
		take:       "queue.tube." + q.name + ":take",
		drop:       "queue.tube." + q.name + ":drop",
		peek:       "queue.tube." + q.name + ":peek",
		touch:      "queue.tube." + q.name + ":touch",
		ack:        "queue.tube." + q.name + ":ack",
		delete:     "queue.tube." + q.name + ":delete",
		bury:       "queue.tube." + q.name + ":bury",
		kick:       "queue.tube." + q.name + ":kick",
		release:    "queue.tube." + q.name + ":release",
		releaseAll: "queue.tube." + q.name + ":release_all",
		cfg:        "queue.cfg",
		identify:   "queue.identify",
		state:      "queue.state",
		statistics: "queue.statistics",
	}
}

type queueData struct {
	q      *queue
	task   *Task
	result interface{}
}

func (qd *queueData) DecodeMsgpack(d *decoder) error {
	var err error
	var l int
	if l, err = d.DecodeArrayLen(); err != nil {
		return err
	}
	if l > 1 {
		return fmt.Errorf("array len doesn't match for queue data: %d", l)
	}
	if l == 0 {
		return nil
	}

	qd.task = &Task{data: qd.result, q: qd.q}
	if err = d.Decode(&qd.task); err != nil {
		return err
	}

	if qd.task.Data() == nil {
		// It may happen if the decoder has a code.Nil value inside. As a
		// result, the task will not be decoded.
		qd.task = nil
	}
	return nil
}
