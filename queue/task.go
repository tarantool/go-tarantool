package queue

import (
	"fmt"

	msgpack "gopkg.in/vmihailenco/msgpack.v2"
)

// Task represents a task from tarantool queue's tube
type Task struct {
	id     uint64
	status string
	data   interface{}
	q      *queue
}

func (t *Task) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var l int
	if l, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if l < 3 {
		return fmt.Errorf("array len doesn't match: %d", l)
	}
	if t.id, err = d.DecodeUint64(); err != nil {
		return err
	}
	if t.status, err = d.DecodeString(); err != nil {
		return err
	}
	if t.data != nil {
		if err = d.Decode(t.data); err != nil {
			return fmt.Errorf("fffuuuu: %s", err)
		}
	} else {
		if t.data, err = d.DecodeInterface(); err != nil {
			return err
		}
	}
	return nil
}

// Id is a getter for task id
func (t *Task) Id() uint64 {
	return t.id
}

// Data is a getter for task data
func (t *Task) Data() interface{} {
	return t.data
}

// Status is a getter for task status
func (t *Task) Status() string {
	return t.status
}

// Ack signals about task completion
func (t *Task) Ack() error {
	return t.accept(t.q._ack(t.id))
}

// Delete task from queue
func (t *Task) Delete() error {
	return t.accept(t.q._delete(t.id))
}

// Bury signals that task task cannot be executed in the current circumstances,
// task becomes "buried" - ie neither completed, nor ready, so it could not be
// deleted or taken by other worker.
// To revert "burying" call queue.Kick(numberOfBurried).
func (t *Task) Bury() error {
	return t.accept(t.q._bury(t.id))
}

// Release returns task back in the queue without making it complete.
// In outher words, this worker failed to complete the task, and
// it, so other worker could try to do that again.
func (t *Task) Release() error {
	return t.accept(t.q._release(t.id, Opts{}))
}

// ReleaseCfg returns task to a queue and changes its configuration.
func (t *Task) ReleaseCfg(cfg Opts) error {
	return t.accept(t.q._release(t.id, cfg))
}

func (t *Task) accept(newStatus string, err error) error {
	if err == nil {
		t.status = newStatus
	}
	return err
}

// IsReady returns if task is ready
func (t *Task) IsReady() bool {
	return t.status == READY
}

// IsTaken returns if task is taken
func (t *Task) IsTaken() bool {
	return t.status == TAKEN
}

// IsDone returns if task is done
func (t *Task) IsDone() bool {
	return t.status == DONE
}

// IsBurred returns if task is buried
func (t *Task) IsBuried() bool {
	return t.status == BURIED
}

// IsDelayed returns if task is delayed
func (t *Task) IsDelayed() bool {
	return t.status == DELAYED
}
