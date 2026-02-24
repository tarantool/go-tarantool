package tarantool

import (
	"io"
	"sync"
	"time"
)

// Future is an interface that handle asynchronous request.
type Future interface {
	Get() ([]interface{}, error)
	GetTyped(result interface{}) error
	GetResponse() (Response, error)
	Release()
	WaitChan() <-chan struct{}
}

// future is inner implementation of Future interface.
type future struct {
	requestId uint32
	req       Request
	next      *future
	timeout   time.Duration
	mutex     sync.Mutex
	resp      Response
	err       error
	cond      sync.Cond
	finished  bool
	done      chan struct{}
}

var _ = Future(&future{})

func (fut *future) wait() {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	for !fut.finished {
		fut.cond.Wait()
	}
}

func (fut *future) finish() {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	fut.finished = true

	if fut.done != nil {
		close(fut.done)
	}

	fut.cond.Broadcast()
}

// NewFutureWithErr returns Future with given error.
func NewFutureWithErr(req Request, err error) Future {
	fut := newFuture(req)
	fut.setError(err)
	return fut
}

// NewFutureWithResponse returns Future with given Response.
func NewFutureWithResponse(req Request, header Header, body io.Reader) (Future, error) {
	fut := newFuture(req)
	if err := fut.setResponse(header, body); err != nil {
		return nil, err
	}
	return fut, nil
}

// newFuture creates a new empty future for a given Request.
func newFuture(req Request) (fut *future) {
	fut = &future{}
	fut.done = nil
	fut.finished = false
	fut.cond.L = &fut.mutex
	fut.req = req
	return fut
}

func (fut *future) isFinished() bool {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	return fut.finished
}

// setResponse sets a response for the future and finishes the future.
func (fut *future) setResponse(header Header, body io.Reader) error {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.finished {
		return nil
	}

	resp, err := fut.req.Response(header, body)
	if err != nil {
		return err
	}
	fut.resp = resp

	fut.finished = true

	if fut.done != nil {
		close(fut.done)
	}

	fut.cond.Broadcast()

	return nil
}

// setError sets an error for the future and finishes the future.
func (fut *future) setError(err error) {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.finished {
		return
	}
	fut.err = err

	fut.finished = true

	if fut.done != nil {
		close(fut.done)
	}

	fut.cond.Broadcast()
}

// GetResponse waits for Future to be filled and returns Response and error.
//
// Note: Response could be equal to nil if ClientError is returned in error.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *future) GetResponse() (Response, error) {
	fut.wait()
	return fut.resp, fut.err
}

// Get waits for Future to be filled and returns the data of the Response and error.
//
// The data will be []interface{}, so if you want more performance, use GetTyped method.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *future) Get() ([]interface{}, error) {
	fut.wait()
	if fut.err != nil {
		return nil, fut.err
	}
	return fut.resp.Decode()
}

// GetTyped waits for Future and calls msgpack.Decoder.Decode(result) if no error happens.
// It is could be much faster than Get() function.
//
// Note: Tarantool usually returns array of tuples (except for Eval and Call17 actions).
func (fut *future) GetTyped(result interface{}) error {
	fut.wait()
	if fut.err != nil {
		return fut.err
	}
	return fut.resp.DecodeTyped(result)
}

var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}

// WaitChan returns channel which becomes closed when response arrived or error occurred.
func (fut *future) WaitChan() <-chan struct{} {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.finished {
		return closedChan
	}

	if fut.done == nil {
		fut.done = make(chan struct{})
	}

	return fut.done
}

// Release is freeing the Future resources.
// After this, using this Future becomes invalid.
func (fut *future) Release() {
	if fut.resp != nil {
		fut.resp.Release()
	}
}
