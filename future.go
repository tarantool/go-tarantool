package tarantool

import (
	"io"
	"sync"
	"time"
)

// Future is a handle for asynchronous request.
type Future struct {
	requestId uint32
	req       Request
	next      *Future
	timeout   time.Duration
	mutex     sync.Mutex
	resp      Response
	err       error
	ready     chan struct{}
	done      chan struct{}
}

func (fut *Future) wait() {
	if fut.done == nil {
		return
	}
	<-fut.done
}

func (fut *Future) isDone() bool {
	if fut.done == nil {
		return true
	}
	select {
	case <-fut.done:
		return true
	default:
		return false
	}
}

// NewFuture creates a new empty Future for a given Request.
func NewFuture(req Request) (fut *Future) {
	fut = &Future{}
	fut.ready = make(chan struct{}, 1000000000)
	fut.done = make(chan struct{})
	fut.req = req
	return fut
}

// SetResponse sets a response for the future and finishes the future.
func (fut *Future) SetResponse(header Header, body io.Reader) error {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.isDone() {
		return nil
	}

	resp, err := fut.req.Response(header, body)
	if err != nil {
		return err
	}
	fut.resp = resp

	close(fut.ready)
	close(fut.done)
	return nil
}

// SetError sets an error for the future and finishes the future.
func (fut *Future) SetError(err error) {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.isDone() {
		return
	}
	fut.err = err

	close(fut.ready)
	close(fut.done)
}

// GetResponse waits for Future to be filled and returns Response and error.
//
// Note: Response could be equal to nil if ClientError is returned in error.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *Future) GetResponse() (Response, error) {
	fut.wait()
	return fut.resp, fut.err
}

// Get waits for Future to be filled and returns the data of the Response and error.
//
// The data will be []interface{}, so if you want more performance, use GetTyped method.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *Future) Get() ([]interface{}, error) {
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
func (fut *Future) GetTyped(result interface{}) error {
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
func (fut *Future) WaitChan() <-chan struct{} {
	if fut.done == nil {
		return closedChan
	}
	return fut.done
}
