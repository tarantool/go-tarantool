package tarantool

import (
	"errors"
	"io"
	"sync"
	"time"
)

// Future is an interface that handle asynchronous request.
type Future interface {
	Get() ([]interface{}, error)
	GetTyped(result interface{}) error
	GetResponse() (Response, error)
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

var errUnexpectedArgType = errors.New("get other type than expected")

// NewFuture returns pair of new example of Future and error.
// Could accept error as args to set it Future.
// Could accept response as args and set it Future.
func NewFuture(req Request, args ...interface{}) (Future, error) {
	fut := newFuture(req)
	switch len(args) {
	case 0:
	case 1:
		if err, ok := args[0].(error); ok {
			fut.setError(err)
		} else {
			return nil, errUnexpectedArgType
		}
	case 2:
		header, ok := args[0].(Header)
		if !ok {
			return nil, errUnexpectedArgType
		}
		body, ok := args[1].(io.Reader)
		if !ok {
			return nil, errUnexpectedArgType
		}
		if err := fut.setResponse(header, body); err != nil {
			return nil, err
		}
	default:
		return nil, errUnexpectedArgType
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
