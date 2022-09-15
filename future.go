package tarantool

import (
	"sync"
	"time"
)

// Future is a handle for asynchronous request.
type Future struct {
	requestId uint32
	next      *Future
	timeout   time.Duration
	mutex     sync.Mutex
	pushes    []*Response
	resp      *Response
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

type asyncResponseIterator struct {
	fut     *Future
	timeout time.Duration
	resp    *Response
	err     error
	curPos  int
	done    bool
}

func (it *asyncResponseIterator) Next() bool {
	if it.done || it.err != nil {
		it.resp = nil
		return false
	}

	var last = false
	var exit = false
	for !exit {
		// We try to read at least once.
		it.fut.mutex.Lock()
		it.resp = it.nextResponse()
		it.err = it.fut.err
		last = it.resp == it.fut.resp
		it.fut.mutex.Unlock()

		if it.timeout == 0 || it.resp != nil || it.err != nil {
			break
		}

		select {
		case <-it.fut.ready:
		case <-time.After(it.timeout):
			exit = true
		}
	}

	if it.resp == nil {
		return false
	}

	if it.err = it.resp.decodeBody(); it.err != nil {
		it.resp = nil
		return false
	}

	if last {
		it.done = true
	} else {
		it.curPos += 1
	}

	return true
}

func (it *asyncResponseIterator) Value() *Response {
	return it.resp
}

func (it *asyncResponseIterator) Err() error {
	return it.err
}

func (it *asyncResponseIterator) WithTimeout(timeout time.Duration) TimeoutResponseIterator {
	it.timeout = timeout
	return it
}

func (it *asyncResponseIterator) nextResponse() (resp *Response) {
	fut := it.fut
	pushesLen := len(fut.pushes)

	if it.curPos < pushesLen {
		resp = fut.pushes[it.curPos]
	} else if it.curPos == pushesLen {
		resp = fut.resp
	}

	return resp
}

// NewFuture creates a new empty Future.
func NewFuture() (fut *Future) {
	fut = &Future{}
	fut.ready = make(chan struct{}, 1000000000)
	fut.done = make(chan struct{})
	fut.pushes = make([]*Response, 0)
	return fut
}

// AppendPush appends the push response to the future.
// Note: it works only before SetResponse() or SetError()
func (fut *Future) AppendPush(resp *Response) {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.isDone() {
		return
	}
	resp.Code = PushCode
	fut.pushes = append(fut.pushes, resp)

	fut.ready <- struct{}{}
}

// SetResponse sets a response for the future and finishes the future.
func (fut *Future) SetResponse(resp *Response) {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.isDone() {
		return
	}
	fut.resp = resp

	close(fut.ready)
	close(fut.done)
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

// Get waits for Future to be filled and returns Response and error.
//
// Response will contain deserialized result in Data field.
// It will be []interface{}, so if you want more performance, use GetTyped method.
//
// Note: Response could be equal to nil if ClientError is returned in error.
//
// "error" could be Error, if it is error returned by Tarantool,
// or ClientError, if something bad happens in a client process.
func (fut *Future) Get() (*Response, error) {
	fut.wait()
	if fut.err != nil {
		return fut.resp, fut.err
	}
	err := fut.resp.decodeBody()
	if err != nil {
		fut.err = err
	}
	return fut.resp, err
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
	err := fut.resp.decodeBodyTyped(result)
	if err != nil {
		fut.err = err
	}
	return err
}

// GetIterator returns an iterator for iterating through push messages
// and a response. Push messages and the response will contain deserialized
// result in Data field as for the Get() function.
//
// # See also
//
// * box.session.push() https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_session/push/
func (fut *Future) GetIterator() (it TimeoutResponseIterator) {
	futit := &asyncResponseIterator{
		fut: fut,
	}
	return futit
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

// Err returns error set on Future.
// It waits for future to be set.
// Note: it doesn't decode body, therefore decoding error are not set here.
func (fut *Future) Err() error {
	fut.wait()
	return fut.err
}
