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
	pushes    []Response
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

type asyncResponseIterator struct {
	fut     *Future
	timeout time.Duration
	resp    Response
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

	if last {
		it.done = true
	} else {
		it.err = nil
		it.curPos += 1
	}

	return true
}

func (it *asyncResponseIterator) Value() Response {
	return it.resp
}

func (it *asyncResponseIterator) IsPush() bool {
	return !it.done
}

func (it *asyncResponseIterator) Err() error {
	return it.err
}

func (it *asyncResponseIterator) WithTimeout(timeout time.Duration) TimeoutResponseIterator {
	it.timeout = timeout
	return it
}

func (it *asyncResponseIterator) nextResponse() (resp Response) {
	fut := it.fut
	pushesLen := len(fut.pushes)

	if it.curPos < pushesLen {
		resp = fut.pushes[it.curPos]
	} else if it.curPos == pushesLen {
		resp = fut.resp
	}

	return resp
}

// PushResponse is used for push requests for the Future.
type PushResponse struct {
	baseResponse
}

func createPushResponse(header Header, body io.Reader) (Response, error) {
	resp, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}
	return &PushResponse{resp}, nil
}

// NewFuture creates a new empty Future for a given Request.
func NewFuture(req Request) (fut *Future) {
	fut = &Future{}
	fut.ready = make(chan struct{}, 1000000000)
	fut.done = make(chan struct{})
	fut.pushes = make([]Response, 0)
	fut.req = req
	return fut
}

// AppendPush appends the push response to the future.
// Note: it works only before SetResponse() or SetError()
//
// Deprecated: the method will be removed in the next major version,
// use Connector.NewWatcher() instead of box.session.push().
func (fut *Future) AppendPush(header Header, body io.Reader) error {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	if fut.isDone() {
		return nil
	}
	resp, err := createPushResponse(header, body)
	if err != nil {
		return err
	}
	fut.pushes = append(fut.pushes, resp)

	fut.ready <- struct{}{}
	return nil
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

// GetIterator returns an iterator for iterating through push messages
// and a response. Push messages and the response will contain deserialized
// result in Data field as for the Get() function.
//
// # See also
//
//   - box.session.push():
//     https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_session/push/
//
// Deprecated: the method will be removed in the next major version,
// use Connector.NewWatcher() instead of box.session.push().
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
