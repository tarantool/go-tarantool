package tarantool

import (
	"io"
	"sync"
	"time"
)

// Future is an interface that handle asynchronous request.
type Future interface {
	Get() ([]any, error)
	GetTyped(result any) error
	GetResponse() (Response, error)
	// Release frees the Future resources and allows them to be reused.
	// It must be called only after the request has completed, i.e. after
	// Get(), GetTyped() or GetResponse() has returned, or after the
	// channel returned by WaitChan() has been closed. Releasing an
	// unfinished future is a no-op. After a Release() the Future must not
	// be used any more.
	Release()
	WaitChan() <-chan struct{}
}

var futurePool = sync.Pool{
	New: func() any { return &future{} },
}

// future is inner implementation of Future interface.
type future struct {
	requestId uint32
	req       Request
	// next links the future into a connection's futureList. It is owned by
	// the shard mutex of that list and must be cleared before the future is
	// finished, see the futureList documentation in connection.go.
	next     *future
	timeout  time.Duration
	mutex    sync.Mutex
	resp     Response
	err      error
	cond     sync.Cond
	finished bool
	done     chan struct{}
}

var _ = Future(&future{})

func (fut *future) wait() {
	fut.mutex.Lock()
	defer fut.mutex.Unlock()

	for !fut.finished {
		fut.cond.Wait()
	}
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
	fut = futurePool.Get().(*future)
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

// finalize is a common code across finish methods.
//
// It is the ownership handover point: once finalize has returned, the future
// belongs to the caller that awaits it. The caller may Release() it, which
// zeroes the object and returns it to futurePool, where a concurrent Do() can
// pick it up for an unrelated request. No connection code may read or write
// the future after this, and in particular it must already be unlinked from
// its futureList, see the futureList documentation in connection.go.
//
// finalize is called with fut.mutex held and returns with it released.
func (fut *future) finalize() {
	fut.finished = true

	done := fut.done

	fut.cond.Broadcast()

	fut.mutex.Unlock()

	if done != nil {
		close(done)
	}
}

func (fut *future) finish() {
	fut.mutex.Lock()

	fut.finalize()
}

// setResponse sets a response for the future and finishes the future.
func (fut *future) setResponse(header Header, body io.Reader) error {
	fut.mutex.Lock()

	if fut.finished {
		fut.mutex.Unlock()

		return nil
	}

	resp, err := fut.req.Response(header, body)
	if err != nil {
		fut.mutex.Unlock()

		return err
	}

	fut.resp = resp

	fut.finalize()

	return nil
}

// setError sets an error for the future and finishes the future.
func (fut *future) setError(err error) {
	fut.mutex.Lock()

	if fut.finished {
		fut.mutex.Unlock()

		return
	}

	fut.err = err

	fut.finalize()
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
func (fut *future) Get() ([]any, error) {
	fut.wait()
	if fut.err != nil {
		return nil, fut.err
	}
	return fut.resp.Decode()
}

// GetTyped waits for Future and calls msgpack.Decoder.Decode(result) if no error happens.
// It is could be much faster than Get() function.
//
// Note: Tarantool usually returns array of tuples (except for Eval and Call actions).
func (fut *future) GetTyped(result any) error {
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
//
// Release must be called only after the request has completed, i.e. after
// Get(), GetTyped() or GetResponse() has returned, or after the channel
// returned by WaitChan() has been closed. Until then the future still belongs
// to the connection, which keeps it in an internal list of pending requests;
// recycling it there would corrupt that list and lose the pending request.
// Releasing an unfinished future is therefore a no-op: the object is not
// reused and is left to the garbage collector.
//
// Futures created by NewFutureWithErr() and NewFutureWithResponse() are
// finished from the start, so they can be released right away.
func (fut *future) Release() {
	if !fut.isFinished() {
		return
	}

	if fut.resp != nil {
		fut.resp.Release()
	}

	*fut = future{}
	futurePool.Put(fut)
}
