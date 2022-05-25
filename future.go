package tarantool

import (
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Future is a handle for asynchronous request.
type Future struct {
	requestId   uint32
	requestCode int32
	timeout     time.Duration
	resp        *Response
	err         error
	ready       chan struct{}
	next        *Future
}

// NewErrorFuture returns new set empty Future with filled error field.
func NewErrorFuture(err error) *Future {
	return &Future{err: err}
}

// Get waits for Future to be filled and returns Response and error.
//
// Response will contain deserialized result in Data field.
// It will be []interface{}, so if you want more performace, use GetTyped method.
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
	fut.err = fut.resp.decodeBody()
	return fut.resp, fut.err
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
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}

var closedChan = make(chan struct{})

func init() {
	close(closedChan)
}

// WaitChan returns channel which becomes closed when response arrived or error occured.
func (fut *Future) WaitChan() <-chan struct{} {
	if fut.ready == nil {
		return closedChan
	}
	return fut.ready
}

// Err returns error set on Future.
// It waits for future to be set.
// Note: it doesn't decode body, therefore decoding error are not set here.
func (fut *Future) Err() error {
	fut.wait()
	return fut.err
}

func (fut *Future) pack(h *smallWBuf, enc *msgpack.Encoder, body func(*msgpack.Encoder) error) (err error) {
	rid := fut.requestId
	hl := h.Len()
	h.Write([]byte{
		0xce, 0, 0, 0, 0, // Length.
		0x82,                           // 2 element map.
		KeyCode, byte(fut.requestCode), // Request code.
		KeySync, 0xce,
		byte(rid >> 24), byte(rid >> 16),
		byte(rid >> 8), byte(rid),
	})

	if err = body(enc); err != nil {
		return
	}

	l := uint32(h.Len() - 5 - hl)
	h.b[hl+1] = byte(l >> 24)
	h.b[hl+2] = byte(l >> 16)
	h.b[hl+3] = byte(l >> 8)
	h.b[hl+4] = byte(l)

	return
}

func (fut *Future) send(conn *Connection, body func(*msgpack.Encoder) error) *Future {
	if fut.ready == nil {
		return fut
	}
	conn.putFuture(fut, body)
	return fut
}

func (fut *Future) markReady(conn *Connection) {
	close(fut.ready)
	if conn.rlimit != nil {
		<-conn.rlimit
	}
}

func (fut *Future) fail(conn *Connection, err error) *Future {
	if f := conn.fetchFuture(fut.requestId); f == fut {
		f.err = err
		fut.markReady(conn)
	}
	return fut
}

func (fut *Future) wait() {
	if fut.ready == nil {
		return
	}
	<-fut.ready
}
