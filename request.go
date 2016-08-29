package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type Request struct {
	conn        *Connection
	requestId   uint32
	requestCode int32
	body        map[int]interface{}
}

type Future struct {
	req     *Request
	resp    Response
	err     error
	ready   chan struct{}
	timeout *time.Timer
}

func (conn *Connection) NewRequest(requestCode int32) (req *Request) {
	req = &Request{}
	req.conn = conn
	req.requestId = conn.nextRequestId()
	req.requestCode = requestCode
	req.body = make(map[int]interface{})
	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	request := conn.NewRequest(PingRequest)
	resp, err = request.perform()
	return
}

func (req *Request) fillSearch(spaceNo, indexNo uint32, key []interface{}) {
	req.body[KeySpaceNo] = spaceNo
	req.body[KeyIndexNo] = indexNo
	req.body[KeyKey] = key
}

func (req *Request) fillIterator(offset, limit, iterator uint32) {
	req.body[KeyIterator] = iterator
	req.body[KeyOffset] = offset
	req.body[KeyLimit] = limit
}

func (req *Request) fillInsert(spaceNo uint32, tuple interface{}) {
	req.body[KeySpaceNo] = spaceNo
	req.body[KeyTuple] = tuple
}

func (conn *Connection) Select(space, index interface{}, offset, limit, iterator uint32, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.fillIterator(offset, limit, iterator)
	resp, err = request.perform()
	return
}

func (conn *Connection) Insert(space interface{}, tuple interface{}) (resp *Response, err error) {
	request := conn.NewRequest(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.fillInsert(spaceNo, tuple)
	resp, err = request.perform()
	return
}

func (conn *Connection) Replace(space interface{}, tuple interface{}) (resp *Response, err error) {
	request := conn.NewRequest(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.fillInsert(spaceNo, tuple)
	resp, err = request.perform()
	return
}

func (conn *Connection) Delete(space, index interface{}, key []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	resp, err = request.perform()
	return
}

func (conn *Connection) Update(space, index interface{}, key, ops []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.body[KeyTuple] = ops
	resp, err = request.perform()
	return
}

func (conn *Connection) Upsert(space interface{}, tuple, ops []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.body[KeySpaceNo] = spaceNo
	request.body[KeyTuple] = tuple
	request.body[KeyDefTuple] = ops
	resp, err = request.perform()
	return
}

func (conn *Connection) Call(functionName string, args []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(CallRequest)
	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple] = args
	resp, err = request.perform()
	return
}

func (conn *Connection) Eval(expr string, args []interface{}) (resp *Response, err error) {
	request := conn.NewRequest(EvalRequest)
	request.body[KeyExpression] = expr
	request.body[KeyTuple] = args
	resp, err = request.perform()
	return
}

// Typed methods
func (conn *Connection) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.fillIterator(offset, limit, iterator)
	return request.performTyped(result)
}

func (conn *Connection) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	request := conn.NewRequest(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.fillInsert(spaceNo, tuple)
	return request.performTyped(result)
}

func (conn *Connection) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	request := conn.NewRequest(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.fillInsert(spaceNo, tuple)
	return request.performTyped(result)
}

func (conn *Connection) DeleteTyped(space, index interface{}, key []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	return request.performTyped(result)
}

func (conn *Connection) UpdateTyped(space, index interface{}, key, ops []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.body[KeyTuple] = ops
	return request.performTyped(result)
}

func (conn *Connection) UpsertTyped(space interface{}, tuple, ops []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return
	}
	request.body[KeySpaceNo] = spaceNo
	request.body[KeyTuple] = tuple
	request.body[KeyDefTuple] = ops
	return request.performTyped(result)
}

func (conn *Connection) CallTyped(functionName string, args []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(CallRequest)
	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple] = args
	return request.performTyped(result)
}

func (conn *Connection) EvalTyped(expr string, args []interface{}, result interface{}) (err error) {
	request := conn.NewRequest(EvalRequest)
	request.body[KeyExpression] = expr
	request.body[KeyTuple] = args
	return request.performTyped(result)
}

// Async methods
func (conn *Connection) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key []interface{}) *Future {
	request := conn.NewRequest(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.fillIterator(offset, limit, iterator)
	return request.future()
}

func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	request := conn.NewRequest(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	request.fillInsert(spaceNo, tuple)
	return request.future()
}

func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	request := conn.NewRequest(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	request.fillInsert(spaceNo, tuple)
	return request.future()
}

func (conn *Connection) DeleteAsync(space, index interface{}, key []interface{}) *Future {
	request := conn.NewRequest(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	request.fillSearch(spaceNo, indexNo, key)
	return request.future()
}

func (conn *Connection) UpdateAsync(space, index interface{}, key, ops []interface{}) *Future {
	request := conn.NewRequest(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	request.fillSearch(spaceNo, indexNo, key)
	request.body[KeyTuple] = ops
	return request.future()
}

func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops []interface{}) *Future {
	request := conn.NewRequest(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	request.body[KeySpaceNo] = spaceNo
	request.body[KeyTuple] = tuple
	request.body[KeyDefTuple] = ops
	return request.future()
}

func (conn *Connection) CallAsync(functionName string, args []interface{}) *Future {
	request := conn.NewRequest(CallRequest)
	request.body[KeyFunctionName] = functionName
	request.body[KeyTuple] = args
	return request.future()
}

func (conn *Connection) EvalAsync(expr string, args []interface{}) *Future {
	request := conn.NewRequest(EvalRequest)
	request.body[KeyExpression] = expr
	request.body[KeyTuple] = args
	return request.future()
}

//
// private
//

func (req *Request) perform() (resp *Response, err error) {
	return req.future().Get()
}

func (req *Request) performTyped(res interface{}) (err error) {
	return req.future().GetTyped(res)
}

func (req *Request) pack() (packet []byte, err error) {
	rid := req.requestId
	h := smallWBuf{
		0xce, 0, 0, 0, 0, // length
		0x82,                           // 2 element map
		KeyCode, byte(req.requestCode), // request code
		KeySync, 0xce,
		byte(rid >> 24), byte(rid >> 16),
		byte(rid >> 8), byte(rid),
	}

	enc := msgpack.NewEncoder(&h)
	err = enc.EncodeMapLen(len(req.body))
	if err != nil {
		return
	}
	for k, v := range req.body {
		err = enc.EncodeInt64(int64(k))
		if err != nil {
			return
		}
		switch vv := v.(type) {
		case uint32:
			err = enc.EncodeUint64(uint64(vv))
		default:
			err = enc.Encode(vv)
		}
		if err != nil {
			return
		}
	}

	l := uint32(len(h) - 5)
	h[1] = byte(l >> 24)
	h[2] = byte(l >> 16)
	h[3] = byte(l >> 8)
	h[4] = byte(l)

	packet = h
	return
}

func (req *Request) future() (fut *Future) {
	fut = &Future{req: req}

	// check connection ready to process packets
	if closed := req.conn.closed; closed {
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	if c := req.conn.c; c == nil {
		fut.err = ClientError{ErrConnectionNotReady, "client connection is not ready"}
		return
	}

	var packet []byte
	if packet, fut.err = req.pack(); fut.err != nil {
		return
	}

	req.conn.mutex.Lock()
	if req.conn.closed {
		req.conn.mutex.Unlock()
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	req.conn.requests[req.requestId] = fut
	req.conn.mutex.Unlock()

	fut.ready = make(chan struct{})
	// TODO: packets may lock
	req.conn.packets <- (packet)

	if req.conn.opts.Timeout > 0 {
		fut.timeout = time.NewTimer(req.conn.opts.Timeout)
	}
	return
}

func badfuture(err error) *Future {
	return &Future{err: err}
}

func (fut *Future) wait() {
	if fut.ready == nil {
		return
	}
	conn := fut.req.conn
	requestId := fut.req.requestId
	select {
	case <-fut.ready:
	default:
		if timeout := fut.timeout; timeout != nil {
			select {
			case <-fut.ready:
			case <-timeout.C:
				conn.mutex.Lock()
				if _, ok := conn.requests[requestId]; ok {
					delete(conn.requests, requestId)
					close(fut.ready)
					fut.err = fmt.Errorf("client timeout for request %d", requestId)
				}
				conn.mutex.Unlock()
			}
		} else {
			<-fut.ready
		}
	}
	if fut.timeout != nil {
		fut.timeout.Stop()
		fut.timeout = nil
	}
}

func (fut *Future) Get() (*Response, error) {
	fut.wait()
	if fut.err != nil {
		return &fut.resp, fut.err
	}
	fut.err = fut.resp.decodeBody()
	return &fut.resp, fut.err
}

func (fut *Future) GetTyped(result interface{}) error {
	fut.wait()
	if fut.err != nil {
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}
