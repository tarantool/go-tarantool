package tarantool

import (
	"errors"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// Future is a handle for asynchronous request
type Future struct {
	requestId   uint32
	requestCode int32
	timeout     time.Duration
	resp        *Response
	err         error
	ready       chan struct{}
	next        *Future
}

// Ping sends empty request to Tarantool to check connection.
func (conn *Connection) Ping() (resp *Response, err error) {
	future := conn.newFuture(PingRequest)
	return future.send(conn, func(enc *msgpack.Encoder) error { enc.EncodeMapLen(0); return nil }).Get()
}

func (req *Future) fillSearch(enc *msgpack.Encoder, spaceNo, indexNo uint32, key interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyIndexNo)
	enc.EncodeUint64(uint64(indexNo))
	enc.EncodeUint64(KeyKey)
	return enc.Encode(key)
}

func (req *Future) fillIterator(enc *msgpack.Encoder, offset, limit, iterator uint32) {
	enc.EncodeUint64(KeyIterator)
	enc.EncodeUint64(uint64(iterator))
	enc.EncodeUint64(KeyOffset)
	enc.EncodeUint64(uint64(offset))
	enc.EncodeUint64(KeyLimit)
	enc.EncodeUint64(uint64(limit))
}

func (req *Future) fillInsert(enc *msgpack.Encoder, spaceNo uint32, tuple interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyTuple)
	return enc.Encode(tuple)
}

// Select performs select to box space.
//
// It is equal to conn.SelectAsync(...).Get()
func (conn *Connection) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (resp *Response, err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).Get()
}

// Insert performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// It is equal to conn.InsertAsync(space, tuple).Get().
func (conn *Connection) Insert(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.InsertAsync(space, tuple).Get()
}

// Replace performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// It is equal to conn.ReplaceAsync(space, tuple).Get().
func (conn *Connection) Replace(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.ReplaceAsync(space, tuple).Get()
}

// Delete performs deletion of a tuple by key.
// Result will contain array with deleted tuple.
//
// It is equal to conn.DeleteAsync(space, tuple).Get().
func (conn *Connection) Delete(space, index interface{}, key interface{}) (resp *Response, err error) {
	return conn.DeleteAsync(space, index, key).Get()
}

// Update performs update of a tuple by key.
// Result will contain array with updated tuple.
//
// It is equal to conn.UpdateAsync(space, tuple).Get().
func (conn *Connection) Update(space, index interface{}, key, ops interface{}) (resp *Response, err error) {
	return conn.UpdateAsync(space, index, key, ops).Get()
}

// Upsert performs "update or insert" action of a tuple by key.
// Result will not contain any tuple.
//
// It is equal to conn.UpsertAsync(space, tuple, ops).Get().
func (conn *Connection) Upsert(space interface{}, tuple, ops interface{}) (resp *Response, err error) {
	return conn.UpsertAsync(space, tuple, ops).Get()
}

// Call calls registered tarantool function.
// It uses request code for tarantool 1.6, so result is converted to array of arrays
//
// It is equal to conn.CallAsync(functionName, args).Get().
func (conn *Connection) Call(functionName string, args interface{}) (resp *Response, err error) {
	return conn.CallAsync(functionName, args).Get()
}

// Call17 calls registered tarantool function.
// It uses request code for tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).Get().
func (conn *Connection) Call17(functionName string, args interface{}) (resp *Response, err error) {
	return conn.Call17Async(functionName, args).Get()
}

// Eval passes lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).Get().
func (conn *Connection) Eval(expr string, args interface{}) (resp *Response, err error) {
	return conn.EvalAsync(expr, args).Get()
}

// single used for conn.GetTyped for decode one tuple
type single struct {
	res   interface{}
	found bool
}

func (s *single) DecodeMsgpack(d *msgpack.Decoder) error {
	var err error
	var len int
	if len, err = d.DecodeSliceLen(); err != nil {
		return err
	}
	if s.found = len >= 1; !s.found {
		return nil
	}
	if len != 1 {
		return errors.New("Tarantool returns unexpected value for Select(limit=1)")
	}
	return d.Decode(s.res)
}

// GetTyped performs select (with limit = 1 and offset = 0)
// to box space and fills typed result.
//
// It is equal to conn.SelectAsync(space, index, 0, 1, IterEq, key).GetTyped(&result)
func (conn *Connection) GetTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	s := single{res: result}
	err = conn.SelectAsync(space, index, 0, 1, IterEq, key).GetTyped(&s)
	return
}

// SelectTyped performs select to box space and fills typed result.
//
// It is equal to conn.SelectAsync(space, index, offset, limit, iterator, key).GetTyped(&result)
func (conn *Connection) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key interface{}, result interface{}) (err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).GetTyped(result)
}

// InsertTyped performs insertion to box space.
// Tarantool will reject Insert when tuple with same primary key exists.
//
// It is equal to conn.InsertAsync(space, tuple).GetTyped(&result).
func (conn *Connection) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.InsertAsync(space, tuple).GetTyped(result)
}

// ReplaceTyped performs "insert or replace" action to box space.
// If tuple with same primary key exists, it will be replaced.
//
// It is equal to conn.ReplaceAsync(space, tuple).GetTyped(&result).
func (conn *Connection) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.ReplaceAsync(space, tuple).GetTyped(result)
}

// DeleteTyped performs deletion of a tuple by key and fills result with deleted tuple.
//
// It is equal to conn.DeleteAsync(space, tuple).GetTyped(&result).
func (conn *Connection) DeleteTyped(space, index interface{}, key interface{}, result interface{}) (err error) {
	return conn.DeleteAsync(space, index, key).GetTyped(result)
}

// UpdateTyped performs update of a tuple by key and fills result with updated tuple.
//
// It is equal to conn.UpdateAsync(space, tuple, ops).GetTyped(&result).
func (conn *Connection) UpdateTyped(space, index interface{}, key, ops interface{}, result interface{}) (err error) {
	return conn.UpdateAsync(space, index, key, ops).GetTyped(result)
}

// CallTyped calls registered function.
// It uses request code for tarantool 1.6, so result is converted to array of arrays
//
// It is equal to conn.CallAsync(functionName, args).GetTyped(&result).
func (conn *Connection) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	return conn.CallAsync(functionName, args).GetTyped(result)
}

// Call17Typed calls registered function.
// It uses request code for tarantool 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).GetTyped(&result).
func (conn *Connection) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	return conn.Call17Async(functionName, args).GetTyped(result)
}

// EvalTyped passes lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).GetTyped(&result).
func (conn *Connection) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	return conn.EvalAsync(expr, args).GetTyped(result)
}

// SelectAsync sends select request to tarantool and returns Future.
func (conn *Connection) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Future {
	future := conn.newFuture(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(6)
		future.fillIterator(enc, offset, limit, iterator)
		return future.fillSearch(enc, spaceNo, indexNo, key)
	})
}

// InsertAsync sends insert action to tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return future.fillInsert(enc, spaceNo, tuple)
	})
}

// ReplaceAsync sends "insert or replace" action to tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return future.fillInsert(enc, spaceNo, tuple)
	})
}

// DeleteAsync sends deletion action to tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (conn *Connection) DeleteAsync(space, index interface{}, key interface{}) *Future {
	future := conn.newFuture(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		return future.fillSearch(enc, spaceNo, indexNo, key)
	})
}

// Update sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (conn *Connection) UpdateAsync(space, index interface{}, key, ops interface{}) *Future {
	future := conn.newFuture(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(4)
		if err := future.fillSearch(enc, spaceNo, indexNo, key); err != nil {
			return err
		}
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(ops)
	})
}

// UpsertAsync sends "update or insert" action to tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *Future {
	future := conn.newFuture(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return future.fail(conn, err)
	}
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		enc.EncodeUint64(KeySpaceNo)
		enc.EncodeUint64(uint64(spaceNo))
		enc.EncodeUint64(KeyTuple)
		if err := enc.Encode(tuple); err != nil {
			return err
		}
		enc.EncodeUint64(KeyDefTuple)
		return enc.Encode(ops)
	})
}

// CallAsync sends a call to registered tarantool function and returns Future.
// It uses request code for tarantool 1.6, so future's result is always array of arrays
func (conn *Connection) CallAsync(functionName string, args interface{}) *Future {
	future := conn.newFuture(CallRequest)
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

// Call17Async sends a call to registered tarantool function and returns Future.
// It uses request code for tarantool 1.7, so future's result will not be converted
// (though, keep in mind, result is always array)
func (conn *Connection) Call17Async(functionName string, args interface{}) *Future {
	future := conn.newFuture(Call17Request)
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

// EvalAsync sends a lua expression for evaluation and returns Future.
func (conn *Connection) EvalAsync(expr string, args interface{}) *Future {
	future := conn.newFuture(EvalRequest)
	return future.send(conn, func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyExpression)
		enc.EncodeString(expr)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

//
// private
//

func (fut *Future) pack(h *smallWBuf, enc *msgpack.Encoder, body func(*msgpack.Encoder) error) (err error) {
	rid := fut.requestId
	hl := len(*h)
	*h = append(*h, smallWBuf{
		0xce, 0, 0, 0, 0, // length
		0x82,                           // 2 element map
		KeyCode, byte(fut.requestCode), // request code
		KeySync, 0xce,
		byte(rid >> 24), byte(rid >> 16),
		byte(rid >> 8), byte(rid),
	}...)

	if err = body(enc); err != nil {
		return
	}

	l := uint32(len(*h) - 5 - hl)
	(*h)[hl+1] = byte(l >> 24)
	(*h)[hl+2] = byte(l >> 16)
	(*h)[hl+3] = byte(l >> 8)
	(*h)[hl+4] = byte(l)

	return
}

func (fut *Future) send(conn *Connection, body func(*msgpack.Encoder) error) *Future {
	if fut.ready == nil {
		return fut
	}
	conn.putFuture(fut, body)
	if fut.err != nil {
		if f := conn.fetchFuture(fut.requestId); f == fut {
			fut.markReady(conn)
		}
		return fut
	}

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

// Get waits for Future to be filled and returns Response and error
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
// Note: Tarantool usually returns array of tuples (except for Eval and Call17 actions)
func (fut *Future) GetTyped(result interface{}) error {
	fut.wait()
	if fut.err != nil {
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}
