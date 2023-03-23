package tarantool

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

func fillSearch(enc *encoder, spaceNo, indexNo uint32, key interface{}) error {
	if err := encodeUint(enc, KeySpaceNo); err != nil {
		return err
	}
	if err := encodeUint(enc, uint64(spaceNo)); err != nil {
		return err
	}
	if err := encodeUint(enc, KeyIndexNo); err != nil {
		return err
	}
	if err := encodeUint(enc, uint64(indexNo)); err != nil {
		return err
	}
	if err := encodeUint(enc, KeyKey); err != nil {
		return err
	}
	return enc.Encode(key)
}

func fillIterator(enc *encoder, offset, limit, iterator uint32) error {
	if err := encodeUint(enc, KeyIterator); err != nil {
		return err
	}
	if err := encodeUint(enc, uint64(iterator)); err != nil {
		return err
	}
	if err := encodeUint(enc, KeyOffset); err != nil {
		return err
	}
	if err := encodeUint(enc, uint64(offset)); err != nil {
		return err
	}
	if err := encodeUint(enc, KeyLimit); err != nil {
		return err
	}
	return encodeUint(enc, uint64(limit))
}

func fillInsert(enc *encoder, spaceNo uint32, tuple interface{}) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}
	if err := encodeUint(enc, KeySpaceNo); err != nil {
		return err
	}
	if err := encodeUint(enc, uint64(spaceNo)); err != nil {
		return err
	}
	if err := encodeUint(enc, KeyTuple); err != nil {
		return err
	}
	return enc.Encode(tuple)
}

func fillSelect(enc *encoder, spaceNo, indexNo, offset, limit, iterator uint32,
	key, after interface{}, fetchPos bool) error {
	mapLen := 6
	if fetchPos {
		mapLen += 1
	}
	if after != nil {
		mapLen += 1
	}
	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}
	if err := fillIterator(enc, offset, limit, iterator); err != nil {
		return err
	}
	if err := fillSearch(enc, spaceNo, indexNo, key); err != nil {
		return err
	}
	if fetchPos {
		if err := encodeUint(enc, KeyFetchPos); err != nil {
			return err
		}
		if err := enc.EncodeBool(fetchPos); err != nil {
			return err
		}
	}
	if after != nil {
		if pos, ok := after.([]byte); ok {
			if err := encodeUint(enc, KeyAfterPos); err != nil {
				return err
			}
			if err := enc.EncodeString(string(pos)); err != nil {
				return err
			}
		} else {
			if err := encodeUint(enc, KeyAfterTuple); err != nil {
				return err
			}
			if err := enc.Encode(after); err != nil {
				return err
			}
		}
	}
	return nil
}

func fillUpdate(enc *encoder, spaceNo, indexNo uint32, key, ops interface{}) error {
	enc.EncodeMapLen(4)
	if err := fillSearch(enc, spaceNo, indexNo, key); err != nil {
		return err
	}
	encodeUint(enc, KeyTuple)
	return enc.Encode(ops)
}

func fillUpsert(enc *encoder, spaceNo uint32, tuple, ops interface{}) error {
	enc.EncodeMapLen(3)
	encodeUint(enc, KeySpaceNo)
	encodeUint(enc, uint64(spaceNo))
	encodeUint(enc, KeyTuple)
	if err := enc.Encode(tuple); err != nil {
		return err
	}
	encodeUint(enc, KeyDefTuple)
	return enc.Encode(ops)
}

func fillDelete(enc *encoder, spaceNo, indexNo uint32, key interface{}) error {
	enc.EncodeMapLen(3)
	return fillSearch(enc, spaceNo, indexNo, key)
}

func fillCall(enc *encoder, functionName string, args interface{}) error {
	enc.EncodeMapLen(2)
	encodeUint(enc, KeyFunctionName)
	enc.EncodeString(functionName)
	encodeUint(enc, KeyTuple)
	return enc.Encode(args)
}

func fillEval(enc *encoder, expr string, args interface{}) error {
	enc.EncodeMapLen(2)
	encodeUint(enc, KeyExpression)
	enc.EncodeString(expr)
	encodeUint(enc, KeyTuple)
	return enc.Encode(args)
}

func fillExecute(enc *encoder, expr string, args interface{}) error {
	enc.EncodeMapLen(2)
	encodeUint(enc, KeySQLText)
	enc.EncodeString(expr)
	encodeUint(enc, KeySQLBind)
	return encodeSQLBind(enc, args)
}

func fillPing(enc *encoder) error {
	return enc.EncodeMapLen(0)
}

// Ping sends empty request to Tarantool to check connection.
func (conn *Connection) Ping() (resp *Response, err error) {
	return conn.Do(NewPingRequest()).Get()
}

// Select performs select to box space.
//
// It is equal to conn.SelectAsync(...).Get().
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
func (conn *Connection) UpsertTyped(space interface{}, tuple, ops, result interface{}) (err error) {
	return conn.UpsertAsync(space, tuple, ops).GetTyped(result)
}

// Call calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
//
// It is equal to conn.CallAsync(functionName, args).Get().
func (conn *Connection) Call(functionName string, args interface{}) (resp *Response, err error) {
	return conn.CallAsync(functionName, args).Get()
}

// Call16 calls registered Tarantool function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
// Deprecated since Tarantool 1.7.2.
//
// It is equal to conn.Call16Async(functionName, args).Get().
func (conn *Connection) Call16(functionName string, args interface{}) (resp *Response, err error) {
	return conn.Call16Async(functionName, args).Get()
}

// Call17 calls registered Tarantool function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).Get().
func (conn *Connection) Call17(functionName string, args interface{}) (resp *Response, err error) {
	return conn.Call17Async(functionName, args).Get()
}

// Eval passes Lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).Get().
func (conn *Connection) Eval(expr string, args interface{}) (resp *Response, err error) {
	return conn.EvalAsync(expr, args).Get()
}

// Execute passes sql expression to Tarantool for execution.
//
// It is equal to conn.ExecuteAsync(expr, args).Get().
// Since 1.6.0
func (conn *Connection) Execute(expr string, args interface{}) (resp *Response, err error) {
	return conn.ExecuteAsync(expr, args).Get()
}

// single used for conn.GetTyped for decode one tuple.
type single struct {
	res   interface{}
	found bool
}

func (s *single) DecodeMsgpack(d *decoder) error {
	var err error
	var len int
	if len, err = d.DecodeArrayLen(); err != nil {
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
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
//
// It is equal to conn.Call16Async(functionName, args).GetTyped(&result).
func (conn *Connection) CallTyped(functionName string, args interface{}, result interface{}) (err error) {
	return conn.CallAsync(functionName, args).GetTyped(result)
}

// Call16Typed calls registered function.
// It uses request code for Tarantool 1.6, so result is converted to array of arrays
// Deprecated since Tarantool 1.7.2.
//
// It is equal to conn.Call16Async(functionName, args).GetTyped(&result).
func (conn *Connection) Call16Typed(functionName string, args interface{}, result interface{}) (err error) {
	return conn.Call16Async(functionName, args).GetTyped(result)
}

// Call17Typed calls registered function.
// It uses request code for Tarantool >= 1.7, so result is not converted
// (though, keep in mind, result is always array)
//
// It is equal to conn.Call17Async(functionName, args).GetTyped(&result).
func (conn *Connection) Call17Typed(functionName string, args interface{}, result interface{}) (err error) {
	return conn.Call17Async(functionName, args).GetTyped(result)
}

// EvalTyped passes Lua expression for evaluation.
//
// It is equal to conn.EvalAsync(space, tuple).GetTyped(&result).
func (conn *Connection) EvalTyped(expr string, args interface{}, result interface{}) (err error) {
	return conn.EvalAsync(expr, args).GetTyped(result)
}

// ExecuteTyped passes sql expression to Tarantool for execution.
//
// In addition to error returns sql info and columns meta data
// Since 1.6.0
func (conn *Connection) ExecuteTyped(expr string, args interface{}, result interface{}) (SQLInfo, []ColumnMetaData, error) {
	fut := conn.ExecuteAsync(expr, args)
	err := fut.GetTyped(&result)
	return fut.resp.SQLInfo, fut.resp.MetaData, err
}

// SelectAsync sends select request to Tarantool and returns Future.
func (conn *Connection) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Future {
	req := NewSelectRequest(space).
		Index(index).
		Offset(offset).
		Limit(limit).
		Iterator(iterator).
		Key(key)
	return conn.Do(req)
}

// InsertAsync sends insert action to Tarantool and returns Future.
// Tarantool will reject Insert when tuple with same primary key exists.
func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	req := NewInsertRequest(space).Tuple(tuple)
	return conn.Do(req)
}

// ReplaceAsync sends "insert or replace" action to Tarantool and returns Future.
// If tuple with same primary key exists, it will be replaced.
func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	req := NewReplaceRequest(space).Tuple(tuple)
	return conn.Do(req)
}

// DeleteAsync sends deletion action to Tarantool and returns Future.
// Future's result will contain array with deleted tuple.
func (conn *Connection) DeleteAsync(space, index interface{}, key interface{}) *Future {
	req := NewDeleteRequest(space).Index(index).Key(key)
	return conn.Do(req)
}

// Update sends deletion of a tuple by key and returns Future.
// Future's result will contain array with updated tuple.
func (conn *Connection) UpdateAsync(space, index interface{}, key, ops interface{}) *Future {
	req := NewUpdateRequest(space).Index(index).Key(key)
	req.ops = ops
	return conn.Do(req)
}

// UpsertAsync sends "update or insert" action to Tarantool and returns Future.
// Future's sesult will not contain any tuple.
func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops interface{}) *Future {
	req := NewUpsertRequest(space).Tuple(tuple)
	req.ops = ops
	return conn.Do(req)
}

// CallAsync sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7 if go-tarantool
// was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func (conn *Connection) CallAsync(functionName string, args interface{}) *Future {
	req := NewCallRequest(functionName).Args(args)
	return conn.Do(req)
}

// Call16Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool 1.6, so future's result is always array of arrays.
// Deprecated since Tarantool 1.7.2.
func (conn *Connection) Call16Async(functionName string, args interface{}) *Future {
	req := NewCall16Request(functionName).Args(args)
	return conn.Do(req)
}

// Call17Async sends a call to registered Tarantool function and returns Future.
// It uses request code for Tarantool >= 1.7, so future's result will not be converted
// (though, keep in mind, result is always array)
func (conn *Connection) Call17Async(functionName string, args interface{}) *Future {
	req := NewCall17Request(functionName).Args(args)
	return conn.Do(req)
}

// EvalAsync sends a Lua expression for evaluation and returns Future.
func (conn *Connection) EvalAsync(expr string, args interface{}) *Future {
	req := NewEvalRequest(expr).Args(args)
	return conn.Do(req)
}

// ExecuteAsync sends a sql expression for execution and returns Future.
// Since 1.6.0
func (conn *Connection) ExecuteAsync(expr string, args interface{}) *Future {
	req := NewExecuteRequest(expr).Args(args)
	return conn.Do(req)
}

// KeyValueBind is a type for encoding named SQL parameters
type KeyValueBind struct {
	Key   string
	Value interface{}
}

//
// private
//

// this map is needed for caching names of struct fields in lower case
// to avoid extra allocations in heap by calling strings.ToLower()
var lowerCaseNames sync.Map

func encodeSQLBind(enc *encoder, from interface{}) error {
	// internal function for encoding single map in msgpack
	encodeKeyInterface := func(key string, val interface{}) error {
		if err := enc.EncodeMapLen(1); err != nil {
			return err
		}
		if err := enc.EncodeString(":" + key); err != nil {
			return err
		}
		if err := enc.Encode(val); err != nil {
			return err
		}
		return nil
	}

	encodeKeyValue := func(key string, val reflect.Value) error {
		if err := enc.EncodeMapLen(1); err != nil {
			return err
		}
		if err := enc.EncodeString(":" + key); err != nil {
			return err
		}
		if err := enc.EncodeValue(val); err != nil {
			return err
		}
		return nil
	}

	encodeNamedFromMap := func(mp map[string]interface{}) error {
		if err := enc.EncodeArrayLen(len(mp)); err != nil {
			return err
		}
		for k, v := range mp {
			if err := encodeKeyInterface(k, v); err != nil {
				return err
			}
		}
		return nil
	}

	encodeNamedFromStruct := func(val reflect.Value) error {
		if err := enc.EncodeArrayLen(val.NumField()); err != nil {
			return err
		}
		cached, ok := lowerCaseNames.Load(val.Type())
		if !ok {
			fields := make([]string, val.NumField())
			for i := 0; i < val.NumField(); i++ {
				key := val.Type().Field(i).Name
				fields[i] = strings.ToLower(key)
				v := val.Field(i)
				if err := encodeKeyValue(fields[i], v); err != nil {
					return err
				}
			}
			lowerCaseNames.Store(val.Type(), fields)
			return nil
		}

		fields := cached.([]string)
		for i := 0; i < val.NumField(); i++ {
			k := fields[i]
			v := val.Field(i)
			if err := encodeKeyValue(k, v); err != nil {
				return err
			}
		}
		return nil
	}

	encodeSlice := func(from interface{}) error {
		castedSlice, ok := from.([]interface{})
		if !ok {
			castedKVSlice := from.([]KeyValueBind)
			t := len(castedKVSlice)
			if err := enc.EncodeArrayLen(t); err != nil {
				return err
			}
			for _, v := range castedKVSlice {
				if err := encodeKeyInterface(v.Key, v.Value); err != nil {
					return err
				}
			}
			return nil
		}

		if err := enc.EncodeArrayLen(len(castedSlice)); err != nil {
			return err
		}
		for i := 0; i < len(castedSlice); i++ {
			if kvb, ok := castedSlice[i].(KeyValueBind); ok {
				k := kvb.Key
				v := kvb.Value
				if err := encodeKeyInterface(k, v); err != nil {
					return err
				}
			} else {
				if err := enc.Encode(castedSlice[i]); err != nil {
					return err
				}
			}
		}
		return nil
	}

	val := reflect.ValueOf(from)
	switch val.Kind() {
	case reflect.Map:
		mp, ok := from.(map[string]interface{})
		if !ok {
			return errors.New("failed to encode map: wrong format")
		}
		if err := encodeNamedFromMap(mp); err != nil {
			return err
		}
	case reflect.Struct:
		if err := encodeNamedFromStruct(val); err != nil {
			return err
		}
	case reflect.Slice, reflect.Array:
		if err := encodeSlice(from); err != nil {
			return err
		}
	}
	return nil
}

// Request is an interface that provides the necessary data to create a request
// that will be sent to a tarantool instance.
type Request interface {
	// Code returns a IPROTO code for the request.
	Code() int32
	// Body fills an encoder with a request body.
	Body(resolver SchemaResolver, enc *encoder) error
	// Ctx returns a context of the request.
	Ctx() context.Context
	// Async returns true if the request does not expect response.
	Async() bool
}

// ConnectedRequest is an interface that provides the info about a Connection
// the request belongs to.
type ConnectedRequest interface {
	Request
	// Conn returns a Connection the request belongs to.
	Conn() *Connection
}

type baseRequest struct {
	requestCode int32
	async       bool
	ctx         context.Context
}

// Code returns a IPROTO code for the request.
func (req *baseRequest) Code() int32 {
	return req.requestCode
}

// Async returns true if the request does not require a response.
func (req *baseRequest) Async() bool {
	return req.async
}

// Ctx returns a context of the request.
func (req *baseRequest) Ctx() context.Context {
	return req.ctx
}

type spaceRequest struct {
	baseRequest
	space interface{}
}

func (req *spaceRequest) setSpace(space interface{}) {
	req.space = space
}

type spaceIndexRequest struct {
	spaceRequest
	index interface{}
}

func (req *spaceIndexRequest) setIndex(index interface{}) {
	req.index = index
}

// authRequest implements IPROTO_AUTH request.
type authRequest struct {
	auth       Auth
	user, pass string
}

// newChapSha1AuthRequest create a new authRequest with chap-sha1
// authentication method.
func newChapSha1AuthRequest(user, password, salt string) (authRequest, error) {
	req := authRequest{}
	scr, err := scramble(salt, password)
	if err != nil {
		return req, fmt.Errorf("scrambling failure: %w", err)
	}

	req.auth = ChapSha1Auth
	req.user = user
	req.pass = string(scr)
	return req, nil
}

// newPapSha256AuthRequest create a new authRequest with pap-sha256
// authentication method.
func newPapSha256AuthRequest(user, password string) authRequest {
	return authRequest{
		auth: PapSha256Auth,
		user: user,
		pass: password,
	}
}

// Code returns a IPROTO code for the request.
func (req authRequest) Code() int32 {
	return AuthRequestCode
}

// Async returns true if the request does not require a response.
func (req authRequest) Async() bool {
	return false
}

// Ctx returns a context of the request.
func (req authRequest) Ctx() context.Context {
	return nil
}

// Body fills an encoder with the auth request body.
func (req authRequest) Body(res SchemaResolver, enc *encoder) error {
	return enc.Encode(map[uint32]interface{}{
		KeyUserName: req.user,
		KeyTuple:    []interface{}{req.auth.String(), req.pass},
	})
}

// PingRequest helps you to create an execute request object for execution
// by a Connection.
type PingRequest struct {
	baseRequest
}

// NewPingRequest returns a new PingRequest.
func NewPingRequest() *PingRequest {
	req := new(PingRequest)
	req.requestCode = PingRequestCode
	return req
}

// Body fills an encoder with the ping request body.
func (req *PingRequest) Body(res SchemaResolver, enc *encoder) error {
	return fillPing(enc)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *PingRequest) Context(ctx context.Context) *PingRequest {
	req.ctx = ctx
	return req
}

// SelectRequest allows you to create a select request object for execution
// by a Connection.
type SelectRequest struct {
	spaceIndexRequest
	isIteratorSet, fetchPos bool
	offset, limit, iterator uint32
	key, after              interface{}
}

// NewSelectRequest returns a new empty SelectRequest.
func NewSelectRequest(space interface{}) *SelectRequest {
	req := new(SelectRequest)
	req.requestCode = SelectRequestCode
	req.setSpace(space)
	req.isIteratorSet = false
	req.fetchPos = false
	req.iterator = IterAll
	req.key = []interface{}{}
	req.after = nil
	req.limit = 0xFFFFFFFF
	return req
}

// Index sets the index for the select request.
// Note: default value is 0.
func (req *SelectRequest) Index(index interface{}) *SelectRequest {
	req.setIndex(index)
	return req
}

// Offset sets the offset for the select request.
// Note: default value is 0.
func (req *SelectRequest) Offset(offset uint32) *SelectRequest {
	req.offset = offset
	return req
}

// Limit sets the limit for the select request.
// Note: default value is 0xFFFFFFFF.
func (req *SelectRequest) Limit(limit uint32) *SelectRequest {
	req.limit = limit
	return req
}

// Iterator set the iterator for the select request.
// Note: default value is IterAll if key is not set or IterEq otherwise.
func (req *SelectRequest) Iterator(iterator uint32) *SelectRequest {
	req.iterator = iterator
	req.isIteratorSet = true
	return req
}

// Key set the key for the select request.
// Note: default value is empty.
func (req *SelectRequest) Key(key interface{}) *SelectRequest {
	req.key = key
	if !req.isIteratorSet {
		req.iterator = IterEq
	}
	return req
}

// FetchPos determines whether to fetch positions of the last tuple. A position
// descriptor will be saved in Response.Pos value.
//
// Note: default value is false.
//
// Requires Tarantool >= 2.11.
// Since 1.11.0
func (req *SelectRequest) FetchPos(fetch bool) *SelectRequest {
	req.fetchPos = fetch
	return req
}

// After must contain a tuple from which selection must continue or its
// position (a value from Response.Pos).
//
// Note: default value in nil.
//
// Requires Tarantool >= 2.11.
// Since 1.11.0
func (req *SelectRequest) After(after interface{}) *SelectRequest {
	req.after = after
	return req
}

// Body fills an encoder with the select request body.
func (req *SelectRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, indexNo, err := res.ResolveSpaceIndex(req.space, req.index)
	if err != nil {
		return err
	}

	return fillSelect(enc, spaceNo, indexNo, req.offset, req.limit, req.iterator,
		req.key, req.after, req.fetchPos)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *SelectRequest) Context(ctx context.Context) *SelectRequest {
	req.ctx = ctx
	return req
}

// InsertRequest helps you to create an insert request object for execution
// by a Connection.
type InsertRequest struct {
	spaceRequest
	tuple interface{}
}

// NewInsertRequest returns a new empty InsertRequest.
func NewInsertRequest(space interface{}) *InsertRequest {
	req := new(InsertRequest)
	req.requestCode = InsertRequestCode
	req.setSpace(space)
	req.tuple = []interface{}{}
	return req
}

// Tuple sets the tuple for insertion the insert request.
// Note: default value is nil.
func (req *InsertRequest) Tuple(tuple interface{}) *InsertRequest {
	req.tuple = tuple
	return req
}

// Body fills an encoder with the insert request body.
func (req *InsertRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, _, err := res.ResolveSpaceIndex(req.space, nil)
	if err != nil {
		return err
	}

	return fillInsert(enc, spaceNo, req.tuple)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *InsertRequest) Context(ctx context.Context) *InsertRequest {
	req.ctx = ctx
	return req
}

// ReplaceRequest helps you to create a replace request object for execution
// by a Connection.
type ReplaceRequest struct {
	spaceRequest
	tuple interface{}
}

// NewReplaceRequest returns a new empty ReplaceRequest.
func NewReplaceRequest(space interface{}) *ReplaceRequest {
	req := new(ReplaceRequest)
	req.requestCode = ReplaceRequestCode
	req.setSpace(space)
	req.tuple = []interface{}{}
	return req
}

// Tuple sets the tuple for replace by the replace request.
// Note: default value is nil.
func (req *ReplaceRequest) Tuple(tuple interface{}) *ReplaceRequest {
	req.tuple = tuple
	return req
}

// Body fills an encoder with the replace request body.
func (req *ReplaceRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, _, err := res.ResolveSpaceIndex(req.space, nil)
	if err != nil {
		return err
	}

	return fillInsert(enc, spaceNo, req.tuple)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *ReplaceRequest) Context(ctx context.Context) *ReplaceRequest {
	req.ctx = ctx
	return req
}

// DeleteRequest helps you to create a delete request object for execution
// by a Connection.
type DeleteRequest struct {
	spaceIndexRequest
	key interface{}
}

// NewDeleteRequest returns a new empty DeleteRequest.
func NewDeleteRequest(space interface{}) *DeleteRequest {
	req := new(DeleteRequest)
	req.requestCode = DeleteRequestCode
	req.setSpace(space)
	req.key = []interface{}{}
	return req
}

// Index sets the index for the delete request.
// Note: default value is 0.
func (req *DeleteRequest) Index(index interface{}) *DeleteRequest {
	req.setIndex(index)
	return req
}

// Key sets the key of tuple for the delete request.
// Note: default value is empty.
func (req *DeleteRequest) Key(key interface{}) *DeleteRequest {
	req.key = key
	return req
}

// Body fills an encoder with the delete request body.
func (req *DeleteRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, indexNo, err := res.ResolveSpaceIndex(req.space, req.index)
	if err != nil {
		return err
	}

	return fillDelete(enc, spaceNo, indexNo, req.key)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *DeleteRequest) Context(ctx context.Context) *DeleteRequest {
	req.ctx = ctx
	return req
}

// UpdateRequest helps you to create an update request object for execution
// by a Connection.
type UpdateRequest struct {
	spaceIndexRequest
	key interface{}
	ops interface{}
}

// NewUpdateRequest returns a new empty UpdateRequest.
func NewUpdateRequest(space interface{}) *UpdateRequest {
	req := new(UpdateRequest)
	req.requestCode = UpdateRequestCode
	req.setSpace(space)
	req.key = []interface{}{}
	req.ops = []interface{}{}
	return req
}

// Index sets the index for the update request.
// Note: default value is 0.
func (req *UpdateRequest) Index(index interface{}) *UpdateRequest {
	req.setIndex(index)
	return req
}

// Key sets the key of tuple for the update request.
// Note: default value is empty.
func (req *UpdateRequest) Key(key interface{}) *UpdateRequest {
	req.key = key
	return req
}

// Operations sets operations to be performed on update.
// Note: default value is empty.
func (req *UpdateRequest) Operations(ops *Operations) *UpdateRequest {
	if ops != nil {
		req.ops = ops.ops
	}
	return req
}

// Body fills an encoder with the update request body.
func (req *UpdateRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, indexNo, err := res.ResolveSpaceIndex(req.space, req.index)
	if err != nil {
		return err
	}

	return fillUpdate(enc, spaceNo, indexNo, req.key, req.ops)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *UpdateRequest) Context(ctx context.Context) *UpdateRequest {
	req.ctx = ctx
	return req
}

// UpsertRequest helps you to create an upsert request object for execution
// by a Connection.
type UpsertRequest struct {
	spaceRequest
	tuple interface{}
	ops   interface{}
}

// NewUpsertRequest returns a new empty UpsertRequest.
func NewUpsertRequest(space interface{}) *UpsertRequest {
	req := new(UpsertRequest)
	req.requestCode = UpsertRequestCode
	req.setSpace(space)
	req.tuple = []interface{}{}
	req.ops = []interface{}{}
	return req
}

// Tuple sets the tuple for insertion or update by the upsert request.
// Note: default value is empty.
func (req *UpsertRequest) Tuple(tuple interface{}) *UpsertRequest {
	req.tuple = tuple
	return req
}

// Operations sets operations to be performed on update case by the upsert request.
// Note: default value is empty.
func (req *UpsertRequest) Operations(ops *Operations) *UpsertRequest {
	if ops != nil {
		req.ops = ops.ops
	}
	return req
}

// Body fills an encoder with the upsert request body.
func (req *UpsertRequest) Body(res SchemaResolver, enc *encoder) error {
	spaceNo, _, err := res.ResolveSpaceIndex(req.space, nil)
	if err != nil {
		return err
	}

	return fillUpsert(enc, spaceNo, req.tuple, req.ops)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *UpsertRequest) Context(ctx context.Context) *UpsertRequest {
	req.ctx = ctx
	return req
}

// CallRequest helps you to create a call request object for execution
// by a Connection.
type CallRequest struct {
	baseRequest
	function string
	args     interface{}
}

// NewCallRequest return a new empty CallRequest. It uses request code for
// Tarantool >= 1.7 if go-tarantool was build with go_tarantool_call_17 tag.
// Otherwise, uses request code for Tarantool 1.6.
func NewCallRequest(function string) *CallRequest {
	req := new(CallRequest)
	req.requestCode = CallRequestCode
	req.function = function
	return req
}

// Args sets the args for the call request.
// Note: default value is empty.
func (req *CallRequest) Args(args interface{}) *CallRequest {
	req.args = args
	return req
}

// Body fills an encoder with the call request body.
func (req *CallRequest) Body(res SchemaResolver, enc *encoder) error {
	args := req.args
	if args == nil {
		args = []interface{}{}
	}
	return fillCall(enc, req.function, args)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *CallRequest) Context(ctx context.Context) *CallRequest {
	req.ctx = ctx
	return req
}

// NewCall16Request returns a new empty Call16Request. It uses request code for
// Tarantool 1.6.
// Deprecated since Tarantool 1.7.2.
func NewCall16Request(function string) *CallRequest {
	req := NewCallRequest(function)
	req.requestCode = Call16RequestCode
	return req
}

// NewCall17Request returns a new empty CallRequest. It uses request code for
// Tarantool >= 1.7.
func NewCall17Request(function string) *CallRequest {
	req := NewCallRequest(function)
	req.requestCode = Call17RequestCode
	return req
}

// EvalRequest helps you to create an eval request object for execution
// by a Connection.
type EvalRequest struct {
	baseRequest
	expr string
	args interface{}
}

// NewEvalRequest returns a new empty EvalRequest.
func NewEvalRequest(expr string) *EvalRequest {
	req := new(EvalRequest)
	req.requestCode = EvalRequestCode
	req.expr = expr
	req.args = []interface{}{}
	return req
}

// Args sets the args for the eval request.
// Note: default value is empty.
func (req *EvalRequest) Args(args interface{}) *EvalRequest {
	req.args = args
	return req
}

// Body fills an encoder with the eval request body.
func (req *EvalRequest) Body(res SchemaResolver, enc *encoder) error {
	return fillEval(enc, req.expr, req.args)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *EvalRequest) Context(ctx context.Context) *EvalRequest {
	req.ctx = ctx
	return req
}

// ExecuteRequest helps you to create an execute request object for execution
// by a Connection.
type ExecuteRequest struct {
	baseRequest
	expr string
	args interface{}
}

// NewExecuteRequest returns a new empty ExecuteRequest.
func NewExecuteRequest(expr string) *ExecuteRequest {
	req := new(ExecuteRequest)
	req.requestCode = ExecuteRequestCode
	req.expr = expr
	req.args = []interface{}{}
	return req
}

// Args sets the args for the execute request.
// Note: default value is empty.
func (req *ExecuteRequest) Args(args interface{}) *ExecuteRequest {
	req.args = args
	return req
}

// Body fills an encoder with the execute request body.
func (req *ExecuteRequest) Body(res SchemaResolver, enc *encoder) error {
	return fillExecute(enc, req.expr, req.args)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *ExecuteRequest) Context(ctx context.Context) *ExecuteRequest {
	req.ctx = ctx
	return req
}
