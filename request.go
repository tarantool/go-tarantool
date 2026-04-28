package tarantool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

type spaceEncoder struct {
	Id   uint32
	Name string
	IsId bool
}

func newSpaceEncoder(res SchemaResolver, spaceInfo any) (spaceEncoder, error) {
	if res.NamesUseSupported() {
		if spaceName, ok := spaceInfo.(string); ok {
			return spaceEncoder{
				Id:   0,
				Name: spaceName,
				IsId: false,
			}, nil
		}
	}

	spaceId, err := res.ResolveSpace(spaceInfo)
	return spaceEncoder{
		Id:   spaceId,
		IsId: true,
	}, err
}

func (e spaceEncoder) Encode(enc *msgpack.Encoder) error {
	if e.IsId {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_SPACE_ID)); err != nil {
			return err
		}
		return enc.EncodeUint(uint64(e.Id))
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_SPACE_NAME)); err != nil {
		return err
	}
	return enc.EncodeString(e.Name)
}

type indexEncoder struct {
	Id   uint32
	Name string
	IsId bool
}

func newIndexEncoder(res SchemaResolver, indexInfo any,
	spaceNo uint32) (indexEncoder, error) {
	if res.NamesUseSupported() {
		if indexName, ok := indexInfo.(string); ok {
			return indexEncoder{
				Name: indexName,
				IsId: false,
			}, nil
		}
	}

	indexId, err := res.ResolveIndex(indexInfo, spaceNo)
	return indexEncoder{
		Id:   indexId,
		IsId: true,
	}, err
}

func (e indexEncoder) Encode(enc *msgpack.Encoder) error {
	if e.IsId {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_INDEX_ID)); err != nil {
			return err
		}
		return enc.EncodeUint(uint64(e.Id))
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_INDEX_NAME)); err != nil {
		return err
	}
	return enc.EncodeString(e.Name)
}

func fillSearch(enc *msgpack.Encoder, spaceEnc spaceEncoder, indexEnc indexEncoder,
	key any) error {
	if err := spaceEnc.Encode(enc); err != nil {
		return err
	}

	if err := indexEnc.Encode(enc); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_KEY)); err != nil {
		return err
	}
	return enc.Encode(key)
}

// KeyValueBind is a type for encoding named SQL parameters.
type KeyValueBind struct {
	Key   string
	Value any
}

//
// private
//

// this map is needed for caching names of struct fields in lower case
// to avoid extra allocations in heap by calling strings.ToLower().
var lowerCaseNames sync.Map

func encodeSQLBind(enc *msgpack.Encoder, from any) error {
	// internal function for encoding single map in msgpack
	encodeKeyInterface := func(key string, val any) error {
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

	encodeNamedFromMap := func(mp map[string]any) error {
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

	encodeSlice := func(from any) error {
		castedSlice, ok := from.([]any)
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
		for i := range castedSlice {
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
		mp, ok := from.(map[string]any)
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
	// Type returns a IPROTO type of the request.
	Type() iproto.Type
	// Body fills an msgpack.Encoder with a request body.
	Body(resolver SchemaResolver, enc *msgpack.Encoder) error
	// Ctx returns a context of the request.
	Ctx() context.Context
	// Async returns true if the request does not expect response.
	Async() bool
	// Response creates a response for current request type.
	Response(header Header, body io.Reader) (Response, error)
}

// ConnectedRequest is an interface that provides the info about a Connection
// the request belongs to.
type ConnectedRequest interface {
	Request
	// Conn returns a Connection the request belongs to.
	Conn() *Connection
}

type baseRequest struct {
	rtype iproto.Type
	async bool
	ctx   context.Context
}

// Type returns a IPROTO type for the request.
func (req *baseRequest) Type() iproto.Type {
	return req.rtype
}

// Async returns true if the request does not require a response.
func (req *baseRequest) Async() bool {
	return req.async
}

// Ctx returns a context of the request.
func (req *baseRequest) Ctx() context.Context {
	return req.ctx
}

// Response creates a response for the baseRequest.
func (req *baseRequest) Response(header Header, body io.Reader) (Response, error) {
	return DecodeBaseResponse(header, body)
}

type spaceRequest struct {
	baseRequest

	space any
}

func (req *spaceRequest) setSpace(space any) {
	req.space = space
}

func EncodeSpace(res SchemaResolver, enc *msgpack.Encoder, space any) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	if err := spaceEnc.Encode(enc); err != nil {
		return err
	}
	return nil
}

type spaceIndexRequest struct {
	spaceRequest

	index any
}

func (req *spaceIndexRequest) setIndex(index any) {
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

// Type returns a IPROTO type for the request.
func (req authRequest) Type() iproto.Type {
	return iproto.IPROTO_AUTH
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
func (req authRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := enc.EncodeUint32(uint32(iproto.IPROTO_USER_NAME)); err != nil {
		return err
	}

	if err := enc.EncodeString(req.user); err != nil {
		return err
	}

	if err := enc.EncodeUint32(uint32(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	if err := enc.EncodeArrayLen(2); err != nil {
		return err
	}

	if err := enc.EncodeString(req.auth.String()); err != nil {
		return err
	}

	if err := enc.EncodeString(req.pass); err != nil {
		return err
	}

	return nil
}

// Response creates a response for the authRequest.
func (req authRequest) Response(header Header, body io.Reader) (Response, error) {
	return DecodeBaseResponse(header, body)
}

// PingRequest helps you to create an execute request object for execution
// by a Connection.
type PingRequest struct {
	baseRequest
}

// NewPingRequest returns a new PingRequest.
func NewPingRequest() *PingRequest {
	req := new(PingRequest)
	req.rtype = iproto.IPROTO_PING
	return req
}

// Body fills an msgpack.Encoder with the ping request body.
func (req *PingRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	return enc.EncodeMapLen(0)
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
	offset, limit           uint32
	iterator                Iter
	key, after              any
}

// NewSelectRequest returns a new empty SelectRequest.
func NewSelectRequest(space any) *SelectRequest {
	req := new(SelectRequest)
	req.rtype = iproto.IPROTO_SELECT
	req.setSpace(space)
	req.isIteratorSet = false
	req.fetchPos = false
	req.iterator = IterAll
	req.key = []any{}
	req.after = nil
	req.limit = 0xFFFFFFFF
	return req
}

// Index sets the index for the select request.
// Note: default value is 0.
func (req *SelectRequest) Index(index any) *SelectRequest {
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
func (req *SelectRequest) Iterator(iterator Iter) *SelectRequest {
	req.iterator = iterator
	req.isIteratorSet = true
	return req
}

// Key set the key for the select request.
// Note: default value is empty.
func (req *SelectRequest) Key(key any) *SelectRequest {
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
//
// Since 1.11.0.
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
//
// Since 1.11.0.
func (req *SelectRequest) After(after any) *SelectRequest {
	req.after = after
	return req
}

// Body fills an encoder with the select request body.
func (req *SelectRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	indexEnc, err := newIndexEncoder(res, req.index, spaceEnc.Id)
	if err != nil {
		return err
	}

	mapLen := 6
	if req.fetchPos {
		mapLen++
	}
	if req.after != nil {
		mapLen++
	}

	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_ITERATOR)); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(req.iterator)); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_OFFSET)); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(req.offset)); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_LIMIT)); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(req.limit)); err != nil {
		return err
	}

	if err := fillSearch(enc, spaceEnc, indexEnc, req.key); err != nil {
		return err
	}

	if req.fetchPos {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_FETCH_POSITION)); err != nil {
			return err
		}

		if err := enc.EncodeBool(req.fetchPos); err != nil {
			return err
		}
	}

	if req.after != nil {
		if pos, ok := req.after.([]byte); ok {
			if err := enc.EncodeUint(uint64(iproto.IPROTO_AFTER_POSITION)); err != nil {
				return err
			}

			if err := enc.EncodeString(string(pos)); err != nil {
				return err
			}
		} else {
			if err := enc.EncodeUint(uint64(iproto.IPROTO_AFTER_TUPLE)); err != nil {
				return err
			}

			if err := enc.Encode(req.after); err != nil {
				return err
			}
		}
	}

	return nil
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

var selectsPool *sync.Pool = &sync.Pool{
	New: func() any {
		return &SelectResponse{}
	},
}

// Response creates a response for the SelectRequest.
func (req *SelectRequest) Response(header Header, body io.Reader) (Response, error) {
	base, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}

	s := selectsPool.Get().(*SelectResponse)
	s.baseResponse = base

	return s, nil
}

// InsertRequest helps you to create an insert request object for execution

// by a Connection.

type InsertRequest struct {
	spaceRequest

	tuple any
}

// NewInsertRequest returns a new empty InsertRequest.
func NewInsertRequest(space any) *InsertRequest {
	req := new(InsertRequest)
	req.rtype = iproto.IPROTO_INSERT
	req.setSpace(space)
	req.tuple = []any{}
	return req
}

// Tuple sets the tuple for insertion the insert request.
// Note: default value is nil.
func (req *InsertRequest) Tuple(tuple any) *InsertRequest {
	req.tuple = tuple
	return req
}

// Body fills an msgpack.Encoder with the insert request body.
func (req *InsertRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := spaceEnc.Encode(enc); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	return enc.Encode(req.tuple)
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

	tuple any
}

// NewReplaceRequest returns a new empty ReplaceRequest.
func NewReplaceRequest(space any) *ReplaceRequest {
	req := new(ReplaceRequest)
	req.rtype = iproto.IPROTO_REPLACE
	req.setSpace(space)
	req.tuple = []any{}
	return req
}

// Tuple sets the tuple for replace by the replace request.
// Note: default value is nil.
func (req *ReplaceRequest) Tuple(tuple any) *ReplaceRequest {
	req.tuple = tuple
	return req
}

// Body fills an msgpack.Encoder with the replace request body.
func (req *ReplaceRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := spaceEnc.Encode(enc); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	return enc.Encode(req.tuple)
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

	key any
}

// NewDeleteRequest returns a new empty DeleteRequest.
func NewDeleteRequest(space any) *DeleteRequest {
	req := new(DeleteRequest)
	req.rtype = iproto.IPROTO_DELETE
	req.setSpace(space)
	req.key = []any{}
	return req
}

// Index sets the index for the delete request.
// Note: default value is 0.
func (req *DeleteRequest) Index(index any) *DeleteRequest {
	req.setIndex(index)
	return req
}

// Key sets the key of tuple for the delete request.
// Note: default value is empty.
func (req *DeleteRequest) Key(key any) *DeleteRequest {
	req.key = key
	return req
}

// Body fills an msgpack.Encoder with the delete request body.
func (req *DeleteRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	indexEnc, err := newIndexEncoder(res, req.index, spaceEnc.Id)
	if err != nil {
		return err
	}

	if err := enc.EncodeMapLen(3); err != nil {
		return err
	}

	return fillSearch(enc, spaceEnc, indexEnc, req.key)
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

	key any
	ops *Operations
}

// NewUpdateRequest returns a new empty UpdateRequest.
func NewUpdateRequest(space any) *UpdateRequest {
	req := new(UpdateRequest)
	req.rtype = iproto.IPROTO_UPDATE
	req.setSpace(space)
	req.key = []any{}
	return req
}

// Index sets the index for the update request.
// Note: default value is 0.
func (req *UpdateRequest) Index(index any) *UpdateRequest {
	req.setIndex(index)
	return req
}

// Key sets the key of tuple for the update request.
// Note: default value is empty.
func (req *UpdateRequest) Key(key any) *UpdateRequest {
	req.key = key
	return req
}

// Operations sets operations to be performed on update.
// Note: default value is empty.
func (req *UpdateRequest) Operations(ops *Operations) *UpdateRequest {
	req.ops = ops
	return req
}

// Body fills an msgpack.Encoder with the update request body.
func (req *UpdateRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	indexEnc, err := newIndexEncoder(res, req.index, spaceEnc.Id)
	if err != nil {
		return err
	}

	if err := enc.EncodeMapLen(4); err != nil {
		return err
	}

	if err := fillSearch(enc, spaceEnc, indexEnc, req.key); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	if req.ops == nil {
		return enc.EncodeArrayLen(0)
	} else {
		return enc.Encode(req.ops)
	}
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

	tuple any
	ops   *Operations
}

// NewUpsertRequest returns a new empty UpsertRequest.
func NewUpsertRequest(space any) *UpsertRequest {
	req := new(UpsertRequest)
	req.rtype = iproto.IPROTO_UPSERT
	req.setSpace(space)
	req.tuple = []any{}
	return req
}

// Tuple sets the tuple for insertion or update by the upsert request.
// Note: default value is empty.
func (req *UpsertRequest) Tuple(tuple any) *UpsertRequest {
	req.tuple = tuple
	return req
}

// Operations sets operations to be performed on update case by the upsert request.
// Note: default value is empty.
func (req *UpsertRequest) Operations(ops *Operations) *UpsertRequest {
	req.ops = ops
	return req
}

// Body fills an msgpack.Encoder with the upsert request body.
func (req *UpsertRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	spaceEnc, err := newSpaceEncoder(res, req.space)
	if err != nil {
		return err
	}

	if err := enc.EncodeMapLen(3); err != nil {
		return err
	}

	if err := spaceEnc.Encode(enc); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	if err := enc.Encode(req.tuple); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_OPS)); err != nil {
		return err
	}

	if req.ops == nil {
		return enc.EncodeArrayLen(0)
	} else {
		return enc.Encode(req.ops)
	}
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
	args     any
}

// NewCallRequest returns a new empty CallRequest. It uses request code for
// Tarantool >= 1.7.
func NewCallRequest(function string) *CallRequest {
	req := new(CallRequest)
	req.rtype = iproto.IPROTO_CALL
	req.function = function
	return req
}

// Args sets the args for the call request.
// Note: default value is empty.
func (req *CallRequest) Args(args any) *CallRequest {
	req.args = args
	return req
}

// Body fills an encoder with the call request body.
func (req *CallRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_FUNCTION_NAME)); err != nil {
		return err
	}

	if err := enc.EncodeString(req.function); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	if req.args == nil {
		return enc.EncodeArrayLen(0)
	} else {
		return enc.Encode(req.args)
	}
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

// EvalRequest helps you to create an eval request object for execution
// by a Connection.
type EvalRequest struct {
	baseRequest

	expr string
	args any
}

// NewEvalRequest returns a new empty EvalRequest.
func NewEvalRequest(expr string) *EvalRequest {
	req := new(EvalRequest)
	req.rtype = iproto.IPROTO_EVAL
	req.expr = expr
	req.args = []any{}
	return req
}

// Args sets the args for the eval request.
// Note: default value is empty.
func (req *EvalRequest) Args(args any) *EvalRequest {
	req.args = args
	return req
}

// Body fills an msgpack.Encoder with the eval request body.
func (req *EvalRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_EXPR)); err != nil {
		return err
	}

	if err := enc.EncodeString(req.expr); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_TUPLE)); err != nil {
		return err
	}

	if req.args == nil {
		return enc.EncodeArrayLen(0)
	} else {
		return enc.Encode(req.args)
	}
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
	args any
}

// NewExecuteRequest returns a new empty ExecuteRequest.
func NewExecuteRequest(expr string) *ExecuteRequest {
	req := new(ExecuteRequest)
	req.rtype = iproto.IPROTO_EXECUTE
	req.expr = expr
	req.args = []any{}
	return req
}

// Args sets the args for the execute request.
// Note: default value is empty.
func (req *ExecuteRequest) Args(args any) *ExecuteRequest {
	req.args = args
	return req
}

// Body fills an msgpack.Encoder with the execute request body.
func (req *ExecuteRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(2); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_SQL_TEXT)); err != nil {
		return err
	}

	if err := enc.EncodeString(req.expr); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_SQL_BIND)); err != nil {
		return err
	}

	return encodeSQLBind(enc, req.args)
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

var executesPool = sync.Pool{
	New: func() any {
		return &ExecuteResponse{}
	},
}

// Response creates a response for the ExecuteRequest.
func (req *ExecuteRequest) Response(header Header, body io.Reader) (Response, error) {
	baseResp, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}

	exec := executesPool.Get().(*ExecuteResponse)
	exec.baseResponse = baseResp

	return exec, nil
}

// WatchOnceRequest synchronously fetches the value currently associated with a
// specified notification key without subscribing to changes.
type WatchOnceRequest struct {
	baseRequest

	key string
}

// NewWatchOnceRequest returns a new watchOnceRequest.
func NewWatchOnceRequest(key string) *WatchOnceRequest {
	req := new(WatchOnceRequest)
	req.rtype = iproto.IPROTO_WATCH_ONCE
	req.key = key
	return req
}

// Body fills an msgpack.Encoder with the watchOnce request body.
func (req *WatchOnceRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(1); err != nil {
		return err
	}

	if err := enc.EncodeUint(uint64(iproto.IPROTO_EVENT_KEY)); err != nil {
		return err
	}

	return enc.EncodeString(req.key)
}

// Context sets a passed context to the request.
func (req *WatchOnceRequest) Context(ctx context.Context) *WatchOnceRequest {
	req.ctx = ctx
	return req
}
