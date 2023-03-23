package connection_pool_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/connection_pool"
)

var testMode Mode = RW

type connectedNowMock struct {
	Pooler
	called int
	mode   Mode
	retErr bool
}

// Tests for different logic.

func (m *connectedNowMock) ConnectedNow(mode Mode) (bool, error) {
	m.called++
	m.mode = mode

	if m.retErr {
		return true, errors.New("mock error")
	}
	return true, nil
}

func TestConnectorConnectedNow(t *testing.T) {
	m := &connectedNowMock{retErr: false}
	c := NewConnectorAdapter(m, testMode)

	require.Truef(t, c.ConnectedNow(), "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

func TestConnectorConnectedNowWithError(t *testing.T) {
	m := &connectedNowMock{retErr: true}
	c := NewConnectorAdapter(m, testMode)

	require.Falsef(t, c.ConnectedNow(), "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type closeMock struct {
	Pooler
	called int
	retErr bool
}

func (m *closeMock) Close() []error {
	m.called++
	if m.retErr {
		return []error{errors.New("err1"), errors.New("err2")}
	}
	return nil
}

func TestConnectorClose(t *testing.T) {
	m := &closeMock{retErr: false}
	c := NewConnectorAdapter(m, testMode)

	require.Nilf(t, c.Close(), "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
}

func TestConnectorCloseWithError(t *testing.T) {
	m := &closeMock{retErr: true}
	c := NewConnectorAdapter(m, testMode)

	err := c.Close()
	require.NotNilf(t, err, "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equal(t, "failed to close connection pool: err1: err2", err.Error())
}

type configuredTimeoutMock struct {
	Pooler
	called  int
	timeout time.Duration
	mode    Mode
	retErr  bool
}

func (m *configuredTimeoutMock) ConfiguredTimeout(mode Mode) (time.Duration, error) {
	m.called++
	m.mode = mode
	m.timeout = 5 * time.Second
	if m.retErr {
		return m.timeout, fmt.Errorf("err")
	}
	return m.timeout, nil
}

func TestConnectorConfiguredTimeout(t *testing.T) {
	m := &configuredTimeoutMock{retErr: false}
	c := NewConnectorAdapter(m, testMode)

	require.Equalf(t, c.ConfiguredTimeout(), m.timeout, "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

func TestConnectorConfiguredTimeoutWithError(t *testing.T) {
	m := &configuredTimeoutMock{retErr: true}
	c := NewConnectorAdapter(m, testMode)

	ret := c.ConfiguredTimeout()

	require.NotEqualf(t, ret, m.timeout, "unexpected result")
	require.Equalf(t, ret, time.Duration(0), "unexpected result")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

// Tests for that ConnectorAdapter is just a proxy for requests.

type baseRequestMock struct {
	Pooler
	called                  int
	functionName            string
	offset, limit, iterator uint32
	space, index            interface{}
	args, tuple, key, ops   interface{}
	result                  interface{}
	mode                    Mode
}

var reqResp *tarantool.Response = &tarantool.Response{}
var reqErr error = errors.New("response error")
var reqFuture *tarantool.Future = &tarantool.Future{}

var reqFunctionName string = "any_name"
var reqOffset uint32 = 1
var reqLimit uint32 = 2
var reqIterator uint32 = 3
var reqSpace interface{} = []interface{}{1}
var reqIndex interface{} = []interface{}{2}
var reqArgs interface{} = []interface{}{3}
var reqTuple interface{} = []interface{}{4}
var reqKey interface{} = []interface{}{5}
var reqOps interface{} = []interface{}{6}

var reqResult interface{} = []interface{}{7}
var reqSqlInfo = tarantool.SQLInfo{AffectedCount: 3}
var reqMeta = []tarantool.ColumnMetaData{{FieldIsNullable: false}}

type getTypedMock struct {
	baseRequestMock
}

func (m *getTypedMock) GetTyped(space, index, key interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorGetTyped(t *testing.T) {
	m := &getTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.GetTyped(reqSpace, reqIndex, reqKey, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type selectMock struct {
	baseRequestMock
}

func (m *selectMock) Select(space, index interface{},
	offset, limit, iterator uint32, key interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.index = index
	m.offset = offset
	m.limit = limit
	m.iterator = iterator
	m.key = key
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorSelect(t *testing.T) {
	m := &selectMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Select(reqSpace, reqIndex, reqOffset, reqLimit, reqIterator, reqKey)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqOffset, m.offset, "unexpected offset was passed")
	require.Equalf(t, reqLimit, m.limit, "unexpected limit was passed")
	require.Equalf(t, reqIterator, m.iterator, "unexpected iterator was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type selectTypedMock struct {
	baseRequestMock
}

func (m *selectTypedMock) SelectTyped(space, index interface{},
	offset, limit, iterator uint32, key interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.index = index
	m.offset = offset
	m.limit = limit
	m.iterator = iterator
	m.key = key
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorSelectTyped(t *testing.T) {
	m := &selectTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.SelectTyped(reqSpace, reqIndex, reqOffset, reqLimit,
		reqIterator, reqKey, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqOffset, m.offset, "unexpected offset was passed")
	require.Equalf(t, reqLimit, m.limit, "unexpected limit was passed")
	require.Equalf(t, reqIterator, m.iterator, "unexpected iterator was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type selectAsyncMock struct {
	baseRequestMock
}

func (m *selectAsyncMock) SelectAsync(space, index interface{},
	offset, limit, iterator uint32, key interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.index = index
	m.offset = offset
	m.limit = limit
	m.iterator = iterator
	m.key = key
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorSelectAsync(t *testing.T) {
	m := &selectAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.SelectAsync(reqSpace, reqIndex, reqOffset, reqLimit,
		reqIterator, reqKey)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqOffset, m.offset, "unexpected offset was passed")
	require.Equalf(t, reqLimit, m.limit, "unexpected limit was passed")
	require.Equalf(t, reqIterator, m.iterator, "unexpected iterator was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type insertMock struct {
	baseRequestMock
}

func (m *insertMock) Insert(space, tuple interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.tuple = tuple
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorInsert(t *testing.T) {
	m := &insertMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Insert(reqSpace, reqTuple)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type insertTypedMock struct {
	baseRequestMock
}

func (m *insertTypedMock) InsertTyped(space, tuple interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.tuple = tuple
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorInsertTyped(t *testing.T) {
	m := &insertTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.InsertTyped(reqSpace, reqTuple, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type insertAsyncMock struct {
	baseRequestMock
}

func (m *insertAsyncMock) InsertAsync(space, tuple interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.tuple = tuple
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorInsertAsync(t *testing.T) {
	m := &insertAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.InsertAsync(reqSpace, reqTuple)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type replaceMock struct {
	baseRequestMock
}

func (m *replaceMock) Replace(space, tuple interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.tuple = tuple
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorReplace(t *testing.T) {
	m := &replaceMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Replace(reqSpace, reqTuple)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type replaceTypedMock struct {
	baseRequestMock
}

func (m *replaceTypedMock) ReplaceTyped(space, tuple interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.tuple = tuple
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorReplaceTyped(t *testing.T) {
	m := &replaceTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.ReplaceTyped(reqSpace, reqTuple, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type replaceAsyncMock struct {
	baseRequestMock
}

func (m *replaceAsyncMock) ReplaceAsync(space, tuple interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.tuple = tuple
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorReplaceAsync(t *testing.T) {
	m := &replaceAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.ReplaceAsync(reqSpace, reqTuple)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type deleteMock struct {
	baseRequestMock
}

func (m *deleteMock) Delete(space, index, key interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorDelete(t *testing.T) {
	m := &deleteMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Delete(reqSpace, reqIndex, reqKey)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type deleteTypedMock struct {
	baseRequestMock
}

func (m *deleteTypedMock) DeleteTyped(space, index, key interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorDeleteTyped(t *testing.T) {
	m := &deleteTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.DeleteTyped(reqSpace, reqIndex, reqKey, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type deleteAsyncMock struct {
	baseRequestMock
}

func (m *deleteAsyncMock) DeleteAsync(space, index, key interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorDeleteAsync(t *testing.T) {
	m := &deleteAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.DeleteAsync(reqSpace, reqIndex, reqKey)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type updateMock struct {
	baseRequestMock
}

func (m *updateMock) Update(space, index, key, ops interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.ops = ops
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorUpdate(t *testing.T) {
	m := &updateMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Update(reqSpace, reqIndex, reqKey, reqOps)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqOps, m.ops, "unexpected ops was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type updateTypedMock struct {
	baseRequestMock
}

func (m *updateTypedMock) UpdateTyped(space, index, key, ops interface{},
	result interface{}, mode ...Mode) error {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.ops = ops
	m.result = result
	m.mode = mode[0]
	return reqErr
}

func TestConnectorUpdateTyped(t *testing.T) {
	m := &updateTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.UpdateTyped(reqSpace, reqIndex, reqKey, reqOps, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqOps, m.ops, "unexpected ops was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type updateAsyncMock struct {
	baseRequestMock
}

func (m *updateAsyncMock) UpdateAsync(space, index, key, ops interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.index = index
	m.key = key
	m.ops = ops
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorUpdateAsync(t *testing.T) {
	m := &updateAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.UpdateAsync(reqSpace, reqIndex, reqKey, reqOps)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqIndex, m.index, "unexpected index was passed")
	require.Equalf(t, reqKey, m.key, "unexpected key was passed")
	require.Equalf(t, reqOps, m.ops, "unexpected ops was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type upsertMock struct {
	baseRequestMock
}

func (m *upsertMock) Upsert(space, tuple, ops interface{},
	mode ...Mode) (*tarantool.Response, error) {
	m.called++
	m.space = space
	m.tuple = tuple
	m.ops = ops
	m.mode = mode[0]
	return reqResp, reqErr
}

func TestConnectorUpsert(t *testing.T) {
	m := &upsertMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Upsert(reqSpace, reqTuple, reqOps)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, reqOps, m.ops, "unexpected ops was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type upsertAsyncMock struct {
	baseRequestMock
}

func (m *upsertAsyncMock) UpsertAsync(space, tuple, ops interface{},
	mode ...Mode) *tarantool.Future {
	m.called++
	m.space = space
	m.tuple = tuple
	m.ops = ops
	m.mode = mode[0]
	return reqFuture
}

func TestConnectorUpsertAsync(t *testing.T) {
	m := &upsertAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.UpsertAsync(reqSpace, reqTuple, reqOps)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqSpace, m.space, "unexpected space was passed")
	require.Equalf(t, reqTuple, m.tuple, "unexpected tuple was passed")
	require.Equalf(t, reqOps, m.ops, "unexpected ops was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type baseCallMock struct {
	baseRequestMock
}

func (m *baseCallMock) call(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	m.called++
	m.functionName = functionName
	m.args = args
	m.mode = mode
	return reqResp, reqErr
}

func (m *baseCallMock) callTyped(functionName string, args interface{},
	result interface{}, mode Mode) error {
	m.called++
	m.functionName = functionName
	m.args = args
	m.result = result
	m.mode = mode
	return reqErr
}

func (m *baseCallMock) callAsync(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	m.called++
	m.functionName = functionName
	m.args = args
	m.mode = mode
	return reqFuture
}

type callMock struct {
	baseCallMock
}

func (m *callMock) Call(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	return m.call(functionName, args, mode)
}

func TestConnectorCall(t *testing.T) {
	m := &callMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Call(reqFunctionName, reqArgs)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type callTypedMock struct {
	baseCallMock
}

func (m *callTypedMock) CallTyped(functionName string, args interface{},
	result interface{}, mode Mode) error {
	return m.callTyped(functionName, args, result, mode)
}

func TestConnectorCallTyped(t *testing.T) {
	m := &callTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.CallTyped(reqFunctionName, reqArgs, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type callAsyncMock struct {
	baseCallMock
}

func (m *callAsyncMock) CallAsync(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	return m.callAsync(functionName, args, mode)
}

func TestConnectorCallAsync(t *testing.T) {
	m := &callAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.CallAsync(reqFunctionName, reqArgs)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call16Mock struct {
	baseCallMock
}

func (m *call16Mock) Call16(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	return m.call(functionName, args, mode)
}

func TestConnectorCall16(t *testing.T) {
	m := &call16Mock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Call16(reqFunctionName, reqArgs)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call16TypedMock struct {
	baseCallMock
}

func (m *call16TypedMock) Call16Typed(functionName string, args interface{},
	result interface{}, mode Mode) error {
	return m.callTyped(functionName, args, result, mode)
}

func TestConnectorCall16Typed(t *testing.T) {
	m := &call16TypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.Call16Typed(reqFunctionName, reqArgs, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call16AsyncMock struct {
	baseCallMock
}

func (m *call16AsyncMock) Call16Async(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	return m.callAsync(functionName, args, mode)
}

func TestConnectorCall16Async(t *testing.T) {
	m := &call16AsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.Call16Async(reqFunctionName, reqArgs)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call17Mock struct {
	baseCallMock
}

func (m *call17Mock) Call17(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	return m.call(functionName, args, mode)
}

func TestConnectorCall17(t *testing.T) {
	m := &call17Mock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Call17(reqFunctionName, reqArgs)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call17TypedMock struct {
	baseCallMock
}

func (m *call17TypedMock) Call17Typed(functionName string, args interface{},
	result interface{}, mode Mode) error {
	return m.callTyped(functionName, args, result, mode)
}

func TestConnectorCall17Typed(t *testing.T) {
	m := &call17TypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.Call17Typed(reqFunctionName, reqArgs, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type call17AsyncMock struct {
	baseCallMock
}

func (m *call17AsyncMock) Call17Async(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	return m.callAsync(functionName, args, mode)
}

func TestConnectorCall17Async(t *testing.T) {
	m := &call17AsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.Call17Async(reqFunctionName, reqArgs)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected functionName was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type evalMock struct {
	baseCallMock
}

func (m *evalMock) Eval(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	return m.call(functionName, args, mode)
}

func TestConnectorEval(t *testing.T) {
	m := &evalMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Eval(reqFunctionName, reqArgs)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type evalTypedMock struct {
	baseCallMock
}

func (m *evalTypedMock) EvalTyped(functionName string, args interface{},
	result interface{}, mode Mode) error {
	return m.callTyped(functionName, args, result, mode)
}

func TestConnectorEvalTyped(t *testing.T) {
	m := &evalTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	err := c.EvalTyped(reqFunctionName, reqArgs, reqResult)

	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type evalAsyncMock struct {
	baseCallMock
}

func (m *evalAsyncMock) EvalAsync(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	return m.callAsync(functionName, args, mode)
}

func TestConnectorEvalAsync(t *testing.T) {
	m := &evalAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.EvalAsync(reqFunctionName, reqArgs)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type executeMock struct {
	baseCallMock
}

func (m *executeMock) Execute(functionName string, args interface{},
	mode Mode) (*tarantool.Response, error) {
	return m.call(functionName, args, mode)
}

func TestConnectorExecute(t *testing.T) {
	m := &executeMock{}
	c := NewConnectorAdapter(m, testMode)

	resp, err := c.Execute(reqFunctionName, reqArgs)

	require.Equalf(t, reqResp, resp, "unexpected response")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type executeTypedMock struct {
	baseCallMock
}

func (m *executeTypedMock) ExecuteTyped(functionName string, args, result interface{},
	mode Mode) (tarantool.SQLInfo, []tarantool.ColumnMetaData, error) {
	m.callTyped(functionName, args, result, mode)
	return reqSqlInfo, reqMeta, reqErr
}

func TestConnectorExecuteTyped(t *testing.T) {
	m := &executeTypedMock{}
	c := NewConnectorAdapter(m, testMode)

	info, meta, err := c.ExecuteTyped(reqFunctionName, reqArgs, reqResult)

	require.Equalf(t, reqSqlInfo, info, "unexpected info")
	require.Equalf(t, reqMeta, meta, "unexpected meta")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, reqResult, m.result, "unexpected result was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type executeAsyncMock struct {
	baseCallMock
}

func (m *executeAsyncMock) ExecuteAsync(functionName string, args interface{},
	mode Mode) *tarantool.Future {
	return m.callAsync(functionName, args, mode)
}

func TestConnectorExecuteAsync(t *testing.T) {
	m := &executeAsyncMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.ExecuteAsync(reqFunctionName, reqArgs)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.functionName,
		"unexpected expr was passed")
	require.Equalf(t, reqArgs, m.args, "unexpected args was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

var reqPrepared *tarantool.Prepared = &tarantool.Prepared{}

type newPreparedMock struct {
	Pooler
	called int
	expr   string
	mode   Mode
}

func (m *newPreparedMock) NewPrepared(expr string,
	mode Mode) (*tarantool.Prepared, error) {
	m.called++
	m.expr = expr
	m.mode = mode
	return reqPrepared, reqErr
}

func TestConnectorNewPrepared(t *testing.T) {
	m := &newPreparedMock{}
	c := NewConnectorAdapter(m, testMode)

	p, err := c.NewPrepared(reqFunctionName)

	require.Equalf(t, reqPrepared, p, "unexpected prepared")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqFunctionName, m.expr,
		"unexpected expr was passed")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

var reqStream *tarantool.Stream = &tarantool.Stream{}

type newStreamMock struct {
	Pooler
	called int
	mode   Mode
}

func (m *newStreamMock) NewStream(mode Mode) (*tarantool.Stream, error) {
	m.called++
	m.mode = mode
	return reqStream, reqErr
}

func TestConnectorNewStream(t *testing.T) {
	m := &newStreamMock{}
	c := NewConnectorAdapter(m, testMode)

	s, err := c.NewStream()

	require.Equalf(t, reqStream, s, "unexpected stream")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

type watcherMock struct{}

func (w *watcherMock) Unregister() {}

const reqWatchKey = "foo"

var reqWatcher tarantool.Watcher = &watcherMock{}

type newWatcherMock struct {
	Pooler
	key      string
	callback tarantool.WatchCallback
	called   int
	mode     Mode
}

func (m *newWatcherMock) NewWatcher(key string,
	callback tarantool.WatchCallback, mode Mode) (tarantool.Watcher, error) {
	m.called++
	m.key = key
	m.callback = callback
	m.mode = mode
	return reqWatcher, reqErr
}

func TestConnectorNewWatcher(t *testing.T) {
	m := &newWatcherMock{}
	c := NewConnectorAdapter(m, testMode)

	w, err := c.NewWatcher(reqWatchKey, func(event tarantool.WatchEvent) {})

	require.Equalf(t, reqWatcher, w, "unexpected watcher")
	require.Equalf(t, reqErr, err, "unexpected error")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqWatchKey, m.key, "unexpected key")
	require.NotNilf(t, m.callback, "callback must be set")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}

var reqRequest tarantool.Request = tarantool.NewPingRequest()

type doMock struct {
	Pooler
	called int
	req    tarantool.Request
	mode   Mode
}

func (m *doMock) Do(req tarantool.Request, mode Mode) *tarantool.Future {
	m.called++
	m.req = req
	m.mode = mode
	return reqFuture
}

func TestConnectorDo(t *testing.T) {
	m := &doMock{}
	c := NewConnectorAdapter(m, testMode)

	fut := c.Do(reqRequest)

	require.Equalf(t, reqFuture, fut, "unexpected future")
	require.Equalf(t, 1, m.called, "should be called only once")
	require.Equalf(t, reqRequest, m.req, "unexpected request")
	require.Equalf(t, testMode, m.mode, "unexpected proxy mode")
}
