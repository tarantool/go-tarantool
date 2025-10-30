package pool_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
	. "github.com/tarantool/go-tarantool/v3/pool"
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

var errReq error = errors.New("response error")
var reqFuture *tarantool.Future = &tarantool.Future{}

var reqFunctionName string = "any_name"
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
	return reqPrepared, errReq
}

func TestConnectorNewPrepared(t *testing.T) {
	m := &newPreparedMock{}
	c := NewConnectorAdapter(m, testMode)

	p, err := c.NewPrepared(reqFunctionName)

	require.Equalf(t, reqPrepared, p, "unexpected prepared")
	require.Equalf(t, errReq, err, "unexpected error")
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
	return reqStream, errReq
}

func TestConnectorNewStream(t *testing.T) {
	m := &newStreamMock{}
	c := NewConnectorAdapter(m, testMode)

	s, err := c.NewStream()

	require.Equalf(t, reqStream, s, "unexpected stream")
	require.Equalf(t, errReq, err, "unexpected error")
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
	return reqWatcher, errReq
}

func TestConnectorNewWatcher(t *testing.T) {
	m := &newWatcherMock{}
	c := NewConnectorAdapter(m, testMode)

	w, err := c.NewWatcher(reqWatchKey, func(event tarantool.WatchEvent) {})

	require.Equalf(t, reqWatcher, w, "unexpected watcher")
	require.Equalf(t, errReq, err, "unexpected error")
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
