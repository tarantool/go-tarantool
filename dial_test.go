package tarantool_test

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/go-tarantool"
)

type mockErrorDialer struct {
	err error
}

func (m mockErrorDialer) Dial(address string,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	return nil, m.err
}

func TestDialer_Dial_error(t *testing.T) {
	const errMsg = "any msg"
	dialer := mockErrorDialer{
		err: errors.New(errMsg),
	}

	conn, err := tarantool.Connect("any", tarantool.Opts{
		Dialer: dialer,
	})
	assert.Nil(t, conn)
	assert.ErrorContains(t, err, errMsg)
}

type mockPassedDialer struct {
	address string
	opts    tarantool.DialOpts
}

func (m *mockPassedDialer) Dial(address string,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	m.address = address
	m.opts = opts
	return nil, errors.New("does not matter")
}

func TestDialer_Dial_passedOpts(t *testing.T) {
	const addr = "127.0.0.1:8080"
	opts := tarantool.DialOpts{
		DialTimeout: 500 * time.Millisecond,
		IoTimeout:   2,
		Transport:   "any",
		Ssl: tarantool.SslOpts{
			KeyFile:  "a",
			CertFile: "b",
			CaFile:   "c",
			Ciphers:  "d",
		},
		RequiredProtocol: tarantool.ProtocolInfo{
			Auth:    tarantool.ChapSha1Auth,
			Version: 33,
			Features: []tarantool.ProtocolFeature{
				tarantool.ErrorExtensionFeature,
			},
		},
		Auth:     tarantool.ChapSha1Auth,
		User:     "user",
		Password: "password",
	}

	dialer := &mockPassedDialer{}
	conn, err := tarantool.Connect(addr, tarantool.Opts{
		Dialer:               dialer,
		Timeout:              opts.IoTimeout,
		Transport:            opts.Transport,
		Ssl:                  opts.Ssl,
		Auth:                 opts.Auth,
		User:                 opts.User,
		Pass:                 opts.Password,
		RequiredProtocolInfo: opts.RequiredProtocol,
	})

	assert.Nil(t, conn)
	assert.NotNil(t, err)
	assert.Equal(t, addr, dialer.address)
	assert.Equal(t, opts, dialer.opts)
}

type mockIoConn struct {
	// Sends an event on Read()/Write()/Flush().
	read, written chan struct{}
	// Read()/Write() buffers.
	readbuf, writebuf bytes.Buffer
	// Calls readWg/writeWg.Wait() in Read()/Flush().
	readWg, writeWg sync.WaitGroup
	// How many times to wait before a wg.Wait() call.
	readWgDelay, writeWgDelay int
	// Write()/Read()/Flush()/Close() calls count.
	writeCnt, readCnt, flushCnt, closeCnt int
	// LocalAddr()/RemoteAddr() calls count.
	localCnt, remoteCnt int
	// Greeting()/ProtocolInfo() calls count.
	greetingCnt, infoCnt int
	// Values for LocalAddr()/RemoteAddr().
	local, remote net.Addr
	// Value for Greeting().
	greeting tarantool.Greeting
	// Value for ProtocolInfo().
	info tarantool.ProtocolInfo
}

func (m *mockIoConn) Read(b []byte) (int, error) {
	m.readCnt++
	if m.readWgDelay == 0 {
		m.readWg.Wait()
	}
	m.readWgDelay--

	ret, err := m.readbuf.Read(b)

	if m.read != nil {
		m.read <- struct{}{}
	}

	return ret, err
}

func (m *mockIoConn) Write(b []byte) (int, error) {
	m.writeCnt++
	if m.writeWgDelay == 0 {
		m.writeWg.Wait()
	}
	m.writeWgDelay--

	ret, err := m.writebuf.Write(b)

	if m.written != nil {
		m.written <- struct{}{}
	}

	return ret, err
}

func (m *mockIoConn) Flush() error {
	m.flushCnt++
	return nil
}

func (m *mockIoConn) Close() error {
	m.closeCnt++
	return nil
}

func (m *mockIoConn) LocalAddr() net.Addr {
	m.localCnt++
	return m.local
}

func (m *mockIoConn) RemoteAddr() net.Addr {
	m.remoteCnt++
	return m.remote
}

func (m *mockIoConn) Greeting() tarantool.Greeting {
	m.greetingCnt++
	return m.greeting
}

func (m *mockIoConn) ProtocolInfo() tarantool.ProtocolInfo {
	m.infoCnt++
	return m.info
}

type mockIoDialer struct {
	init func(conn *mockIoConn)
	conn *mockIoConn
}

func newMockIoConn() *mockIoConn {
	conn := new(mockIoConn)
	conn.readWg.Add(1)
	conn.writeWg.Add(1)
	return conn
}

func (m *mockIoDialer) Dial(address string,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	m.conn = newMockIoConn()
	if m.init != nil {
		m.init(m.conn)
	}
	return m.conn, nil
}

func dialIo(t *testing.T,
	init func(conn *mockIoConn)) (*tarantool.Connection, mockIoDialer) {
	t.Helper()

	dialer := mockIoDialer{
		init: init,
	}
	conn, err := tarantool.Connect("any", tarantool.Opts{
		Dialer:     &dialer,
		Timeout:    1000 * time.Second, // Avoid pings.
		SkipSchema: true,
	})
	require.Nil(t, err)
	require.NotNil(t, conn)

	return conn, dialer
}

func TestConn_Close(t *testing.T) {
	conn, dialer := dialIo(t, nil)
	conn.Close()

	assert.Equal(t, 1, dialer.conn.closeCnt)

	dialer.conn.readWg.Done()
	dialer.conn.writeWg.Done()
}

type stubAddr struct {
	net.Addr
	str string
}

func (a stubAddr) String() string {
	return a.str
}

func TestConn_LocalAddr(t *testing.T) {
	const addr = "any"
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.local = stubAddr{str: addr}
	})
	defer func() {
		dialer.conn.readWg.Done()
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	assert.Equal(t, addr, conn.LocalAddr())
	assert.Equal(t, 1, dialer.conn.localCnt)
}

func TestConn_RemoteAddr(t *testing.T) {
	const addr = "any"
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.remote = stubAddr{str: addr}
	})
	defer func() {
		dialer.conn.readWg.Done()
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	assert.Equal(t, addr, conn.RemoteAddr())
	assert.Equal(t, 1, dialer.conn.remoteCnt)
}

func TestConn_Greeting(t *testing.T) {
	greeting := tarantool.Greeting{
		Version: "any",
	}
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.greeting = greeting
	})
	defer func() {
		dialer.conn.readWg.Done()
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	assert.Equal(t, &greeting, conn.Greeting)
	assert.Equal(t, 1, dialer.conn.greetingCnt)
}

func TestConn_ProtocolInfo(t *testing.T) {
	info := tarantool.ProtocolInfo{
		Auth:    tarantool.ChapSha1Auth,
		Version: 33,
		Features: []tarantool.ProtocolFeature{
			tarantool.ErrorExtensionFeature,
		},
	}
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.info = info
	})
	defer func() {
		dialer.conn.readWg.Done()
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	assert.Equal(t, info, conn.ServerProtocolInfo())
	assert.Equal(t, 1, dialer.conn.infoCnt)
}

func TestConn_ReadWrite(t *testing.T) {
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.read = make(chan struct{})
		conn.written = make(chan struct{})
		conn.writeWgDelay = 1
		conn.readbuf.Write([]byte{
			0xce, 0x00, 0x00, 0x00, 0x0a, // Length.
			0x82, // Header map.
			0x00, 0x00,
			0x01, 0xce, 0x00, 0x00, 0x00, 0x02,
			0x80, // Body map.
		})
		conn.Close()
	})
	defer func() {
		dialer.conn.writeWg.Done()
	}()

	fut := conn.Do(tarantool.NewPingRequest())

	<-dialer.conn.written
	dialer.conn.readWg.Done()
	<-dialer.conn.read
	<-dialer.conn.read

	assert.Equal(t, []byte{
		0xce, 0x00, 0x00, 0x00, 0xa, // Length.
		0x82, // Header map.
		0x00, 0x40,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x02,
		0x80, // Empty map.
	}, dialer.conn.writebuf.Bytes())

	resp, err := fut.Get()
	assert.Nil(t, err)
	assert.NotNil(t, resp)
}
