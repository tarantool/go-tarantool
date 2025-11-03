package tarantool_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

type mockErrorDialer struct {
	err error
}

func (m mockErrorDialer) Dial(ctx context.Context,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	return nil, m.err
}

func TestDialer_Dial_error(t *testing.T) {
	const errMsg = "any msg"
	dialer := mockErrorDialer{
		err: errors.New(errMsg),
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	assert.Nil(t, conn)
	assert.ErrorContains(t, err, errMsg)
}

type mockPassedDialer struct {
	ctx  context.Context
	opts tarantool.DialOpts
}

func (m *mockPassedDialer) Dial(ctx context.Context,
	opts tarantool.DialOpts) (tarantool.Conn, error) {
	m.opts = opts
	if ctx != m.ctx {
		return nil, errors.New("wrong context")
	}
	return nil, errors.New("does not matter")
}

func TestDialer_Dial_passedOpts(t *testing.T) {
	dialer := &mockPassedDialer{}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	dialer.ctx = ctx

	dialOpts := tarantool.DialOpts{
		IoTimeout: opts.Timeout,
	}

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{
		Timeout: opts.Timeout,
	})

	assert.Nil(t, conn)
	assert.NotNil(t, err)
	assert.NotEqual(t, err.Error(), "wrong context")
	assert.Equal(t, dialOpts, dialer.opts)
}

type mockIoConn struct {
	addr net.Addr
	// Addr calls count.
	addrCnt int
	// Sends an event on Read()/Write()/Flush().
	read, written chan struct{}
	// Read()/Write() buffers.
	readbuf, writebuf bytes.Buffer
	// Calls readWg/writeWg.Wait() in Read()/Flush().
	readWg, writeWg sync.WaitGroup
	// wgDoneOnClose call Done() on the wait groups on Close().
	wgDoneOnClose bool
	// How many times to wait before a wg.Wait() call.
	readWgDelay, writeWgDelay int
	// Write()/Read()/Flush()/Close() calls count.
	writeCnt, readCnt, flushCnt, closeCnt int
	// Greeting()/ProtocolInfo() calls count.
	greetingCnt, infoCnt int
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

	if ret != 0 && m.read != nil {
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
	if m.wgDoneOnClose {
		m.readWg.Done()
		m.writeWg.Done()
		m.wgDoneOnClose = false
	}

	m.closeCnt++
	return nil
}

func (m *mockIoConn) Greeting() tarantool.Greeting {
	m.greetingCnt++
	return m.greeting
}

func (m *mockIoConn) ProtocolInfo() tarantool.ProtocolInfo {
	m.infoCnt++
	return m.info
}

func (m *mockIoConn) Addr() net.Addr {
	m.addrCnt++
	return m.addr
}

type mockIoDialer struct {
	init func(conn *mockIoConn)
	conn *mockIoConn
}

func newMockIoConn() *mockIoConn {
	conn := new(mockIoConn)
	conn.readWg.Add(1)
	conn.writeWg.Add(1)
	conn.wgDoneOnClose = true
	return conn
}

func (m *mockIoDialer) Dial(ctx context.Context, opts tarantool.DialOpts) (tarantool.Conn, error) {
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
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := tarantool.Connect(ctx, &dialer,
		tarantool.Opts{
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
}

type stubAddr struct {
	str string
}

func (a stubAddr) String() string {
	return a.str
}

func (a stubAddr) Network() string {
	return "stub"
}

func TestConn_Addr(t *testing.T) {
	const addr = "any"
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.addr = stubAddr{str: addr}
	})
	defer func() {
		conn.Close()
	}()

	assert.Equal(t, addr, conn.Addr().String())
	assert.Equal(t, 1, dialer.conn.addrCnt)
}

func TestConn_Greeting(t *testing.T) {
	greeting := tarantool.Greeting{
		Version: "any",
		Salt:    "salt",
	}
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.greeting = greeting
	})
	defer func() {
		conn.Close()
	}()

	assert.Equal(t, &greeting, conn.Greeting)
	assert.Equal(t, 1, dialer.conn.greetingCnt)
}

func TestConn_ProtocolInfo(t *testing.T) {
	info := tarantool.ProtocolInfo{
		Auth:    tarantool.ChapSha1Auth,
		Version: 33,
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_ERROR_EXTENSION,
		},
	}
	conn, dialer := dialIo(t, func(conn *mockIoConn) {
		conn.info = info
	})
	defer func() {
		conn.Close()
	}()

	assert.Equal(t, info, conn.ProtocolInfo())
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
		conn.wgDoneOnClose = false
	})
	defer func() {
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	fut := conn.Do(tarantool.NewPingRequest())

	<-dialer.conn.written
	dialer.conn.written = nil

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

	_, err := fut.Get()
	assert.Nil(t, err)
}

func TestConn_ContextCancel(t *testing.T) {
	dialer := tarantool.NetDialer{Address: "127.0.0.1:8080"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.Nil(t, conn)
	assert.NotNil(t, err)
	assert.Truef(t, strings.Contains(err.Error(), "operation was canceled"),
		fmt.Sprintf("unexpected error, expected to contain %s, got %v",
			"operation was canceled", err))
}

func genSalt() [64]byte {
	salt := [64]byte{}
	for i := 0; i < 44; i++ {
		salt[i] = 'a'
	}
	return salt
}

var (
	testDialUser    = "test"
	testDialPass    = "test"
	testDialVersion = [64]byte{'t', 'e', 's', 't'}

	// Salt with end zeros.
	testDialSalt = genSalt()

	idRequestExpected = []byte{
		0xce, 0x00, 0x00, 0x00, 31, // Length.
		0x82, // Header map.
		0x00, 0x49,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,

		0x82, // Data map.
		0x54,
		0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, // Version.
		0x55,
		0x99,                                                 //  Fixed arrау with 9 elements.
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x0b, 0x0c, // Features (9 elements).
	}

	idResponseTyped = tarantool.ProtocolInfo{
		Version:  6,
		Features: []iproto.Feature{iproto.Feature(1), iproto.Feature(21)},
		Auth:     tarantool.ChapSha1Auth,
	}

	idResponse = []byte{
		0xce, 0x00, 0x00, 0x00, 37, // Length.
		0x83, // Header map.
		0x00, 0xce, 0x00, 0x00, 0x00, 0x00,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,
		0x05, 0xce, 0x00, 0x00, 0x00, 0x61,

		0x83, // Data map.
		0x54,
		0x06, // Version.
		0x55,
		0x92, 0x01, 0x15, // Features.
		0x5b,
		0xa9, 'c', 'h', 'a', 'p', '-', 's', 'h', 'a', '1',
	}

	idResponseNotSupported = []byte{
		0xce, 0x00, 0x00, 0x00, 25, // Length.
		0x83, // Header map.
		0x00, 0xce, 0x00, 0x00, 0x80, 0x30,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,
		0x05, 0xce, 0x00, 0x00, 0x00, 0x61,
		0x81,
		0x31,
		0xa3, 'e', 'r', 'r',
	}

	authRequestExpectedChapSha1 = []byte{
		0xce, 0x00, 0x00, 0x00, 57, // Length.
		0x82, // Header map.
		0x00, 0x07,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,

		0x82, // Data map.
		0xce, 0x00, 0x00, 0x00, 0x23,
		0xa4, 't', 'e', 's', 't', // Login.
		0xce, 0x00, 0x00, 0x00, 0x21,
		0x92, // Tuple.
		0xa9, 'c', 'h', 'a', 'p', '-', 's', 'h', 'a', '1',

		// Scramble.
		0xb4, 0x1b, 0xd4, 0x20, 0x45, 0x73, 0x22,
		0xcf, 0xab, 0x05, 0x03, 0xf3, 0x89, 0x4b,
		0xfe, 0xc7, 0x24, 0x5a, 0xe6, 0xe8, 0x31,
	}

	authRequestExpectedPapSha256 = []byte{
		0xce, 0x00, 0x00, 0x00, 0x2a, // Length.
		0x82, // Header map.
		0x00, 0x07,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,

		0x82, // Data map.
		0xce, 0x00, 0x00, 0x00, 0x23,
		0xa4, 't', 'e', 's', 't', // Login.
		0xce, 0x00, 0x00, 0x00, 0x21,
		0x92, // Tuple.
		0xaa, 'p', 'a', 'p', '-', 's', 'h', 'a', '2', '5', '6',
		0xa4, 't', 'e', 's', 't',
	}

	okResponse = []byte{
		0xce, 0x00, 0x00, 0x00, 19, // Length.
		0x83, // Header map.
		0x00, 0xce, 0x00, 0x00, 0x00, 0x00,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,
		0x05, 0xce, 0x00, 0x00, 0x00, 0x61,
	}

	errResponse = []byte{0xce}
)

type testDialOpts struct {
	name                 string
	wantErr              bool
	expectedErr          string
	expectedProtocolInfo tarantool.ProtocolInfo

	// These options configure the behavior of the server.
	isErrGreeting   bool
	isErrId         bool
	isIdUnsupported bool
	isErrAuth       bool
	isEmptyAuth     bool
}

type dialServerActual struct {
	IdRequest   []byte
	AuthRequest []byte
}

func testDialAccept(opts testDialOpts, l net.Listener) chan dialServerActual {
	ch := make(chan dialServerActual, 1)

	go func() {
		client, err := l.Accept()
		if err != nil {
			return
		}
		defer client.Close()
		if opts.isErrGreeting {
			client.Write(errResponse)
			return
		} else {
			// Write greeting.
			client.Write(testDialVersion[:])
			client.Write(testDialSalt[:])
		}

		// Read Id request.
		idRequestActual := make([]byte, len(idRequestExpected))
		client.Read(idRequestActual)

		// Make Id response.
		if opts.isErrId {
			client.Write(errResponse)
		} else if opts.isIdUnsupported {
			client.Write(idResponseNotSupported)
		} else {
			client.Write(idResponse)
		}

		// Read Auth request.
		authRequestExpected := authRequestExpectedChapSha1
		if opts.isEmptyAuth {
			authRequestExpected = []byte{}
		}
		authRequestActual := make([]byte, len(authRequestExpected))
		client.Read(authRequestActual)

		// Make Auth response.
		if opts.isErrAuth {
			client.Write(errResponse)
		} else {
			client.Write(okResponse)
		}
		ch <- dialServerActual{
			IdRequest:   idRequestActual,
			AuthRequest: authRequestActual,
		}
	}()

	return ch
}

func testDialer(t *testing.T, l net.Listener, dialer tarantool.Dialer,
	opts testDialOpts) {
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	ch := testDialAccept(opts, l)
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{
		IoTimeout: time.Second * 2,
	})
	if opts.wantErr {
		require.Error(t, err)
		require.Contains(t, err.Error(), opts.expectedErr)
		return
	}
	require.NoError(t, err)
	require.Equal(t, opts.expectedProtocolInfo, conn.ProtocolInfo())
	require.Equal(t, testDialVersion[:], []byte(conn.Greeting().Version))
	require.Equal(t, testDialSalt[:44], []byte(conn.Greeting().Salt))

	actual := <-ch
	require.Equal(t, idRequestExpected, actual.IdRequest)

	authRequestExpected := authRequestExpectedChapSha1
	if opts.isEmptyAuth {
		authRequestExpected = []byte{}
	}
	require.Equal(t, authRequestExpected, actual.AuthRequest)
	conn.Close()
}

func TestNetDialer_Dial(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	dialer := tarantool.NetDialer{
		Address:  l.Addr().String(),
		User:     testDialUser,
		Password: testDialPass,
	}
	cases := []testDialOpts{
		{
			name:                 "all is ok",
			expectedProtocolInfo: idResponseTyped.Clone(),
		},
		{
			name:                 "id request unsupported",
			expectedProtocolInfo: tarantool.ProtocolInfo{},
			isIdUnsupported:      true,
		},
		{
			name:          "greeting response error",
			wantErr:       true,
			expectedErr:   "failed to read greeting",
			isErrGreeting: true,
		},
		{
			name:        "id response error",
			wantErr:     true,
			expectedErr: "failed to identify",
			isErrId:     true,
		},
		{
			name:        "auth response error",
			wantErr:     true,
			expectedErr: "failed to authenticate",
			isErrAuth:   true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			testDialer(t, l, dialer, tc)
		})
	}
}

func TestNetDialer_Dial_hang_connection(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	dialer := tarantool.NetDialer{
		Address: l.Addr().String(),
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	require.Nil(t, conn)
	require.Error(t, err, context.DeadlineExceeded)
}

func TestNetDialer_Dial_requirements(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	dialer := tarantool.NetDialer{
		Address:  l.Addr().String(),
		User:     testDialUser,
		Password: testDialPass,
		RequiredProtocolInfo: tarantool.ProtocolInfo{
			Features: []iproto.Feature{42},
		},
	}
	testDialAccept(testDialOpts{}, l)
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if err == nil {
		conn.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid server protocol")
}

func TestFdDialer_Dial(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	addr := l.Addr().String()

	cases := []testDialOpts{
		{
			name:                 "all is ok",
			expectedProtocolInfo: idResponseTyped.Clone(),
			isEmptyAuth:          true,
		},
		{
			name:                 "id request unsupported",
			expectedProtocolInfo: tarantool.ProtocolInfo{},
			isIdUnsupported:      true,
			isEmptyAuth:          true,
		},
		{
			name:          "greeting response error",
			wantErr:       true,
			expectedErr:   "failed to read greeting",
			isErrGreeting: true,
		},
		{
			name:        "id response error",
			wantErr:     true,
			expectedErr: "failed to identify",
			isErrId:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sock, err := net.Dial("tcp", addr)
			require.NoError(t, err)
			defer sock.Close()

			// It seems that the file descriptor is not always fully ready
			// after the connection is created. These lines help to avoid the
			// "bad file descriptor" errors.
			//
			// We already tried to use the SyscallConn(), but it has the same
			// issue.
			time.Sleep(time.Millisecond)
			sock.(*net.TCPConn).SetLinger(0)

			f, err := sock.(*net.TCPConn).File()
			require.NoError(t, err)
			defer f.Close()

			dialer := tarantool.FdDialer{
				Fd: f.Fd(),
			}
			testDialer(t, l, dialer, tc)
		})
	}
}

func TestFdDialer_Dial_requirements(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	addr := l.Addr().String()

	sock, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer sock.Close()

	// It seems that the file descriptor is not always fully ready after the
	// connection is created. These lines help to avoid the
	// "bad file descriptor" errors.
	//
	// We already tried to use the SyscallConn(), but it has the same
	// issue.
	time.Sleep(time.Millisecond)
	sock.(*net.TCPConn).SetLinger(0)

	f, err := sock.(*net.TCPConn).File()
	require.NoError(t, err)
	defer f.Close()

	dialer := tarantool.FdDialer{
		Fd: f.Fd(),
		RequiredProtocolInfo: tarantool.ProtocolInfo{
			Features: []iproto.Feature{42},
		},
	}

	testDialAccept(testDialOpts{}, l)
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if err == nil {
		conn.Close()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid server protocol")
}

func TestAuthDialer_Dial_DialerError(t *testing.T) {
	dialer := tarantool.AuthDialer{
		Dialer: mockErrorDialer{
			err: fmt.Errorf("some error"),
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NotNil(t, err)
	assert.EqualError(t, err, "some error")
}

func TestAuthDialer_Dial_NoSalt(t *testing.T) {
	dialer := tarantool.AuthDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.greeting = tarantool.Greeting{
					Salt: "",
				}
			},
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "an invalid connection without salt")
	if conn != nil {
		conn.Close()
		t.Errorf("connection is not nil")
	}
}

func TestConn_AuthDialer_hang_connection(t *testing.T) {
	salt := fmt.Sprintf("%s", testDialSalt)
	salt = base64.StdEncoding.EncodeToString([]byte(salt))
	mock := &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.greeting.Salt = salt
			conn.readWgDelay = 0
			conn.writeWgDelay = 0
		},
	}
	dialer := tarantool.AuthDialer{
		Dialer:   mock,
		Username: "test",
		Password: "test",
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := tarantool.Connect(ctx, &dialer,
		tarantool.Opts{
			Timeout:    1000 * time.Second, // Avoid pings.
			SkipSchema: true,
		})

	require.Nil(t, conn)
	require.Error(t, err, context.DeadlineExceeded)
	require.Equal(t, mock.conn.writeCnt, 1)
	require.Equal(t, mock.conn.readCnt, 0)
	require.Greater(t, mock.conn.closeCnt, 1)
}

func TestAuthDialer_Dial(t *testing.T) {
	salt := fmt.Sprintf("%s", testDialSalt)
	salt = base64.StdEncoding.EncodeToString([]byte(salt))
	dialer := mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.greeting.Salt = salt
			conn.writeWgDelay = 1
			conn.readWgDelay = 2
			conn.readbuf.Write(okResponse)
			conn.wgDoneOnClose = false
		},
	}
	defer func() {
		dialer.conn.writeWg.Done()
	}()

	authDialer := tarantool.AuthDialer{
		Dialer:   &dialer,
		Username: "test",
		Password: "test",
	}
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := authDialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, authRequestExpectedChapSha1[:41], dialer.conn.writebuf.Bytes()[:41])
}

func TestAuthDialer_Dial_PapSha256Auth(t *testing.T) {
	salt := fmt.Sprintf("%s", testDialSalt)
	salt = base64.StdEncoding.EncodeToString([]byte(salt))
	dialer := mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.greeting.Salt = salt
			conn.writeWgDelay = 1
			conn.readWgDelay = 2
			conn.readbuf.Write(okResponse)
			conn.wgDoneOnClose = false
		},
	}
	defer func() {
		dialer.conn.writeWg.Done()
	}()

	authDialer := tarantool.AuthDialer{
		Dialer:   &dialer,
		Username: "test",
		Password: "test",
		Auth:     tarantool.PapSha256Auth,
	}
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := authDialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, authRequestExpectedPapSha256[:41], dialer.conn.writebuf.Bytes()[:41])
}

func TestProtocolDialer_Dial_DialerError(t *testing.T) {
	dialer := tarantool.ProtocolDialer{
		Dialer: mockErrorDialer{
			err: fmt.Errorf("some error"),
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NotNil(t, err)
	assert.EqualError(t, err, "some error")
}

func TestConn_ProtocolDialer_hang_connection(t *testing.T) {
	mock := &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.info = tarantool.ProtocolInfo{Version: 1}
			conn.readWgDelay = 0
			conn.writeWgDelay = 0
		},
	}
	dialer := tarantool.ProtocolDialer{
		Dialer: mock,
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := tarantool.Connect(ctx, &dialer,
		tarantool.Opts{
			Timeout:    1000 * time.Second, // Avoid pings.
			SkipSchema: true,
		})

	require.Nil(t, conn)
	require.Error(t, err, context.DeadlineExceeded)
	require.Equal(t, mock.conn.writeCnt, 1)
	require.Equal(t, mock.conn.readCnt, 0)
	require.Greater(t, mock.conn.closeCnt, 1)
}

func TestProtocolDialer_Dial_IdentifyFailed(t *testing.T) {
	dialer := tarantool.ProtocolDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.info = tarantool.ProtocolInfo{Version: 1}
				conn.writeWgDelay = 1
				conn.readWgDelay = 2
				conn.readbuf.Write(errResponse)
			},
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "failed to identify")
	if conn != nil {
		conn.Close()
		t.Errorf("connection is not nil")
	}
}

func TestProtocolDialer_Dial_WrongInfo(t *testing.T) {
	dialer := tarantool.ProtocolDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.info = tarantool.ProtocolInfo{Version: 1}
				conn.writeWgDelay = 1
				conn.readWgDelay = 2
				conn.readbuf.Write(idResponse)
			},
		},
		RequiredProtocolInfo: validProtocolInfo,
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "invalid server protocol")
	if conn != nil {
		conn.Close()
		t.Errorf("connection is not nil")
	}
}

func TestProtocolDialer_Dial(t *testing.T) {
	protoInfo := tarantool.ProtocolInfo{
		Auth:     tarantool.ChapSha1Auth,
		Version:  6,
		Features: []iproto.Feature{0x01, 0x15},
	}

	dialer := tarantool.ProtocolDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.info = tarantool.ProtocolInfo{Version: 1}
				conn.writeWgDelay = 1
				conn.readWgDelay = 2
				conn.readbuf.Write(idResponse)
			},
		},
		RequiredProtocolInfo: protoInfo,
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, protoInfo, conn.ProtocolInfo())
}

func TestGreetingDialer_Dial_DialerError(t *testing.T) {
	dialer := tarantool.GreetingDialer{
		Dialer: mockErrorDialer{
			err: fmt.Errorf("some error"),
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NotNil(t, err)
	assert.EqualError(t, err, "some error")
}

func TestConn_GreetingDialer_hang_connection(t *testing.T) {
	mock := &mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.readWgDelay = 0
		},
	}
	dialer := tarantool.GreetingDialer{
		Dialer: mock,
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := tarantool.Connect(ctx, &dialer,
		tarantool.Opts{
			Timeout:    1000 * time.Second, // Avoid pings.
			SkipSchema: true,
		})

	require.Nil(t, conn)
	require.Error(t, err, context.DeadlineExceeded)
	require.Equal(t, mock.conn.readCnt, 1)
	require.Greater(t, mock.conn.closeCnt, 1)
}

func TestGreetingDialer_Dial_GreetingFailed(t *testing.T) {
	dialer := tarantool.GreetingDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.writeWgDelay = 1
				conn.readWgDelay = 2
				conn.readbuf.Write(errResponse)
			},
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})

	assert.NotNil(t, err)
	assert.ErrorContains(t, err, "failed to read greeting")
	if conn != nil {
		conn.Close()
		t.Errorf("connection is not nil")
	}
}

func TestGreetingDialer_Dial(t *testing.T) {
	dialer := tarantool.GreetingDialer{
		Dialer: &mockIoDialer{
			init: func(conn *mockIoConn) {
				conn.info = tarantool.ProtocolInfo{Version: 1}
				conn.writeWgDelay = 1
				conn.readWgDelay = 3
				conn.readbuf.Write(append(testDialVersion[:], testDialSalt[:]...))
				conn.readbuf.Write(idResponse)
			},
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if conn != nil {
		conn.Close()
	}

	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.Equal(t, string(testDialVersion[:]), conn.Greeting().Version)
	assert.Equal(t, string(testDialSalt[:44]), conn.Greeting().Salt)
}
