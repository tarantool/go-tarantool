package tarantool_test

import (
	"bytes"
	"context"
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
	"github.com/tarantool/go-openssl"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
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

	dialer.conn.readWg.Done()
	dialer.conn.writeWg.Done()
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
		dialer.conn.readWg.Done()
		dialer.conn.writeWg.Done()
		conn.Close()
	}()

	assert.Equal(t, addr, conn.Addr().String())
	assert.Equal(t, 1, dialer.conn.addrCnt)
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
		Features: []iproto.Feature{
			iproto.IPROTO_FEATURE_ERROR_EXTENSION,
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
		0xce, 0x00, 0x00, 0x00, 29, // Length.
		0x82, // Header map.
		0x00, 0x49,
		0x01, 0xce, 0x00, 0x00, 0x00, 0x00,

		0x82, // Data map.
		0x54,
		0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, // Version.
		0x55,
		0x97, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, // Features.
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
	isPapSha256Auth bool
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
		if opts.isPapSha256Auth {
			authRequestExpected = authRequestExpectedPapSha256
		} else if opts.isEmptyAuth {
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

	actual := <-ch
	require.Equal(t, idRequestExpected, actual.IdRequest)

	authRequestExpected := authRequestExpectedChapSha1
	if opts.isPapSha256Auth {
		authRequestExpected = authRequestExpectedPapSha256
	} else if opts.isEmptyAuth {
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
			name: "id request unsupported",
			// Dialer sets auth.
			expectedProtocolInfo: tarantool.ProtocolInfo{Auth: tarantool.ChapSha1Auth},
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

func createSslListener(t *testing.T, opts tarantool.SslTestOpts) net.Listener {
	ctx, err := tarantool.SslCreateContext(opts)
	require.NoError(t, err)
	l, err := openssl.Listen("tcp", "127.0.0.1:0", ctx.(*openssl.Ctx))
	require.NoError(t, err)
	return l
}

func TestOpenSslDialer_Dial_basic(t *testing.T) {
	l := createSslListener(t, tarantool.SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
	})

	defer l.Close()
	addr := l.Addr().String()

	dialer := tarantool.OpenSslDialer{
		Address:  addr,
		User:     testDialUser,
		Password: testDialPass,
	}

	cases := []testDialOpts{
		{
			name:                 "all is ok",
			expectedProtocolInfo: idResponseTyped.Clone(),
		},
		{
			name: "id request unsupported",
			// Dialer sets auth.
			expectedProtocolInfo: tarantool.ProtocolInfo{Auth: tarantool.ChapSha1Auth},
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

func TestOpenSslDialer_Dial_requirements(t *testing.T) {
	l := createSslListener(t, tarantool.SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
	})

	defer l.Close()
	addr := l.Addr().String()

	dialer := tarantool.OpenSslDialer{
		Address:  addr,
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

func TestOpenSslDialer_Dial_papSha256Auth(t *testing.T) {
	l := createSslListener(t, tarantool.SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
	})

	defer l.Close()
	addr := l.Addr().String()

	dialer := tarantool.OpenSslDialer{
		Address:  addr,
		User:     testDialUser,
		Password: testDialPass,
		Auth:     tarantool.PapSha256Auth,
	}

	protocol := idResponseTyped.Clone()
	protocol.Auth = tarantool.PapSha256Auth

	testDialer(t, l, dialer, testDialOpts{
		expectedProtocolInfo: protocol,
		isPapSha256Auth:      true,
	})
}

func TestOpenSslDialer_Dial_opts(t *testing.T) {
	for _, test := range sslTests {
		t.Run(test.name, func(t *testing.T) {
			l := createSslListener(t, test.serverOpts)
			defer l.Close()
			addr := l.Addr().String()

			dialer := tarantool.OpenSslDialer{
				Address:         addr,
				User:            testDialUser,
				Password:        testDialPass,
				SslKeyFile:      test.clientOpts.KeyFile,
				SslCertFile:     test.clientOpts.CertFile,
				SslCaFile:       test.clientOpts.CaFile,
				SslCiphers:      test.clientOpts.Ciphers,
				SslPassword:     test.clientOpts.Password,
				SslPasswordFile: test.clientOpts.PasswordFile,
			}
			testDialer(t, l, dialer, testDialOpts{
				wantErr:              !test.ok,
				expectedProtocolInfo: idResponseTyped.Clone(),
			})
		})
	}
}

func TestOpenSslDialer_Dial_ctx_cancel(t *testing.T) {
	serverOpts := tarantool.SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
		CaFile:   "testdata/ca.crt",
		Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
	}
	clientOpts := tarantool.SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
		CaFile:   "testdata/ca.crt",
		Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
	}

	l := createSslListener(t, serverOpts)
	defer l.Close()
	addr := l.Addr().String()
	testDialAccept(testDialOpts{}, l)

	dialer := tarantool.OpenSslDialer{
		Address:         addr,
		User:            testDialUser,
		Password:        testDialPass,
		SslKeyFile:      clientOpts.KeyFile,
		SslCertFile:     clientOpts.CertFile,
		SslCaFile:       clientOpts.CaFile,
		SslCiphers:      clientOpts.Ciphers,
		SslPassword:     clientOpts.Password,
		SslPasswordFile: clientOpts.PasswordFile,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := dialer.Dial(ctx, tarantool.DialOpts{})
	if err == nil {
		conn.Close()
	}
	require.Error(t, err)
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
			f, err := sock.(*net.TCPConn).File()
			require.NoError(t, err)
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
	f, err := sock.(*net.TCPConn).File()
	require.NoError(t, err)
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
