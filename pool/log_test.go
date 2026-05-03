package pool_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/pool"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type cancelDiscoveryHandler struct{}

func (h *cancelDiscoveryHandler) Discovered(_ string, _ *tarantool.Connection, _ pool.Role) error {
	return errors.New("discovery canceled")
}

func (h *cancelDiscoveryHandler) Deactivated(_ string, _ *tarantool.Connection, _ pool.Role) error {
	return nil
}

type failDeactivationHandler struct{}

func (h *failDeactivationHandler) Discovered(_ string, _ *tarantool.Connection, _ pool.Role) error {
	return nil
}

func (h *failDeactivationHandler) Deactivated(
	_ string, _ *tarantool.Connection, _ pool.Role,
) error {
	return errors.New("deactivation failed")
}

type mockIoAddr struct{}

func (a mockIoAddr) Network() string { return "tcp" }
func (a mockIoAddr) String() string  { return "mock:0" }

type mockIoConn struct {
	readbuf bytes.Buffer
	readWg  sync.WaitGroup
	readCnt int
	writeWg sync.WaitGroup
	closeWg sync.WaitGroup
}

func newMockIoConn(response []byte) *mockIoConn {
	c := &mockIoConn{}
	c.readbuf.Write(response)
	c.readWg.Add(1)
	c.writeWg.Add(1)
	c.closeWg.Add(1)
	return c
}

func (m *mockIoConn) Read(b []byte) (int, error) {
	n, err := m.readbuf.Read(b)
	if err != nil {
		m.readWg.Wait()
		n, err = m.readbuf.Read(b)
	}
	m.readCnt++
	return n, err
}

func (m *mockIoConn) Write(_ []byte) (int, error) {
	m.writeWg.Wait()
	return 0, nil
}

func (m *mockIoConn) Flush() error { return nil }

func (m *mockIoConn) Close() error {
	m.readWg.Done()
	m.writeWg.Done()
	m.closeWg.Done()
	return nil
}

func (m *mockIoConn) Greeting() tarantool.Greeting {
	return tarantool.Greeting{}
}

func (m *mockIoConn) ProtocolInfo() tarantool.ProtocolInfo {
	return tarantool.ProtocolInfo{}
}

func (m *mockIoConn) Addr() net.Addr { return mockIoAddr{} }

type mockIoDialer struct {
	conn *mockIoConn
}

func (d *mockIoDialer) Dial(_ context.Context, _ tarantool.DialOpts) (tarantool.Conn, error) {
	return d.conn, nil
}

// unexpectedRequestIdResponse is an IPROTO response with sync=0xDEAD,
// which will never match any registered future.
var unexpectedRequestIdResponse = []byte{
	0xce, 0x00, 0x00, 0x00, 0x0a, // Length = 10.
	0x82,       // Header map (2 entries).
	0x00, 0x00, // IPROTO_REQUEST_TYPE = 0 (OK).
	0x01, 0xce, 0x00, 0x00, 0xde, 0xad, // IPROTO_SYNC = 0xDEAD.
	0x80, // Body: empty map.
}

func TestPoolLogMessageConstants(t *testing.T) {
	msgs := []string{
		pool.LogMsgConnectFailed,
		pool.LogMsgInitWatchersFailed,
		pool.LogMsgStoringConnectionCanceled,
		pool.LogMsgDeactivatingFailed,
		pool.LogMsgStoringConnectionFailed,
		pool.LogMsgReconnectFailed,
		pool.LogMsgReopenFailed,
	}
	for _, msg := range msgs {
		assert.NotEmpty(t, msg, "log message constant must not be empty")
	}
}

func TestPoolLogKeyConstants(t *testing.T) {
	keys := []string{
		pool.LogKeyInstance,
		pool.LogKeyError,
	}
	for _, key := range keys {
		assert.NotEmpty(t, key, "log key constant must not be empty")
	}
}

func TestPoolCustomLogger(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       logger,
	}

	badInstance := pool.Instance{
		Name: "unreachable",
		Dialer: tarantool.NetDialer{
			Address: "127.0.0.1:0",
		},
		Opts: tarantool.Opts{
			Timeout:    500 * time.Millisecond,
			SkipSchema: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, []pool.Instance{badInstance}, poolOpts)
	require.NoError(t, err)
	defer func() { _ = connPool.Close() }()

	output := buf.String()
	require.Contains(t, output, pool.LogMsgConnectFailed)
	require.Contains(t, output, pool.LogKeyInstance)
	require.Contains(t, output, pool.LogKeyError)
}

func TestPoolLoggerWithGroup(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	grouped := logger.WithGroup("myapp")

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       grouped,
	}

	badInstance := pool.Instance{
		Name: "unreachable",
		Dialer: tarantool.NetDialer{
			Address: "127.0.0.1:0",
		},
		Opts: tarantool.Opts{
			Timeout:    500 * time.Millisecond,
			SkipSchema: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, []pool.Instance{badInstance}, poolOpts)
	require.NoError(t, err)
	defer func() { _ = connPool.Close() }()

	output := buf.String()
	require.Contains(t, output, "myapp.tarantool.pool")
}

func TestPoolNilLoggerDiscards(t *testing.T) {
	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       nil,
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, instances, poolOpts)
	require.NoError(t, err)
	defer func() { _ = connPool.Close() }()
}

func TestPoolLoggerStoringConnectionCanceled(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       logger,
		Handler:      &cancelDiscoveryHandler{},
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, instances, poolOpts)
	require.NoError(t, err)
	defer func() { _ = connPool.Close() }()

	output := buf.String()
	require.Contains(t, output, pool.LogMsgStoringConnectionCanceled)
	require.Contains(t, output, pool.LogKeyInstance)
	require.Contains(t, output, pool.LogKeyError)
}

func TestPoolLoggerDeactivatingFailed(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       logger,
		Handler:      &failDeactivationHandler{},
	}

	ctx, cancel := test_helpers.GetPoolConnectContext()
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, instances, poolOpts)
	require.NoError(t, err)

	require.NoError(t, connPool.Close())

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), pool.LogMsgDeactivatingFailed)
	}, 2*time.Second, 50*time.Millisecond)

	output := buf.String()
	require.Contains(t, output, pool.LogKeyInstance)
	require.Contains(t, output, pool.LogKeyError)
}

func TestPoolLoggerInheritedByConnections(t *testing.T) {
	var poolBuf safeBuffer
	poolLogger := slog.New(slog.NewTextHandler(&poolBuf,
		&slog.HandlerOptions{Level: slog.LevelWarn}))

	mockConn := newMockIoConn(unexpectedRequestIdResponse)
	dialer := &mockIoDialer{conn: mockConn}

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       poolLogger,
	}

	mockInstance := pool.Instance{
		Name:   "mock_inherited",
		Dialer: dialer,
		Opts: tarantool.Opts{
			Timeout:    500 * time.Millisecond,
			SkipSchema: true,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, []pool.Instance{mockInstance}, poolOpts)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return strings.Contains(poolBuf.String(), tarantool.LogMsgUnexpectedRequestId)
	}, 2*time.Second, 50*time.Millisecond)

	output := poolBuf.String()
	require.Contains(t, output, tarantool.LogMsgUnexpectedRequestId)
	require.Contains(t, output, "tarantool.")

	require.NoError(t, connPool.Close())
}

func TestPoolLoggerNotInheritedWhenInstanceHasOwn(t *testing.T) {
	var poolBuf, instanceBuf safeBuffer
	poolLogger := slog.New(slog.NewTextHandler(&poolBuf,
		&slog.HandlerOptions{Level: slog.LevelWarn}))
	instanceLogger := slog.New(slog.NewTextHandler(&instanceBuf,
		&slog.HandlerOptions{Level: slog.LevelWarn}))

	mockConn := newMockIoConn(unexpectedRequestIdResponse)
	dialer := &mockIoDialer{conn: mockConn}

	poolOpts := pool.Opts{
		CheckTimeout: 1 * time.Second,
		Logger:       poolLogger,
	}

	mockInstance := pool.Instance{
		Name:   "mock_own_logger",
		Dialer: dialer,
		Opts: tarantool.Opts{
			Timeout:    500 * time.Millisecond,
			SkipSchema: true,
			Logger:     instanceLogger,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	connPool, err := pool.NewWithOpts(ctx, []pool.Instance{mockInstance}, poolOpts)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return strings.Contains(instanceBuf.String(), tarantool.LogMsgUnexpectedRequestId)
	}, 2*time.Second, 50*time.Millisecond)

	instanceOutput := instanceBuf.String()
	poolOutput := poolBuf.String()
	require.Contains(t, instanceOutput, tarantool.LogMsgUnexpectedRequestId)
	require.Contains(t, instanceOutput, "tarantool")
	require.NotContains(t, poolOutput, tarantool.LogMsgUnexpectedRequestId)

	require.NoError(t, connPool.Close())
}
