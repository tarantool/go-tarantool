package tarantool_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/tarantool/go-tarantool/v3"
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

func TestLogMessageConstants(t *testing.T) {
	msgs := []string{
		LogMsgReconnectFailed,
		LogMsgLastReconnectFailed,
		LogMsgUnexpectedRequestId,
		LogMsgWatchEventReadFailed,
		LogMsgPushUnsupported,
	}
	for _, msg := range msgs {
		assert.NotEmpty(t, msg, "log message constant must not be empty")
	}
}

func TestLogKeyConstants(t *testing.T) {
	keys := []string{
		LogKeyAttempt,
		LogKeyMaxAttempts,
		LogKeyError,
		LogKeyRequestId,
		LogKeyAddress,
	}
	for _, key := range keys {
		assert.NotEmpty(t, key, "log key constant must not be empty")
	}
}

func TestConnectionCustomLogger(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	testOpts := opts
	testOpts.Logger = logger

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewCallRequest("push_func").Args([]any{1})).Get()
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, LogMsgPushUnsupported)
	require.Contains(t, output, LogKeyRequestId)
}

func TestConnectionLoggerWithGroup(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	grouped := logger.WithGroup("myapp")

	testOpts := opts
	testOpts.Logger = grouped

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewCallRequest("push_func").Args([]any{1})).Get()
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "myapp.tarantool")
	require.Contains(t, output, LogMsgPushUnsupported)
}

func TestConnectionNilLoggerDiscards(t *testing.T) {
	testOpts := opts
	testOpts.Logger = nil

	conn := test_helpers.ConnectWithValidation(t, dialer, testOpts)
	defer func() { _ = conn.Close() }()

	_, err := conn.Do(NewCallRequest("push_func").Args([]any{1})).Get()
	require.NoError(t, err)
}

func TestConnectionUnexpectedRequestId(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf,
		&slog.HandlerOptions{Level: slog.LevelWarn}))

	dialer := mockIoDialer{
		init: func(conn *mockIoConn) {
			conn.read = make(chan struct{})
			conn.written = make(chan struct{})
			conn.writeWgDelay = 1
			conn.readWgDelay = 2
			conn.readbuf.Write([]byte{
				0xce, 0x00, 0x00, 0x00, 0x0a,
				0x82,
				0x00, 0x00,
				0x01, 0xce, 0x00, 0x00, 0xde, 0xad,
				0x80,
			})
			conn.wgDoneOnClose = false
		},
	}

	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()

	conn, err := Connect(ctx, &dialer, Opts{
		Timeout:    1000 * time.Second,
		SkipSchema: true,
		Logger:     logger,
	})
	require.NoError(t, err)

	conn.Do(NewPingRequest())

	<-dialer.conn.written
	dialer.conn.written = nil

	<-dialer.conn.read
	<-dialer.conn.read

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), LogMsgUnexpectedRequestId)
	}, 2*time.Second, 50*time.Millisecond)

	output := buf.String()
	require.Contains(t, output, LogKeyRequestId)

	dialer.conn.writeWg.Done()
	_ = conn.Close()
}

func TestConnectionErrorAttributeFormat(t *testing.T) {
	var buf safeBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	reconnectOpts := Opts{
		Timeout:       500 * time.Millisecond,
		Reconnect:     100 * time.Millisecond,
		MaxReconnects: 1,
		Logger:        logger,
		SkipSchema:    true,
	}

	_, err := Connect(context.Background(), NetDialer{
		Address: "127.0.0.1:0",
	}, reconnectOpts)
	require.Error(t, err)

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), LogMsgReconnectFailed)
	}, 3*time.Second, 50*time.Millisecond)

	output := buf.String()
	assert.Contains(t, output, LogKeyError)
	assert.Contains(t, output, LogKeyAttempt)
	assert.Contains(t, output, LogKeyMaxAttempts)

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), LogMsgLastReconnectFailed)
	}, 3*time.Second, 50*time.Millisecond)

	output = buf.String()
	assert.Contains(t, output, LogKeyError)
	assert.Contains(t, output, LogKeyAddress)
}
