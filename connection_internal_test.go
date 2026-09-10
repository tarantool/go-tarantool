package tarantool

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
)

// recordingHandler is a slog.Handler that reports every message it gets over
// a channel.
type recordingHandler struct {
	msgs chan string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	select {
	case h.msgs <- record.Message:
	default:
	}
	return nil
}

func (h *recordingHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }

func (h *recordingHandler) WithGroup(_ string) slog.Handler { return h }

// TestConn_dial_shutdownWatcherFailure checks that a connection stays
// consistent when the "box.shutdown" watcher cannot be registered.
//
// The watcher is registered after the connection is published, because the
// watch request travels over that connection. A failed registration used to
// fail the whole dial, leaving the connection published and its goroutines
// running while the caller reported a failure: connect() then saw conn.c != nil
// and skipped the redial, so runReconnects() reported success and the
// Connected event was never sent.
//
// The registration is failed here the way it can fail in production: the
// request is rate limited on the client, which happens when the connection
// reconnects while its rate limit is saturated.
func TestConn_dial_shutdownWatcherFailure(t *testing.T) {
	dialer := &stubDialer{
		opts: stubDialerOpts{
			info: func(dial int) ProtocolInfo {
				info := ProtocolInfo{
					Auth:    ChapSha1Auth,
					Version: ProtocolVersion(6),
				}
				if dial > 1 {
					// Only the second and later connections report the
					// watchers support, so that the shutdown watcher is
					// registered on a reconnect rather than on the first
					// dial.
					info.Features = []iproto.Feature{
						iproto.IPROTO_FEATURE_WATCHERS,
					}
				}
				return info
			},
		},
	}

	logs := make(chan string, 100)
	notify := make(chan ConnEvent, 100)
	conn, err := Connect(t.Context(), dialer, Opts{
		Timeout:       1000 * time.Second, // Avoid pings and timeouts.
		Reconnect:     10 * time.Millisecond,
		MaxReconnects: 0, // Infinite.
		SkipSchema:    true,
		RateLimit:     1,
		RLimitAction:  RLimitDrop,
		Notify:        notify,
		Logger:        slog.New(&recordingHandler{msgs: logs}),
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()

	require.True(t, conn.ConnectedNow())
	conn.mutex.Lock()
	require.Nil(t, conn.shutdownWatcher,
		"the first connection does not support watchers")
	conn.mutex.Unlock()

	// Saturate the rate limiter, so that the watch request of the shutdown
	// watcher is dropped on the next dial. Nothing releases the slot: there
	// are no requests in flight.
	conn.rlimit <- struct{}{}

	// Break the first connection to trigger a reconnect.
	dialer.breakConn(1)

	require.Eventually(t, func() bool {
		return dialer.conn(2) != nil && conn.ConnectedNow()
	}, time.Second, time.Millisecond, "expected a reconnect")

	// The failed registration is reported, and only reported.
	require.Eventually(t, func() bool {
		for {
			select {
			case msg := <-logs:
				if msg == LogMsgShutdownWatcherFailed {
					return true
				}
			default:
				return false
			}
		}
	}, time.Second, time.Millisecond,
		"expected a warning about the shutdown watcher")

	// The connection is connected and it says so: without the Connected
	// event a user has no way to learn that the reconnect succeeded.
	connected := 0
	require.Eventually(t, func() bool {
		for {
			select {
			case event := <-notify:
				if event.Kind == Connected {
					connected++
					if connected >= 2 {
						return true
					}
				}
			default:
				return false
			}
		}
	}, time.Second, time.Millisecond,
		"expected a Connected event for the reestablished connection")

	assert.True(t, conn.ConnectedNow())

	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	assert.NotNil(t, conn.c, "the connection must stay published")
	// The watcher is not registered, so the next reconnect tries again.
	assert.Nil(t, conn.shutdownWatcher)
	_, ok := conn.watchMap.Load(shutdownEventKey)
	assert.False(t, ok, "a failed registration must leave no watch state")
}
