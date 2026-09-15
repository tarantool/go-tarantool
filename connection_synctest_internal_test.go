//go:build go1.25

package tarantool

import (
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
)

// drainChan returns every value buffered in ch at the moment of the call.
func drainChan[T any](ch <-chan T) []T {
	var values []T
	for {
		select {
		case v := <-ch:
			values = append(values, v)
		default:
			return values
		}
	}
}

// TestConn_dial_shutdownWatcherFailure_synctest is the same check as
// TestConn_dial_shutdownWatcherFailure, run in a synctest bubble: a single
// synctest.Wait() replaces polling, so the assertions run as soon as the
// reconnect has settled and a failure reports every broken invariant at once
// instead of waiting for a timeout on the first one.
//
// The stub dialer never fails, so the reconnect completes without waiting for
// the reconnect interval and the bubble is idle only once it is done.
func TestConn_dial_shutdownWatcherFailure_synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const reconnect = 10 * time.Millisecond

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
						// registered on a reconnect.
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
			Reconnect:     reconnect,
			MaxReconnects: 0, // Infinite.
			SkipSchema:    true,
			RateLimit:     1,
			RLimitAction:  RLimitDrop,
			Notify:        notify,
			Logger:        slog.New(&recordingHandler{msgs: logs}),
		})
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer func() {
			_ = conn.Close()
			// A reconnect loop waiting for its ticker does not watch the
			// connection state. Let it wake up and exit, otherwise a failed
			// assertion ends with a bubble deadlock panic that aborts the
			// whole test binary.
			time.Sleep(2 * reconnect)
		}()

		require.True(t, conn.ConnectedNow())
		conn.mutex.Lock()
		require.Nil(t, conn.shutdownWatcher,
			"the first connection does not support watchers")
		conn.mutex.Unlock()

		// Saturate the rate limiter, so that the watch request of the
		// shutdown watcher is dropped on the next dial.
		conn.rlimit <- struct{}{}

		// Break the first connection and let the reconnect settle.
		dialer.breakConn(1)
		synctest.Wait()

		require.NotNil(t, dialer.conn(2), "expected a reconnect")
		require.True(t, conn.ConnectedNow())
		assert.Contains(t, drainChan(logs), LogMsgShutdownWatcherFailed,
			"expected a warning about the shutdown watcher")

		connected := 0
		for _, event := range drainChan(notify) {
			if event.Kind == Connected {
				connected++
			}
		}
		// One event for the first connection and one for the reconnect.
		assert.Equal(t, 2, connected,
			"expected a Connected event for the reestablished connection")

		conn.mutex.Lock()
		defer conn.mutex.Unlock()
		assert.NotNil(t, conn.c, "the connection must stay published")
		// The watcher is not registered, so the next reconnect tries again.
		assert.Nil(t, conn.shutdownWatcher)
		_, ok := conn.watchMap.Load(shutdownEventKey)
		assert.False(t, ok, "a failed registration must leave no watch state")
	})
}
