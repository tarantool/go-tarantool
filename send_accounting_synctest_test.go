//go:build go1.25

package tarantool

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnection_requestCnt_balancedWhenReconnectCompletesRequest_synctest is
// the same check as
// TestConnection_requestCnt_balancedWhenReconnectCompletesRequest, run in a
// synctest bubble. synctest.Wait() returns only when every other goroutine of
// the bubble is durably blocked, so after it the request that was just sent is
// linked and either waits for its response or is parked on the rate limit
// channel. That replaces both the polling and the sleep that keep the senders
// parking in the send order.
func TestConnection_requestCnt_balancedWhenReconnectCompletesRequest_synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const rateLimit = 2
		const parked = 16

		dialer := &stubDialer{}

		conn, err := Connect(t.Context(), dialer, Opts{
			Timeout:       1000 * time.Second, // Avoid pings.
			Reconnect:     time.Millisecond,
			MaxReconnects: 0, // Infinite.
			SkipSchema:    true,
			Concurrency:   1,
			RateLimit:     rateLimit,
			RLimitAction:  RLimitWait,
		})
		require.NoError(t, err)
		require.NotNil(t, conn)
		// Completes the parked requests when an assertion below fails, so
		// that no goroutine is left blocked when the bubble ends.
		defer func() { _ = conn.Close() }()

		var wg sync.WaitGroup
		for i := range rateLimit + parked {
			wg.Go(func() {
				// The connection answers nothing, so every request is
				// completed by the reconnect below.
				_, _ = conn.Do(NewPingRequest()).Get()
			})

			synctest.Wait()
			require.Equalf(t, i+1, linkedFutures(conn),
				"request %d did not reach the request list", i)
		}

		// Break the connection. Only the first connection is broken, so there
		// is exactly one reconnect, and it completes every linked request with
		// futureList.clear().
		dialer.breakConn(1)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("requests were not completed by the reconnect")
		}

		require.NoError(t, conn.Close())

		assert.Equal(t, int64(0), conn.requestCnt.Load(),
			"every incrementRequestCnt() must be matched by exactly one decrement")
	})
}
