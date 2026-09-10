package tarantool

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// linkedFutures counts the futures in the request lists of the only shard of
// the connection.
func linkedFutures(conn *Connection) int {
	shard := &conn.shard[0]
	shard.rmut.Lock()
	defer shard.rmut.Unlock()

	n := 0
	requestsLists := []*[requestsMap]futureList{&shard.requests, &shard.requestsWithCtx}
	for _, requests := range requestsLists {
		for pos := range requests {
			for fut := requests[pos].first; fut != nil; fut = fut.next {
				n++
			}
		}
	}
	return n
}

// TestConnection_requestCnt_balancedWhenReconnectCompletesRequest verifies
// that every increment of the active request counter is matched by exactly one
// decrement when a request is completed by a reconnect while send() is still
// working on it.
//
// A future added to a request list is completed by futureList.clear() during a
// reconnect, and clear() calls markDone() for it, which decrements the counter.
// send() must not decrement the counter for such a future once more: the
// counter would drift below zero and CloseGraceful(), whose drain loop waits
// for exactly zero, would never return.
//
// The window is normally a few nanoseconds wide, so the test widens it with
// RLimitWait: a request that is already in a request list parks in newFuture()
// waiting for a rate limit token, and the reconnect completes it while it
// waits. A single shard keeps the completion order equal to the send order,
// which is what keeps the parked senders and markDone() in step.
func TestConnection_requestCnt_balancedWhenReconnectCompletesRequest(t *testing.T) {
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

	var wg sync.WaitGroup
	for i := range rateLimit + parked {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The connection answers nothing, so every request is
			// completed by the reconnect below.
			_, _ = conn.Do(NewPingRequest()).Get()
		}()

		// Requests have to reach the request list one by one: the first
		// rateLimit of them take the rate limit tokens, the rest park in
		// newFuture() in the same order.
		require.Eventuallyf(t, func() bool {
			return linkedFutures(conn) == i+1
		}, time.Second, time.Millisecond, "request %d did not reach the request list", i)
		time.Sleep(2 * time.Millisecond)
	}

	// Break the connection. The reader fails, and the reconnect completes
	// every linked request with futureList.clear(). Only the first
	// connection is broken, so there is exactly one reconnect.
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
}
