package tarantool

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnection_rateLimit_tokenReleasedOnRefusedRequest verifies that a
// request refused before it was added to a request list gives its rate limit
// token back.
//
// With RLimitDrop the token is taken at the very beginning of newFuture(), but
// such a request is never passed to markDone(), which is the only place that
// drains the rate limit channel. A leaked token is never returned, so after
// Opts.RateLimit refused requests the connection refuses everything with
// CodeRateLimited, even when it is healthy again.
func TestConnection_rateLimit_tokenReleasedOnRefusedRequest(t *testing.T) {
	const rateLimit = 2

	// Nothing breaks the connection here, so the stub stays idle.
	conn, err := Connect(t.Context(), &stubDialer{}, Opts{
		Timeout:      1000 * time.Second, // Avoid pings.
		SkipSchema:   true,
		RateLimit:    rateLimit,
		RLimitAction: RLimitDrop,
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()

	// Pretend that the connection is broken: requests are refused before
	// they reach a request list.
	atomic.StoreUint32(&conn.state, connDisconnected)

	for i := range rateLimit * 2 {
		_, err := conn.Do(NewPingRequest()).Get()
		require.Error(t, err)
		assert.Falsef(t, errors.Is(err, ErrRateLimited),
			"request %d must not be rate limited: a refused request has to "+
				"give its rate limit token back", i)
		assert.ErrorIsf(t, err, ErrConnectionNotReady, "request %d", i)
	}

	assert.Lenf(t, conn.rlimit, 0,
		"a refused request must not hold a rate limit token")
	assert.Equal(t, int64(0), conn.requestCnt.Load(),
		"a refused request must not be counted as active")
}

// TestConnection_rateLimit_tokenReleasedOnCancelledContext is the same check
// for a request whose context is already done when it reaches newFuture():
// it is not added to a request list either.
func TestConnection_rateLimit_tokenReleasedOnCancelledContext(t *testing.T) {
	const rateLimit = 2

	// Nothing breaks the connection here, so the stub stays idle.
	conn, err := Connect(t.Context(), &stubDialer{}, Opts{
		Timeout:      1000 * time.Second, // Avoid pings.
		SkipSchema:   true,
		RateLimit:    rateLimit,
		RLimitAction: RLimitDrop,
	})
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for i := range rateLimit * 2 {
		_, err := conn.Do(NewPingRequest().Context(ctx)).Get()
		require.Error(t, err)
		assert.Falsef(t, errors.Is(err, ErrRateLimited),
			"request %d must not be rate limited: a request with a done "+
				"context has to give its rate limit token back", i)
		assert.ErrorIsf(t, err, context.Canceled, "request %d", i)
	}

	assert.Lenf(t, conn.rlimit, 0,
		"a request with a done context must not hold a rate limit token")
	assert.Equal(t, int64(0), conn.requestCnt.Load(),
		"a request with a done context must not be counted as active")
}
