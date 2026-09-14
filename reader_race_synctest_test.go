//go:build go1.25

package tarantool_test

import (
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
)

// TestConn_reader_race_on_reconnect_synctest is the same check as
// TestConn_reader_race_on_reconnect, run on the fake clock of a synctest
// bubble instead of a real-time window.
//
// Every reader parks on its wait group after a fixed number of reads, with
// most of its data still unread, which lets the bubble idle and the writer's
// Flush() timer fire. The writer-initiated reconnect closes the previous
// connection, which releases the parked reader, and starts the next reader,
// so the previous reader drains the rest of its data while the next one is
// already running. The race detector reports a missing happens-before
// relation, not a real-time overlap, so a fake clock does not hide the race.
func TestConn_reader_race_on_reconnect_synctest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Larger than the number of Write() calls a single connection can
		// make, so that the writer never blocks on its wait group.
		const noWgWait = 1 << 30
		// A reader parks after that many reads, far from the end of
		// readerRaceStream.
		const parkAfter = 1000
		const flushWait = 2 * time.Millisecond

		var dialCount atomic.Uint32
		dialer := mockIoDialer{
			init: func(conn *mockIoConn) {
				dialCount.Add(1)
				conn.readbuf.Write(readerRaceStream)
				conn.readWgDelay = parkAfter
				conn.writeWgDelay = noWgWait
				// Close() releases the parked reader.
				conn.wgDoneOnClose = true
				// Fail the writer once the bubble idles, so that
				// reconnect() starts a new reader while this one still
				// has data to drain.
				conn.flushWait = flushWait
				conn.flushErr = io.ErrClosedPipe
			},
		}

		conn, err := tarantool.Connect(t.Context(), &dialer, tarantool.Opts{
			Timeout:       1000 * time.Second, // Avoid pings.
			Reconnect:     1 * time.Millisecond,
			MaxReconnects: 0, // Infinite.
			SkipSchema:    true,
		})
		require.NoError(t, err)
		require.NotNil(t, conn)
		defer func() {
			// Closes the connection when an assertion fails and is a no-op
			// after the Close() below. The last writer still sleeps in
			// Flush(): let it wake up and exit, a goroutine left blocked
			// makes the bubble panic.
			_ = conn.Close()
			time.Sleep(2 * flushWait)
		}()

		// Every reconnect cycle takes exactly one flushWait of fake time.
		time.Sleep(5 * flushWait)

		require.NoError(t, conn.Close())

		assert.Greater(t, dialCount.Load(), uint32(2),
			"expected several reconnects, so that readers overlap")
	})
}
