package tarantool_test

import (
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
)

// readerRaceResponse is a complete IPROTO response with request ID 2. There is
// no future with such an ID in the test below, so the connection just logs it
// and keeps reading, which is exactly what is needed to hold a reader goroutine
// busy.
var readerRaceResponse = []byte{
	0xce, 0x00, 0x00, 0x00, 0x0a, // Length.
	0x82, // Header map.
	0x00, 0x00,
	0x01, 0xce, 0x00, 0x00, 0x00, 0x02,
	0x80, // Body map.
}

// readerRaceStream is a stream of responses long enough that a reader is still
// draining it when the writer has already failed a Flush() and the reconnect
// has started the next reader.
var readerRaceStream = makeReaderRaceStream(50000)

func makeReaderRaceStream(packets int) []byte {
	stream := make([]byte, 0, packets*len(readerRaceResponse))
	for range packets {
		stream = append(stream, readerRaceResponse...)
	}
	return stream
}

// TestConn_reader_race_on_reconnect verifies that two reader goroutines of the
// same Connection do not share mutable state. A writer-initiated reconnect
// starts a new reader while the previous one is still draining data buffered
// by the previous connection, so the length buffer and the msgpack decoder
// used by reader() must not be per-Connection fields. Before the fix the race
// was reported by the -race flag on conn.lenbuf and conn.dec.
//
// TODO: remove the test once the minimal supported Go version is 1.25 or
// newer. TestConn_reader_race_on_reconnect_synctest checks the same race on a
// fake clock, but testing/synctest is not available in Go 1.24. Keep
// readerRaceResponse and readerRaceStream, the synctest test uses them.
func TestConn_reader_race_on_reconnect(t *testing.T) {
	// Larger than the number of Read()/Write() calls a single connection can
	// make, so that neither ever blocks on its wait group.
	const noWgWait = 1 << 30

	var dialCount atomic.Uint32
	dialer := mockIoDialer{
		init: func(conn *mockIoConn) {
			dialCount.Add(1)
			// Serve the prepared responses without blocking, so the reader
			// goroutine of this connection has work for a while after the
			// connection itself is gone.
			conn.readbuf.Write(readerRaceStream)
			conn.readWgDelay = noWgWait
			conn.writeWgDelay = noWgWait
			conn.wgDoneOnClose = false
			// Fail the writer shortly after the dial, so that reconnect()
			// starts a new reader while this one is still draining.
			conn.flushWait = 2 * time.Millisecond
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
	defer func() { _ = conn.Close() }()

	// Let the writer fail a few times so that several readers overlap.
	time.Sleep(150 * time.Millisecond)

	require.NoError(t, conn.Close())

	assert.Greater(t, dialCount.Load(), uint32(2),
		"expected several reconnects, so that readers overlap")
}
