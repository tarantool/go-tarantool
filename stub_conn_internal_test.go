package tarantool

import (
	"context"
	"io"
	"net"
	"sync"
)

// stubConnAddr is a net.Addr for a connection that has no real socket.
type stubConnAddr struct{}

func (stubConnAddr) String() string  { return "stub" }
func (stubConnAddr) Network() string { return "stub" }

// stubConn is a Conn for the tests of this package. It accepts everything
// written to it and never returns any data: Read() blocks until the test
// breaks the connection or until Close(), and then reports io.EOF, which is
// what a Connection sees when the socket dies.
//
// Every connection has its own gate, so a test decides which of them breaks
// and when. A connection whose gate is never closed simply stays idle for as
// long as the test needs it.
type stubConn struct {
	// info is the value reported by ProtocolInfo().
	info ProtocolInfo

	// gate is closed by a test to make Read() return io.EOF.
	gate chan struct{}
	// closed is closed by Close() to release a blocked Read().
	closed    chan struct{}
	closeOnce sync.Once
}

func (c *stubConn) Read(_ []byte) (int, error) {
	select {
	case <-c.gate:
	case <-c.closed:
	}
	return 0, io.EOF
}

func (c *stubConn) Write(b []byte) (int, error) { return len(b), nil }

func (c *stubConn) Flush() error { return nil }

func (c *stubConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (c *stubConn) Greeting() Greeting { return Greeting{} }

func (c *stubConn) ProtocolInfo() ProtocolInfo { return c.info }

func (c *stubConn) Addr() net.Addr { return stubConnAddr{} }

// stubDialerOpts configures a stubDialer. The zero value hands out idle
// connections that report an empty ProtocolInfo.
type stubDialerOpts struct {
	// info returns the protocol info of the n-th (1-based) connection, which
	// is how a test makes a feature appear or disappear on a reconnect. The
	// empty ProtocolInfo is reported when info is nil.
	info func(dial int) ProtocolInfo
}

// stubDialer hands out stubConn's and keeps them, so that a test can reach a
// particular connection after the Connection has dialed it.
type stubDialer struct {
	opts stubDialerOpts

	mutex sync.Mutex
	conns []*stubConn
}

func (d *stubDialer) Dial(_ context.Context, _ DialOpts) (Conn, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	conn := &stubConn{
		gate:   make(chan struct{}),
		closed: make(chan struct{}),
	}
	if d.opts.info != nil {
		conn.info = d.opts.info(len(d.conns) + 1)
	}
	d.conns = append(d.conns, conn)

	return conn, nil
}

// conn returns the n-th (1-based) connection the dialer has handed out, or nil
// when it has not dialed that many times yet.
func (d *stubDialer) conn(n int) *stubConn {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if n > len(d.conns) {
		return nil
	}
	return d.conns[n-1]
}

// breakConn makes the n-th (1-based) connection report io.EOF from Read(), the
// way a broken socket does, which starts a reconnect.
func (d *stubDialer) breakConn(n int) {
	close(d.conn(n).gate)
}
