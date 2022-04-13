// Package with implementation of methods and structures for work with
// Tarantool instance.
package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

const requestsMap = 128
const (
	connDisconnected = 0
	connConnected    = 1
	connClosed       = 2
)

const (
	connTransportNone = ""
	connTransportSsl  = "ssl"
)

type ConnEventKind int
type ConnLogKind int

const (
	// Connected signals that connection is established or reestablished.
	Connected ConnEventKind = iota + 1
	// Disconnected signals that connection is broken.
	Disconnected
	// ReconnectFailed signals that attempt to reconnect has failed.
	ReconnectFailed
	// Either reconnect attempts exhausted, or explicit Close is called.
	Closed

	// LogReconnectFailed is logged when reconnect attempt failed.
	LogReconnectFailed ConnLogKind = iota + 1
	// LogLastReconnectFailed is logged when last reconnect attempt failed,
	// connection will be closed after that.
	LogLastReconnectFailed
	// LogUnexpectedResultId is logged when response with unknown id was received.
	// Most probably it is due to request timeout.
	LogUnexpectedResultId
)

// ConnEvent is sent throw Notify channel specified in Opts.
type ConnEvent struct {
	Conn *Connection
	Kind ConnEventKind
	When time.Time
}

var epoch = time.Now()

// Logger is logger type expected to be passed in options.
type Logger interface {
	Report(event ConnLogKind, conn *Connection, v ...interface{})
}

type defaultLogger struct{}

func (d defaultLogger) Report(event ConnLogKind, conn *Connection, v ...interface{}) {
	switch event {
	case LogReconnectFailed:
		reconnects := v[0].(uint)
		err := v[1].(error)
		log.Printf("tarantool: reconnect (%d/%d) to %s failed: %s\n", reconnects, conn.opts.MaxReconnects, conn.addr, err.Error())
	case LogLastReconnectFailed:
		err := v[0].(error)
		log.Printf("tarantool: last reconnect to %s failed: %s, giving it up.\n", conn.addr, err.Error())
	case LogUnexpectedResultId:
		resp := v[0].(*Response)
		log.Printf("tarantool: connection %s got unexpected resultId (%d) in response", conn.addr, resp.RequestId)
	default:
		args := append([]interface{}{"tarantool: unexpected event ", event, conn}, v...)
		log.Print(args...)
	}
}

// Connection is a handle with a single connection to a Tarantool instance.
//
// It is created and configured with Connect function, and could not be
// reconfigured later.
//
// Connection could be in three possible states:
//
// - In "Connected" state it sends queries to Tarantool.
//
// - In "Disconnected" state it rejects queries with ClientError{Code:
// ErrConnectionNotReady}
//
// - In "Closed" state it rejects queries with ClientError{Code:
// ErrConnectionClosed}. Connection could become "Closed" when
// Connection.Close() method called, or when Tarantool disconnected and
// Reconnect pause is not specified or MaxReconnects is specified and
// MaxReconnect reconnect attempts already performed.
//
// You may perform data manipulation operation by calling its methods:
// Call*, Insert*, Replace*, Update*, Upsert*, Call*, Eval*.
//
// In any method that accepts space you my pass either space number or space
// name (in this case it will be looked up in schema). Same is true for index.
//
// ATTENTION: tuple, key, ops and args arguments for any method should be
// and array or should serialize to msgpack array.
//
// ATTENTION: result argument for *Typed methods should deserialize from
// msgpack array, cause Tarantool always returns result as an array.
// For all space related methods and Call16* (but not Call17*) methods Tarantool
// always returns array of array (array of tuples for space related methods).
// For Eval* and Call* Tarantool always returns array, but does not forces
// array of arrays.
type Connection struct {
	addr  string
	c     net.Conn
	mutex sync.Mutex
	// Schema contains schema loaded on connection.
	Schema    *Schema
	requestId uint32
	// Greeting contains first message sent by Tarantool.
	Greeting *Greeting

	shard      []connShard
	dirtyShard chan uint32

	control chan struct{}
	rlimit  chan struct{}
	opts    Opts
	state   uint32
	dec     *msgpack.Decoder
	lenbuf  [PacketLengthBytes]byte
}

var _ = Connector(&Connection{}) // Check compatibility with connector interface.

type connShard struct {
	rmut     sync.Mutex
	requests [requestsMap]struct {
		first *Future
		last  **Future
	}
	bufmut sync.Mutex
	buf    smallWBuf
	enc    *msgpack.Encoder
	_pad   [16]uint64 //nolint: unused,structcheck
}

// Greeting is a message sent by Tarantool on connect.
type Greeting struct {
	Version string
	auth    string
}

// Opts is a way to configure Connection
type Opts struct {
	// Timeout for response to a particular request. The timeout is reset when
	// push messages are received. If Timeout is zero, any request can be
	// blocked infinitely.
	// Also used to setup net.TCPConn.Set(Read|Write)Deadline.
	Timeout time.Duration
	// Timeout between reconnect attempts. If Reconnect is zero, no
	// reconnect attempts will be made.
	// If specified, then when Tarantool is not reachable or disconnected,
	// new connect attempt is performed after pause.
	// By default, no reconnection attempts are performed,
	// so once disconnected, connection becomes Closed.
	Reconnect time.Duration
	// Maximum number of reconnect failures; after that we give it up to
	// on. If MaxReconnects is zero, the client will try to reconnect
	// endlessly.
	// After MaxReconnects attempts Connection becomes closed.
	MaxReconnects uint
	// Username for logging in to Tarantool.
	User string
	// User password for logging in to Tarantool.
	Pass string
	// RateLimit limits number of 'in-fly' request, i.e. already put into
	// requests queue, but not yet answered by server or timeouted.
	// It is disabled by default.
	// See RLimitAction for possible actions when RateLimit.reached.
	RateLimit uint
	// RLimitAction tells what to do when RateLimit reached:
	//   RLimitDrop - immediatly abort request,
	//   RLimitWait - wait during timeout period for some request to be answered.
	//                If no request answered during timeout period, this request
	//                is aborted.
	//                If no timeout period is set, it will wait forever.
	// It is required if RateLimit is specified.
	RLimitAction uint
	// Concurrency is amount of separate mutexes for request
	// queues and buffers inside of connection.
	// It is rounded upto nearest power of 2.
	// By default it is runtime.GOMAXPROCS(-1) * 4
	Concurrency uint32
	// SkipSchema disables schema loading. Without disabling schema loading,
	// there is no way to create Connection for currently not accessible Tarantool.
	SkipSchema bool
	// Notify is a channel which receives notifications about Connection status
	// changes.
	Notify chan<- ConnEvent
	// Handle is user specified value, that could be retrivied with
	// Handle() method.
	Handle interface{}
	// Logger is user specified logger used for error messages.
	Logger Logger
	// Transport is the connection type, by default the connection is unencrypted.
	Transport string
	// SslOpts is used only if the Transport == 'ssl' is set.
	Ssl SslOpts
}

// SslOpts is a way to configure ssl transport.
type SslOpts struct {
	// KeyFile is a path to a private SSL key file.
	KeyFile string
	// CertFile is a path to an SSL sertificate file.
	CertFile string
	// CaFile is a path to a trusted certificate authorities (CA) file.
	CaFile string
	// Ciphers is a colon-separated (:) list of SSL cipher suites the connection
	// can use.
	//
	// We don't provide a list of supported ciphers. This is what OpenSSL
	// does. The only limitation is usage of TLSv1.2 (because other protocol
	// versions don't seem to support the GOST cipher). To add additional
	// ciphers (GOST cipher), you must configure OpenSSL.
	//
	// See also
	//
	// * https://www.openssl.org/docs/man1.1.1/man1/ciphers.html
	Ciphers string
}

// Connect creates and configures a new Connection.
//
// Address could be specified in following ways:
//
// - TCP connections (tcp://192.168.1.1:3013, tcp://my.host:3013,
// tcp:192.168.1.1:3013, tcp:my.host:3013, 192.168.1.1:3013, my.host:3013)
//
// - Unix socket, first '/' or '.' indicates Unix socket
// (unix:///abs/path/tnt.sock, unix:path/tnt.sock, /abs/path/tnt.sock,
// ./rel/path/tnt.sock, unix/:path/tnt.sock)
//
// Notes:
//
// - If opts.Reconnect is zero (default), then connection either already connected
// or error is returned.
//
// - If opts.Reconnect is non-zero, then error will be returned only if authorization
// fails. But if Tarantool is not reachable, then it will make an attempt to reconnect later
// and will not finish to make attempts on authorization failures.
func Connect(addr string, opts Opts) (conn *Connection, err error) {
	conn = &Connection{
		addr:      addr,
		requestId: 0,
		Greeting:  &Greeting{},
		control:   make(chan struct{}),
		opts:      opts,
		dec:       msgpack.NewDecoder(&smallBuf{}),
	}
	maxprocs := uint32(runtime.GOMAXPROCS(-1))
	if conn.opts.Concurrency == 0 || conn.opts.Concurrency > maxprocs*128 {
		conn.opts.Concurrency = maxprocs * 4
	}
	if c := conn.opts.Concurrency; c&(c-1) != 0 {
		for i := uint(1); i < 32; i *= 2 {
			c |= c >> i
		}
		conn.opts.Concurrency = c + 1
	}
	conn.dirtyShard = make(chan uint32, conn.opts.Concurrency*2)
	conn.shard = make([]connShard, conn.opts.Concurrency)
	for i := range conn.shard {
		shard := &conn.shard[i]
		for j := range shard.requests {
			shard.requests[j].last = &shard.requests[j].first
		}
	}

	if opts.RateLimit > 0 {
		conn.rlimit = make(chan struct{}, opts.RateLimit)
		if opts.RLimitAction != RLimitDrop && opts.RLimitAction != RLimitWait {
			return nil, errors.New("RLimitAction should be specified to RLimitDone nor RLimitWait")
		}
	}

	if conn.opts.Logger == nil {
		conn.opts.Logger = defaultLogger{}
	}

	if err = conn.createConnection(false); err != nil {
		ter, ok := err.(Error)
		if conn.opts.Reconnect <= 0 {
			return nil, err
		} else if ok && (ter.Code == ErrNoSuchUser ||
			ter.Code == ErrPasswordMismatch) {
			// Reported auth errors immediately.
			return nil, err
		} else {
			// Without SkipSchema it is useless.
			go func(conn *Connection) {
				conn.mutex.Lock()
				defer conn.mutex.Unlock()
				if err := conn.createConnection(true); err != nil {
					conn.closeConnection(err, true)
				}
			}(conn)
			err = nil
		}
	}

	go conn.pinger()
	if conn.opts.Timeout > 0 {
		go conn.timeouts()
	}

	// TODO: reload schema after reconnect.
	if !conn.opts.SkipSchema {
		if err = conn.loadSchema(); err != nil {
			conn.mutex.Lock()
			defer conn.mutex.Unlock()
			conn.closeConnection(err, true)
			return nil, err
		}
	}

	return conn, err
}

// ConnectedNow reports if connection is established at the moment.
func (conn *Connection) ConnectedNow() bool {
	return atomic.LoadUint32(&conn.state) == connConnected
}

// ClosedNow reports if connection is closed by user or after reconnect.
func (conn *Connection) ClosedNow() bool {
	return atomic.LoadUint32(&conn.state) == connClosed
}

// Close closes Connection.
// After this method called, there is no way to reopen this Connection.
func (conn *Connection) Close() error {
	err := ClientError{ErrConnectionClosed, "connection closed by client"}
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	return conn.closeConnection(err, true)
}

// Addr returns a configured address of Tarantool socket.
func (conn *Connection) Addr() string {
	return conn.addr
}

// RemoteAddr returns an address of Tarantool socket.
func (conn *Connection) RemoteAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.RemoteAddr().String()
}

// LocalAddr returns an address of outgoing socket.
func (conn *Connection) LocalAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.LocalAddr().String()
}

// Handle returns a user-specified handle from Opts.
func (conn *Connection) Handle() interface{} {
	return conn.opts.Handle
}

func (conn *Connection) dial() (err error) {
	var connection net.Conn
	network := "tcp"
	opts := conn.opts
	address := conn.addr
	timeout := opts.Reconnect / 2
	transport := opts.Transport
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	} else if timeout > 5*time.Second {
		timeout = 5 * time.Second
	}
	// Unix socket connection
	addrLen := len(address)
	if addrLen > 0 && (address[0] == '.' || address[0] == '/') {
		network = "unix"
	} else if addrLen >= 7 && address[0:7] == "unix://" {
		network = "unix"
		address = address[7:]
	} else if addrLen >= 5 && address[0:5] == "unix:" {
		network = "unix"
		address = address[5:]
	} else if addrLen >= 6 && address[0:6] == "unix/:" {
		network = "unix"
		address = address[6:]
	} else if addrLen >= 6 && address[0:6] == "tcp://" {
		address = address[6:]
	} else if addrLen >= 4 && address[0:4] == "tcp:" {
		address = address[4:]
	}
	if transport == connTransportNone {
		connection, err = net.DialTimeout(network, address, timeout)
	} else if transport == connTransportSsl {
		connection, err = sslDialTimeout(network, address, timeout, opts.Ssl)
	} else {
		err = errors.New("An unsupported transport type: " + transport)
	}
	if err != nil {
		return
	}
	dc := &DeadlineIO{to: opts.Timeout, c: connection}
	r := bufio.NewReaderSize(dc, 128*1024)
	w := bufio.NewWriterSize(dc, 128*1024)
	greeting := make([]byte, 128)
	_, err = io.ReadFull(r, greeting)
	if err != nil {
		connection.Close()
		return
	}
	conn.Greeting.Version = bytes.NewBuffer(greeting[:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:108]).String()

	// Auth
	if opts.User != "" {
		scr, err := scramble(conn.Greeting.auth, opts.Pass)
		if err != nil {
			err = errors.New("auth: scrambling failure " + err.Error())
			connection.Close()
			return err
		}
		if err = conn.writeAuthRequest(w, scr); err != nil {
			connection.Close()
			return err
		}
		if err = conn.readAuthResponse(r); err != nil {
			connection.Close()
			return err
		}
	}

	// Only if connected and authenticated.
	conn.lockShards()
	conn.c = connection
	atomic.StoreUint32(&conn.state, connConnected)
	conn.unlockShards()
	go conn.writer(w, connection)
	go conn.reader(r, connection)

	return
}

func (conn *Connection) writeAuthRequest(w *bufio.Writer, scramble []byte) (err error) {
	request := &Future{
		requestId:   0,
		requestCode: AuthRequestCode,
	}
	var packet smallWBuf
	err = request.pack(&packet, msgpack.NewEncoder(&packet), func(enc *msgpack.Encoder) error {
		return enc.Encode(map[uint32]interface{}{
			KeyUserName: conn.opts.User,
			KeyTuple:    []interface{}{string("chap-sha1"), string(scramble)},
		})
	})
	if err != nil {
		return errors.New("auth: pack error " + err.Error())
	}
	if err := write(w, packet.b); err != nil {
		return errors.New("auth: write error " + err.Error())
	}
	if err = w.Flush(); err != nil {
		return errors.New("auth: flush error " + err.Error())
	}
	return
}

func (conn *Connection) readAuthResponse(r io.Reader) (err error) {
	respBytes, err := conn.read(r)
	if err != nil {
		return errors.New("auth: read error " + err.Error())
	}
	resp := Response{buf: smallBuf{b: respBytes}}
	err = resp.decodeHeader(conn.dec)
	if err != nil {
		return errors.New("auth: decode response header error " + err.Error())
	}
	err = resp.decodeBody()
	if err != nil {
		switch err.(type) {
		case Error:
			return err
		default:
			return errors.New("auth: decode response body error " + err.Error())
		}
	}
	return
}

func (conn *Connection) createConnection(reconnect bool) (err error) {
	var reconnects uint
	for conn.c == nil && conn.state == connDisconnected {
		now := time.Now()
		err = conn.dial()
		if err == nil || !reconnect {
			if err == nil {
				conn.notify(Connected)
			}
			return
		}
		if conn.opts.MaxReconnects > 0 && reconnects > conn.opts.MaxReconnects {
			conn.opts.Logger.Report(LogLastReconnectFailed, conn, err)
			err = ClientError{ErrConnectionClosed, "last reconnect failed"}
			// mark connection as closed to avoid reopening by another goroutine
			return
		}
		conn.opts.Logger.Report(LogReconnectFailed, conn, reconnects, err)
		conn.notify(ReconnectFailed)
		reconnects++
		conn.mutex.Unlock()
		time.Sleep(time.Until(now.Add(conn.opts.Reconnect)))
		conn.mutex.Lock()
	}
	if conn.state == connClosed {
		err = ClientError{ErrConnectionClosed, "using closed connection"}
	}
	return
}

func (conn *Connection) closeConnection(neterr error, forever bool) (err error) {
	conn.lockShards()
	defer conn.unlockShards()
	if forever {
		if conn.state != connClosed {
			close(conn.control)
			atomic.StoreUint32(&conn.state, connClosed)
			conn.notify(Closed)
		}
	} else {
		atomic.StoreUint32(&conn.state, connDisconnected)
		conn.notify(Disconnected)
	}
	if conn.c != nil {
		err = conn.c.Close()
		conn.c = nil
	}
	for i := range conn.shard {
		conn.shard[i].buf.Reset()
		requests := &conn.shard[i].requests
		for pos := range requests {
			fut := requests[pos].first
			requests[pos].first = nil
			requests[pos].last = &requests[pos].first
			for fut != nil {
				fut.SetError(neterr)
				conn.markDone(fut)
				fut, fut.next = fut.next, nil
			}
		}
	}
	return
}

func (conn *Connection) reconnect(neterr error, c net.Conn) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.opts.Reconnect > 0 {
		if c == conn.c {
			conn.closeConnection(neterr, false)
			if err := conn.createConnection(true); err != nil {
				conn.closeConnection(err, true)
			}
		}
	} else {
		conn.closeConnection(neterr, true)
	}
}

func (conn *Connection) lockShards() {
	for i := range conn.shard {
		conn.shard[i].rmut.Lock()
		conn.shard[i].bufmut.Lock()
	}
}

func (conn *Connection) unlockShards() {
	for i := range conn.shard {
		conn.shard[i].rmut.Unlock()
		conn.shard[i].bufmut.Unlock()
	}
}

func (conn *Connection) pinger() {
	to := conn.opts.Timeout
	if to == 0 {
		to = 3 * time.Second
	}
	t := time.NewTicker(to / 3)
	defer t.Stop()
	for {
		select {
		case <-conn.control:
			return
		case <-t.C:
		}
		conn.Ping()
	}
}

func (conn *Connection) notify(kind ConnEventKind) {
	if conn.opts.Notify != nil {
		select {
		case conn.opts.Notify <- ConnEvent{Kind: kind, Conn: conn, When: time.Now()}:
		default:
		}
	}
}

func (conn *Connection) writer(w *bufio.Writer, c net.Conn) {
	var shardn uint32
	var packet smallWBuf
	for atomic.LoadUint32(&conn.state) != connClosed {
		select {
		case shardn = <-conn.dirtyShard:
		default:
			runtime.Gosched()
			if len(conn.dirtyShard) == 0 {
				if err := w.Flush(); err != nil {
					conn.reconnect(err, c)
					return
				}
			}
			select {
			case shardn = <-conn.dirtyShard:
			case <-conn.control:
				return
			}
		}
		shard := &conn.shard[shardn]
		shard.bufmut.Lock()
		if conn.c != c {
			conn.dirtyShard <- shardn
			shard.bufmut.Unlock()
			return
		}
		packet, shard.buf = shard.buf, packet
		shard.bufmut.Unlock()
		if packet.Len() == 0 {
			continue
		}
		if err := write(w, packet.b); err != nil {
			conn.reconnect(err, c)
			return
		}
		packet.Reset()
	}
}

func (conn *Connection) reader(r *bufio.Reader, c net.Conn) {
	for atomic.LoadUint32(&conn.state) != connClosed {
		respBytes, err := conn.read(r)
		if err != nil {
			conn.reconnect(err, c)
			return
		}
		resp := &Response{buf: smallBuf{b: respBytes}}
		err = resp.decodeHeader(conn.dec)
		if err != nil {
			conn.reconnect(err, c)
			return
		}

		var fut *Future = nil
		if resp.Code == PushCode {
			if fut = conn.peekFuture(resp.RequestId); fut != nil {
				fut.AppendPush(resp)
			}
		} else {
			if fut = conn.fetchFuture(resp.RequestId); fut != nil {
				fut.SetResponse(resp)
				conn.markDone(fut)
			}
		}
		if fut == nil {
			conn.opts.Logger.Report(LogUnexpectedResultId, conn, resp)
		}
	}
}

func (conn *Connection) newFuture(requestCode int32) (fut *Future) {
	fut = NewFuture()
	if conn.rlimit != nil && conn.opts.RLimitAction == RLimitDrop {
		select {
		case conn.rlimit <- struct{}{}:
		default:
			fut.err = ClientError{
				ErrRateLimited,
				"Request is rate limited on client",
			}
			fut.ready = nil
			fut.done = nil
			return
		}
	}
	fut.requestId = conn.nextRequestId()
	fut.requestCode = requestCode
	shardn := fut.requestId & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardn]
	shard.rmut.Lock()
	switch conn.state {
	case connClosed:
		fut.err = ClientError{
			ErrConnectionClosed,
			"using closed connection",
		}
		fut.ready = nil
		fut.done = nil
		shard.rmut.Unlock()
		return
	case connDisconnected:
		fut.err = ClientError{
			ErrConnectionNotReady,
			"client connection is not ready",
		}
		fut.ready = nil
		fut.done = nil
		shard.rmut.Unlock()
		return
	}
	pos := (fut.requestId / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	*pair.last = fut
	pair.last = &fut.next
	if conn.opts.Timeout > 0 {
		fut.timeout = time.Since(epoch) + conn.opts.Timeout
	}
	shard.rmut.Unlock()
	if conn.rlimit != nil && conn.opts.RLimitAction == RLimitWait {
		select {
		case conn.rlimit <- struct{}{}:
		default:
			runtime.Gosched()
			select {
			case conn.rlimit <- struct{}{}:
			case <-fut.done:
				if fut.err == nil {
					panic("fut.done is closed, but err is nil")
				}
			}
		}
	}
	return
}

func (conn *Connection) sendFuture(fut *Future, body func(*msgpack.Encoder) error) *Future {
	if fut.ready == nil {
		return fut
	}
	conn.putFuture(fut, body)
	return fut
}

func (conn *Connection) failFuture(fut *Future, err error) *Future {
	if f := conn.fetchFuture(fut.requestId); f == fut {
		fut.SetError(err)
		conn.markDone(fut)
	}
	return fut
}

func (conn *Connection) putFuture(fut *Future, body func(*msgpack.Encoder) error) {
	shardn := fut.requestId & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardn]
	shard.bufmut.Lock()
	select {
	case <-fut.done:
		shard.bufmut.Unlock()
		return
	default:
	}
	firstWritten := shard.buf.Len() == 0
	if shard.buf.Cap() == 0 {
		shard.buf.b = make([]byte, 0, 128)
		shard.enc = msgpack.NewEncoder(&shard.buf)
	}
	blen := shard.buf.Len()
	if err := fut.pack(&shard.buf, shard.enc, body); err != nil {
		shard.buf.Trunc(blen)
		shard.bufmut.Unlock()
		if f := conn.fetchFuture(fut.requestId); f == fut {
			fut.SetError(err)
			conn.markDone(fut)
		} else if f != nil {
			/* in theory, it is possible. In practice, you have
			 * to have race condition that lasts hours */
			panic("Unknown future")
		} else {
			fut.wait()
			if fut.err == nil {
				panic("Future removed from queue without error")
			}
			if _, ok := fut.err.(ClientError); ok {
				// packing error is more important than connection
				// error, because it is indication of programmer's
				// mistake.
				fut.SetError(err)
			}
		}
		return
	}
	shard.bufmut.Unlock()
	if firstWritten {
		conn.dirtyShard <- shardn
	}
}

func (conn *Connection) markDone(fut *Future) {
	if conn.rlimit != nil {
		<-conn.rlimit
	}
}

func (conn *Connection) peekFuture(reqid uint32) (fut *Future) {
	shard := &conn.shard[reqid&(conn.opts.Concurrency-1)]
	pos := (reqid / conn.opts.Concurrency) & (requestsMap - 1)
	shard.rmut.Lock()
	defer shard.rmut.Unlock()

	if conn.opts.Timeout > 0 {
		fut = conn.getFutureImp(reqid, true)
		pair := &shard.requests[pos]
		*pair.last = fut
		pair.last = &fut.next
		fut.timeout = time.Since(epoch) + conn.opts.Timeout
	} else {
		fut = conn.getFutureImp(reqid, false)
	}

	return fut
}

func (conn *Connection) fetchFuture(reqid uint32) (fut *Future) {
	shard := &conn.shard[reqid&(conn.opts.Concurrency-1)]
	shard.rmut.Lock()
	fut = conn.getFutureImp(reqid, true)
	shard.rmut.Unlock()
	return fut
}

func (conn *Connection) getFutureImp(reqid uint32, fetch bool) *Future {
	shard := &conn.shard[reqid&(conn.opts.Concurrency-1)]
	pos := (reqid / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	root := &pair.first
	for {
		fut := *root
		if fut == nil {
			return nil
		}
		if fut.requestId == reqid {
			if fetch {
				*root = fut.next
				if fut.next == nil {
					pair.last = root
				} else {
					fut.next = nil
				}
			}
			return fut
		}
		root = &fut.next
	}
}

func (conn *Connection) timeouts() {
	timeout := conn.opts.Timeout
	t := time.NewTimer(timeout)
	for {
		var nowepoch time.Duration
		select {
		case <-conn.control:
			t.Stop()
			return
		case <-t.C:
		}
		minNext := time.Since(epoch) + timeout
		for i := range conn.shard {
			nowepoch = time.Since(epoch)
			shard := &conn.shard[i]
			for pos := range shard.requests {
				shard.rmut.Lock()
				pair := &shard.requests[pos]
				for pair.first != nil && pair.first.timeout < nowepoch {
					shard.bufmut.Lock()
					fut := pair.first
					pair.first = fut.next
					if fut.next == nil {
						pair.last = &pair.first
					} else {
						fut.next = nil
					}
					fut.SetError(ClientError{
						Code: ErrTimeouted,
						Msg:  fmt.Sprintf("client timeout for request %d", fut.requestId),
					})
					conn.markDone(fut)
					shard.bufmut.Unlock()
				}
				if pair.first != nil && pair.first.timeout < minNext {
					minNext = pair.first.timeout
				}
				shard.rmut.Unlock()
			}
		}
		nowepoch = time.Since(epoch)
		if nowepoch+time.Microsecond < minNext {
			t.Reset(minNext - nowepoch)
		} else {
			t.Reset(time.Microsecond)
		}
	}
}

func write(w io.Writer, data []byte) (err error) {
	l, err := w.Write(data)
	if err != nil {
		return
	}
	if l != len(data) {
		panic("Wrong length writed")
	}
	return
}

func (conn *Connection) read(r io.Reader) (response []byte, err error) {
	var length int

	if _, err = io.ReadFull(r, conn.lenbuf[:]); err != nil {
		return
	}
	if conn.lenbuf[0] != 0xce {
		err = errors.New("Wrong reponse header")
		return
	}
	length = (int(conn.lenbuf[1]) << 24) +
		(int(conn.lenbuf[2]) << 16) +
		(int(conn.lenbuf[3]) << 8) +
		int(conn.lenbuf[4])

	if length == 0 {
		err = errors.New("Response should not be 0 length")
		return
	}
	response = make([]byte, length)
	_, err = io.ReadFull(r, response)

	return
}

func (conn *Connection) nextRequestId() (requestId uint32) {
	return atomic.AddUint32(&conn.requestId, 1)
}

// Do verifies, sends the request and returns a response.
//
// An error is returned if the request was formed incorrectly, or failed to
// communicate by the connection, or unable to decode the response.
func (conn *Connection) Do(req Request) (*Response, error) {
	fut, err := conn.DoAsync(req)
	if err != nil {
		return nil, err
	}
	return fut.Get()
}

// DoTyped verifies, sends the request and fills the typed result.
//
// An error is returned if the request was formed incorrectly, or failed to
// communicate by the connection, or unable to decode the response.
func (conn *Connection) DoTyped(req Request, result interface{}) error {
	fut, err := conn.DoAsync(req)
	if err != nil {
		return err
	}
	return fut.GetTyped(result)
}

// DoAsync verifies, sends the request and returns a future.
//
// An error is returned if the request was formed incorrectly, or failed to
// create the future.
func (conn *Connection) DoAsync(req Request) (*Future, error) {
	bodyFunc, err := req.BodyFunc(conn.Schema)
	if err != nil {
		return nil, err
	}
	future := conn.newFuture(req.Code())
	return conn.sendFuture(future, bodyFunc), nil
}

// ConfiguredTimeout returns a timeout from connection config.
func (conn *Connection) ConfiguredTimeout() time.Duration {
	return conn.opts.Timeout
}

// OverrideSchema sets Schema for the connection.
func (conn *Connection) OverrideSchema(s *Schema) {
	if s != nil {
		conn.mutex.Lock()
		defer conn.mutex.Unlock()
		conn.Schema = s
	}
}
