package tarantool

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

const bufSize = 128 * 1024

// Greeting is a message sent by Tarantool on connect.
type Greeting struct {
	Version string
}

// writeFlusher is the interface that groups the basic Write and Flush methods.
type writeFlusher interface {
	io.Writer
	Flush() error
}

// Conn is a generic stream-oriented network connection to a Tarantool
// instance.
type Conn interface {
	// Read reads data from the connection.
	Read(b []byte) (int, error)
	// Write writes data to the connection. There may be an internal buffer for
	// better performance control from a client side.
	Write(b []byte) (int, error)
	// Flush writes any buffered data.
	Flush() error
	// Close closes the connection.
	// Any blocked Read or Flush operations will be unblocked and return
	// errors.
	Close() error
	// Greeting returns server greeting.
	Greeting() Greeting
	// ProtocolInfo returns server protocol info.
	ProtocolInfo() ProtocolInfo
	// Addr returns the connection address.
	Addr() net.Addr
}

// DialOpts is a way to configure a Dial method to create a new Conn.
type DialOpts struct {
	// IoTimeout is a timeout per a network read/write.
	IoTimeout time.Duration
}

// Dialer is the interface that wraps a method to connect to a Tarantool
// instance. The main idea is to provide a ready-to-work connection with
// basic preparation, successful authorization and additional checks.
//
// You can provide your own implementation to Connect() call if
// some functionality is not implemented in the connector. See NetDialer.Dial()
// implementation as example.
type Dialer interface {
	// Dial connects to a Tarantool instance to the address with specified
	// options.
	Dial(ctx context.Context, opts DialOpts) (Conn, error)
}

type tntConn struct {
	net      net.Conn
	reader   io.Reader
	writer   writeFlusher
	greeting Greeting
	protocol ProtocolInfo
}

// rawDial does basic dial operations:
// reads greeting, identifies a protocol and validates it.
func rawDial(conn *tntConn, requiredProto ProtocolInfo) (string, error) {
	version, salt, err := readGreeting(conn.reader)
	if err != nil {
		return "", fmt.Errorf("failed to read greeting: %w", err)
	}
	conn.greeting.Version = version

	if conn.protocol, err = identify(conn.writer, conn.reader); err != nil {
		return "", fmt.Errorf("failed to identify: %w", err)
	}

	if err = checkProtocolInfo(requiredProto, conn.protocol); err != nil {
		return "", fmt.Errorf("invalid server protocol: %w", err)
	}
	return salt, err
}

// NetDialer is a basic Dialer implementation.
type NetDialer struct {
	// Address is an address to connect.
	// It could be specified in following ways:
	//
	// - TCP connections (tcp://192.168.1.1:3013, tcp://my.host:3013,
	// tcp:192.168.1.1:3013, tcp:my.host:3013, 192.168.1.1:3013, my.host:3013)
	//
	// - Unix socket, first '/' or '.' indicates Unix socket
	// (unix:///abs/path/tnt.sock, unix:path/tnt.sock, /abs/path/tnt.sock,
	// ./rel/path/tnt.sock, unix/:path/tnt.sock)
	Address string
	// Username for logging in to Tarantool.
	User string
	// User password for logging in to Tarantool.
	Password string
	// RequiredProtocol contains minimal protocol version and
	// list of protocol features that should be supported by
	// Tarantool server. By default, there are no restrictions.
	RequiredProtocolInfo ProtocolInfo
}

// Dial makes NetDialer satisfy the Dialer interface.
func (d NetDialer) Dial(ctx context.Context, opts DialOpts) (Conn, error) {
	var err error
	conn := new(tntConn)

	network, address := parseAddress(d.Address)
	dialer := net.Dialer{}
	conn.net, err = dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	dc := &deadlineIO{to: opts.IoTimeout, c: conn.net}
	conn.reader = bufio.NewReaderSize(dc, bufSize)
	conn.writer = bufio.NewWriterSize(dc, bufSize)

	salt, err := rawDial(conn, d.RequiredProtocolInfo)
	if err != nil {
		conn.net.Close()
		return nil, err
	}

	if d.User == "" {
		return conn, nil
	}

	conn.protocol.Auth = ChapSha1Auth
	if err = authenticate(conn, ChapSha1Auth, d.User, d.Password, salt); err != nil {
		conn.net.Close()
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	return conn, nil
}

// OpenSslDialer allows to use SSL transport for connection.
type OpenSslDialer struct {
	// Address is an address to connect.
	// It could be specified in following ways:
	//
	// - TCP connections (tcp://192.168.1.1:3013, tcp://my.host:3013,
	// tcp:192.168.1.1:3013, tcp:my.host:3013, 192.168.1.1:3013, my.host:3013)
	//
	// - Unix socket, first '/' or '.' indicates Unix socket
	// (unix:///abs/path/tnt.sock, unix:path/tnt.sock, /abs/path/tnt.sock,
	// ./rel/path/tnt.sock, unix/:path/tnt.sock)
	Address string
	// Auth is an authentication method.
	Auth Auth
	// Username for logging in to Tarantool.
	User string
	// User password for logging in to Tarantool.
	Password string
	// RequiredProtocol contains minimal protocol version and
	// list of protocol features that should be supported by
	// Tarantool server. By default, there are no restrictions.
	RequiredProtocolInfo ProtocolInfo
	// SslKeyFile is a path to a private SSL key file.
	SslKeyFile string
	// SslCertFile is a path to an SSL certificate file.
	SslCertFile string
	// SslCaFile is a path to a trusted certificate authorities (CA) file.
	SslCaFile string
	// SslCiphers is a colon-separated (:) list of SSL cipher suites the connection
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
	SslCiphers string
	// SslPassword is a password for decrypting the private SSL key file.
	// The priority is as follows: try to decrypt with SslPassword, then
	// try SslPasswordFile.
	SslPassword string
	// SslPasswordFile is a path to the list of passwords for decrypting
	// the private SSL key file. The connection tries every line from the
	// file as a password.
	SslPasswordFile string
}

// Dial makes OpenSslDialer satisfy the Dialer interface.
func (d OpenSslDialer) Dial(ctx context.Context, opts DialOpts) (Conn, error) {
	var err error
	conn := new(tntConn)

	network, address := parseAddress(d.Address)
	conn.net, err = sslDialContext(ctx, network, address, sslOpts{
		KeyFile:      d.SslKeyFile,
		CertFile:     d.SslCertFile,
		CaFile:       d.SslCaFile,
		Ciphers:      d.SslCiphers,
		Password:     d.SslPassword,
		PasswordFile: d.SslPasswordFile,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	dc := &deadlineIO{to: opts.IoTimeout, c: conn.net}
	conn.reader = bufio.NewReaderSize(dc, bufSize)
	conn.writer = bufio.NewWriterSize(dc, bufSize)

	salt, err := rawDial(conn, d.RequiredProtocolInfo)
	if err != nil {
		conn.net.Close()
		return nil, err
	}

	if d.User == "" {
		return conn, nil
	}

	if d.Auth == AutoAuth {
		if conn.protocol.Auth != AutoAuth {
			d.Auth = conn.protocol.Auth
		} else {
			d.Auth = ChapSha1Auth
		}
	}
	conn.protocol.Auth = d.Auth

	if err = authenticate(conn, d.Auth, d.User, d.Password, salt); err != nil {
		conn.net.Close()
		return nil, fmt.Errorf("failed to authenticate: %w", err)
	}

	return conn, nil
}

// FdDialer allows to use an existing socket fd for connection.
type FdDialer struct {
	// Fd is a socket file descrpitor.
	Fd uintptr
	// RequiredProtocol contains minimal protocol version and
	// list of protocol features that should be supported by
	// Tarantool server. By default, there are no restrictions.
	RequiredProtocolInfo ProtocolInfo
}

type fdAddr struct {
	Fd uintptr
}

func (a fdAddr) Network() string {
	return "fd"
}

func (a fdAddr) String() string {
	return fmt.Sprintf("fd://%d", a.Fd)
}

type fdConn struct {
	net.Conn
	Addr fdAddr
}

func (c *fdConn) RemoteAddr() net.Addr {
	return c.Addr
}

// Dial makes FdDialer satisfy the Dialer interface.
func (d FdDialer) Dial(ctx context.Context, opts DialOpts) (Conn, error) {
	file := os.NewFile(d.Fd, "")
	c, err := net.FileConn(file)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	conn := new(tntConn)
	conn.net = &fdConn{Conn: c, Addr: fdAddr{Fd: d.Fd}}

	dc := &deadlineIO{to: opts.IoTimeout, c: conn.net}
	conn.reader = bufio.NewReaderSize(dc, bufSize)
	conn.writer = bufio.NewWriterSize(dc, bufSize)

	_, err = rawDial(conn, d.RequiredProtocolInfo)
	if err != nil {
		conn.net.Close()
		return nil, err
	}

	return conn, nil
}

// Addr makes tntConn satisfy the Conn interface.
func (c *tntConn) Addr() net.Addr {
	return c.net.RemoteAddr()
}

// Read makes tntConn satisfy the Conn interface.
func (c *tntConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

// Write makes tntConn satisfy the Conn interface.
func (c *tntConn) Write(p []byte) (int, error) {
	if l, err := c.writer.Write(p); err != nil {
		return l, err
	} else if l != len(p) {
		return l, errors.New("wrong length written")
	} else {
		return l, nil
	}
}

// Flush makes tntConn satisfy the Conn interface.
func (c *tntConn) Flush() error {
	return c.writer.Flush()
}

// Close makes tntConn satisfy the Conn interface.
func (c *tntConn) Close() error {
	return c.net.Close()
}

// Greeting makes tntConn satisfy the Conn interface.
func (c *tntConn) Greeting() Greeting {
	return c.greeting
}

// ProtocolInfo makes tntConn satisfy the Conn interface.
func (c *tntConn) ProtocolInfo() ProtocolInfo {
	return c.protocol
}

// parseAddress split address into network and address parts.
func parseAddress(address string) (string, string) {
	network := "tcp"
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

	return network, address
}

// readGreeting reads a greeting message.
func readGreeting(reader io.Reader) (string, string, error) {
	var version, salt string

	data := make([]byte, 128)
	_, err := io.ReadFull(reader, data)
	if err == nil {
		version = bytes.NewBuffer(data[:64]).String()
		salt = bytes.NewBuffer(data[64:108]).String()
	}

	return version, salt, err
}

// identify sends info about client protocol, receives info
// about server protocol in response and stores it in the connection.
func identify(w writeFlusher, r io.Reader) (ProtocolInfo, error) {
	var info ProtocolInfo

	req := NewIdRequest(clientProtocolInfo)
	if err := writeRequest(w, req); err != nil {
		return info, err
	}

	resp, err := readResponse(r)
	if err != nil {
		if iproto.Error(resp.Code) == iproto.ER_UNKNOWN_REQUEST_TYPE {
			// IPROTO_ID requests are not supported by server.
			return info, nil
		}

		return info, err
	}

	if len(resp.Data) == 0 {
		return info, errors.New("unexpected response: no data")
	}

	info, ok := resp.Data[0].(ProtocolInfo)
	if !ok {
		return info, errors.New("unexpected response: wrong data")
	}

	return info, nil
}

// checkProtocolInfo checks that required protocol version is
// and protocol features are supported.
func checkProtocolInfo(required ProtocolInfo, actual ProtocolInfo) error {
	if required.Version > actual.Version {
		return fmt.Errorf("protocol version %d is not supported",
			required.Version)
	}

	// It seems that iterating over a small list is way faster
	// than building a map: https://stackoverflow.com/a/52710077/11646599
	var missed []string
	for _, requiredFeature := range required.Features {
		found := false
		for _, actualFeature := range actual.Features {
			if requiredFeature == actualFeature {
				found = true
			}
		}
		if !found {
			missed = append(missed, requiredFeature.String())
		}
	}

	switch {
	case len(missed) == 1:
		return fmt.Errorf("protocol feature %s is not supported", missed[0])
	case len(missed) > 1:
		joined := strings.Join(missed, ", ")
		return fmt.Errorf("protocol features %s are not supported", joined)
	default:
		return nil
	}
}

// authenticate authenticates for a connection.
func authenticate(c Conn, auth Auth, user string, pass string, salt string) error {
	var req Request
	var err error

	switch auth {
	case ChapSha1Auth:
		req, err = newChapSha1AuthRequest(user, pass, salt)
		if err != nil {
			return err
		}
	case PapSha256Auth:
		req = newPapSha256AuthRequest(user, pass)
	default:
		return errors.New("unsupported method " + auth.String())
	}

	if err = writeRequest(c, req); err != nil {
		return err
	}
	if _, err = readResponse(c); err != nil {
		return err
	}
	return nil
}

// writeRequest writes a request to the writer.
func writeRequest(w writeFlusher, req Request) error {
	var packet smallWBuf
	err := pack(&packet, msgpack.NewEncoder(&packet), 0, req, ignoreStreamId, nil)

	if err != nil {
		return fmt.Errorf("pack error: %w", err)
	}
	if _, err = w.Write(packet.b); err != nil {
		return fmt.Errorf("write error: %w", err)
	}
	if err = w.Flush(); err != nil {
		return fmt.Errorf("flush error: %w", err)
	}
	return err
}

// readResponse reads a response from the reader.
func readResponse(r io.Reader) (Response, error) {
	var lenbuf [packetLengthBytes]byte

	respBytes, err := read(r, lenbuf[:])
	if err != nil {
		return Response{}, fmt.Errorf("read error: %w", err)
	}

	resp := Response{buf: smallBuf{b: respBytes}}
	err = resp.decodeHeader(msgpack.NewDecoder(&smallBuf{}))
	if err != nil {
		return resp, fmt.Errorf("decode response header error: %w", err)
	}

	err = resp.decodeBody()
	if err != nil {
		switch err.(type) {
		case Error:
			return resp, err
		default:
			return resp, fmt.Errorf("decode response body error: %w", err)
		}
	}
	return resp, nil
}
