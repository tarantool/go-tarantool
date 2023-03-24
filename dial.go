package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	dialTransportNone = ""
	dialTransportSsl  = "ssl"
)

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
	// LocalAddr returns the local network address, if known.
	LocalAddr() net.Addr
	// RemoteAddr returns the remote network address, if known.
	RemoteAddr() net.Addr
	// Greeting returns server greeting.
	Greeting() Greeting
	// ProtocolInfo returns server protocol info.
	ProtocolInfo() ProtocolInfo
}

// DialOpts is a way to configure a Dial method to create a new Conn.
type DialOpts struct {
	// DialTimeout is a timeout for an initial network dial.
	DialTimeout time.Duration
	// IoTimeout is a timeout per a network read/write.
	IoTimeout time.Duration
	// Transport is a connect transport type.
	Transport string
	// Ssl configures "ssl" transport.
	Ssl SslOpts
	// RequiredProtocol contains minimal protocol version and
	// list of protocol features that should be supported by
	// Tarantool server. By default there are no restrictions.
	RequiredProtocol ProtocolInfo
	// Auth is an authentication method.
	Auth Auth
	// Username for logging in to Tarantool.
	User string
	// User password for logging in to Tarantool.
	Password string
}

// Dialer is the interface that wraps a method to connect to a Tarantool
// instance. The main idea is to provide a ready-to-work connection with
// basic preparation, successful authorization and additional checks.
//
// You can provide your own implementation to Connect() call via Opts.Dialer if
// some functionality is not implemented in the connector. See TtDialer.Dial()
// implementation as example.
type Dialer interface {
	// Dial connects to a Tarantool instance to the address with specified
	// options.
	Dial(address string, opts DialOpts) (Conn, error)
}

type tntConn struct {
	net      net.Conn
	reader   io.Reader
	writer   writeFlusher
	greeting Greeting
	protocol ProtocolInfo
}

// TtDialer is a default implementation of the Dialer interface which is
// used by the connector.
type TtDialer struct {
}

// Dial connects to a Tarantool instance to the address with specified
// options.
func (t TtDialer) Dial(address string, opts DialOpts) (Conn, error) {
	var err error
	conn := new(tntConn)

	if conn.net, err = dial(address, opts); err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	dc := &deadlineIO{to: opts.IoTimeout, c: conn.net}
	conn.reader = bufio.NewReaderSize(dc, 128*1024)
	conn.writer = bufio.NewWriterSize(dc, 128*1024)

	var version, salt string
	if version, salt, err = readGreeting(conn.reader); err != nil {
		conn.net.Close()
		return nil, fmt.Errorf("failed to read greeting: %w", err)
	}
	conn.greeting.Version = version

	if conn.protocol, err = identify(conn.writer, conn.reader); err != nil {
		conn.net.Close()
		return nil, fmt.Errorf("failed to identify: %w", err)
	}

	if err = checkProtocolInfo(opts.RequiredProtocol, conn.protocol); err != nil {
		conn.net.Close()
		return nil, fmt.Errorf("invalid server protocol: %w", err)
	}

	if opts.User != "" {
		if opts.Auth == AutoAuth {
			if conn.protocol.Auth != AutoAuth {
				opts.Auth = conn.protocol.Auth
			} else {
				opts.Auth = ChapSha1Auth
			}
		}

		err := authenticate(conn, opts, salt)
		if err != nil {
			conn.net.Close()
			return nil, fmt.Errorf("failed to authenticate: %w", err)
		}
	}

	return conn, nil
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

// RemoteAddr makes tntConn satisfy the Conn interface.
func (c *tntConn) RemoteAddr() net.Addr {
	return c.net.RemoteAddr()
}

// LocalAddr makes tntConn satisfy the Conn interface.
func (c *tntConn) LocalAddr() net.Addr {
	return c.net.LocalAddr()
}

// Greeting makes tntConn satisfy the Conn interface.
func (c *tntConn) Greeting() Greeting {
	return c.greeting
}

// ProtocolInfo makes tntConn satisfy the Conn interface.
func (c *tntConn) ProtocolInfo() ProtocolInfo {
	return c.protocol
}

// dial connects to a Tarantool instance.
func dial(address string, opts DialOpts) (net.Conn, error) {
	network, address := parseAddress(address)
	switch opts.Transport {
	case dialTransportNone:
		return net.DialTimeout(network, address, opts.DialTimeout)
	case dialTransportSsl:
		return sslDialTimeout(network, address, opts.DialTimeout, opts.Ssl)
	default:
		return nil, fmt.Errorf("unsupported transport type: %s", opts.Transport)
	}
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

// authenticate authenticate for a connection.
func authenticate(c Conn, opts DialOpts, salt string) error {
	auth := opts.Auth
	user := opts.User
	pass := opts.Password

	var req Request
	var err error

	switch opts.Auth {
	case ChapSha1Auth:
		req, err = newChapSha1AuthRequest(user, pass, salt)
		if err != nil {
			return err
		}
	case PapSha256Auth:
		if opts.Transport != dialTransportSsl {
			return errors.New("forbidden to use " + auth.String() +
				" unless SSL is enabled for the connection")
		}
		req = newPapSha256AuthRequest(user, pass)
	default:
		return errors.New("unsupported method " + opts.Auth.String())
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
