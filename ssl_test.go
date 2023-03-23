//go:build !go_tarantool_ssl_disable
// +build !go_tarantool_ssl_disable

package tarantool_test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tarantool/go-openssl"

	. "github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

const sslHost = "127.0.0.1"
const tntHost = "127.0.0.1:3014"

func serverSsl(network, address string, opts SslOpts) (net.Listener, error) {
	ctx, err := SslCreateContext(opts)
	if err != nil {
		return nil, errors.New("Unable to create SSL context: " + err.Error())
	}

	return openssl.Listen(network, address, ctx.(*openssl.Ctx))
}

func serverSslAccept(l net.Listener) (<-chan string, <-chan error) {
	message := make(chan string, 1)
	errors := make(chan error, 1)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			errors <- err
		} else {
			bytes, err := ioutil.ReadAll(conn)
			if err != nil {
				errors <- err
			} else {
				message <- string(bytes)
			}
			conn.Close()
		}

		close(message)
		close(errors)
	}()

	return message, errors
}

func serverSslRecv(msgs <-chan string, errs <-chan error) (string, error) {
	return <-msgs, <-errs
}

func clientSsl(network, address string, opts SslOpts) (net.Conn, error) {
	timeout := 5 * time.Second
	return SslDialTimeout(network, address, timeout, opts)
}

func createClientServerSsl(t testing.TB, serverOpts,
	clientOpts SslOpts) (net.Listener, net.Conn, error, <-chan string, <-chan error) {
	t.Helper()

	l, err := serverSsl("tcp", sslHost+":0", serverOpts)
	if err != nil {
		t.Fatalf("Unable to create server, error %q", err.Error())
	}

	msgs, errs := serverSslAccept(l)

	port := l.Addr().(*net.TCPAddr).Port
	c, err := clientSsl("tcp", sslHost+":"+strconv.Itoa(port), clientOpts)

	return l, c, err, msgs, errs
}

func createClientServerSslOk(t testing.TB, serverOpts,
	clientOpts SslOpts) (net.Listener, net.Conn, <-chan string, <-chan error) {
	t.Helper()

	l, c, err, msgs, errs := createClientServerSsl(t, serverOpts, clientOpts)
	if err != nil {
		t.Fatalf("Unable to create client, error %q", err.Error())
	}

	return l, c, msgs, errs
}

func serverTnt(serverOpts, clientOpts SslOpts, auth Auth) (test_helpers.TarantoolInstance, error) {
	listen := tntHost + "?transport=ssl&"

	key := serverOpts.KeyFile
	if key != "" {
		listen += fmt.Sprintf("ssl_key_file=%s&", key)
	}

	cert := serverOpts.CertFile
	if cert != "" {
		listen += fmt.Sprintf("ssl_cert_file=%s&", cert)
	}

	ca := serverOpts.CaFile
	if ca != "" {
		listen += fmt.Sprintf("ssl_ca_file=%s&", ca)
	}

	ciphers := serverOpts.Ciphers
	if ciphers != "" {
		listen += fmt.Sprintf("ssl_ciphers=%s&", ciphers)
	}

	listen = listen[:len(listen)-1]

	return test_helpers.StartTarantool(test_helpers.StartOpts{
		Auth:            auth,
		InitScript:      "config.lua",
		Listen:          listen,
		SslCertsDir:     "testdata",
		ClientServer:    tntHost,
		ClientTransport: "ssl",
		ClientSsl:       clientOpts,
		User:            "test",
		Pass:            "test",
		WaitStart:       100 * time.Millisecond,
		ConnectRetry:    3,
		RetryTimeout:    500 * time.Millisecond,
	})
}

func serverTntStop(inst test_helpers.TarantoolInstance) {
	test_helpers.StopTarantoolWithCleanup(inst)
}

func assertConnectionSslFail(t testing.TB, serverOpts, clientOpts SslOpts) {
	t.Helper()

	l, c, err, _, _ := createClientServerSsl(t, serverOpts, clientOpts)
	l.Close()
	if err == nil {
		c.Close()
		t.Errorf("An unexpected connection to the server.")
	}
}

func assertConnectionSslOk(t testing.TB, serverOpts, clientOpts SslOpts) {
	t.Helper()

	l, c, msgs, errs := createClientServerSslOk(t, serverOpts, clientOpts)
	const message = "any test string"
	c.Write([]byte(message))
	c.Close()

	recv, err := serverSslRecv(msgs, errs)
	l.Close()

	if err != nil {
		t.Errorf("An unexpected server error: %q", err.Error())
	} else if recv != message {
		t.Errorf("An unexpected server message: %q, expected %q", recv, message)
	}
}

func assertConnectionTntFail(t testing.TB, serverOpts, clientOpts SslOpts) {
	t.Helper()

	inst, err := serverTnt(serverOpts, clientOpts, AutoAuth)
	serverTntStop(inst)

	if err == nil {
		t.Errorf("An unexpected connection to the server")
	}
}

func assertConnectionTntOk(t testing.TB, serverOpts, clientOpts SslOpts) {
	t.Helper()

	inst, err := serverTnt(serverOpts, clientOpts, AutoAuth)
	serverTntStop(inst)

	if err != nil {
		t.Errorf("An unexpected server error %q", err.Error())
	}
}

type test struct {
	name       string
	ok         bool
	serverOpts SslOpts
	clientOpts SslOpts
}

/*
Requirements from Tarantool Enterprise Edition manual:
https://www.tarantool.io/ru/enterprise_doc/security/#configuration

For a server:
KeyFile - mandatory
CertFile - mandatory
CaFile - optional
Ciphers - optional

For a client:
KeyFile - optional, mandatory if server.CaFile set
CertFile - optional, mandatory if server.CaFile set
CaFile - optional,
Ciphers - optional
*/
var tests = []test{
	{
		"empty",
		false,
		SslOpts{},
		SslOpts{},
	},
	{
		"key_crt_client",
		false,
		SslOpts{},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"key_crt_server",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslOpts{},
	},
	{
		"key_crt_server_and_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"key_crt_ca_server",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{},
	},
	{
		"key_crt_ca_server_key_crt_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"key_crt_ca_server_and_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_key",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "any_invalid_path",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_crt",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "any_invalid_path",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_ca",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "any_invalid_path",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_key",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/empty",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_crt",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/empty",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_ca",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/empty",
		},
	},
	{
		"key_crt_server_and_key_crt_ca_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_ciphers_server_key_crt_ca_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_ciphers_server_and_client",
		true,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
	},
	{
		"non_equal_ciphers_client",
		false,
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "TLS_AES_128_GCM_SHA256",
		},
	},
}

func isTestTntSsl() bool {
	testTntSsl, exists := os.LookupEnv("TEST_TNT_SSL")
	return exists &&
		(testTntSsl == "1" || strings.ToUpper(testTntSsl) == "TRUE")
}

func TestSslOpts(t *testing.T) {
	isTntSsl := isTestTntSsl()

	for _, test := range tests {
		if test.ok {
			t.Run("ok_ssl_"+test.name, func(t *testing.T) {
				assertConnectionSslOk(t, test.serverOpts, test.clientOpts)
			})
		} else {
			t.Run("fail_ssl_"+test.name, func(t *testing.T) {
				assertConnectionSslFail(t, test.serverOpts, test.clientOpts)
			})
		}
		if !isTntSsl {
			continue
		}
		if test.ok {
			t.Run("ok_tnt_"+test.name, func(t *testing.T) {
				assertConnectionTntOk(t, test.serverOpts, test.clientOpts)
			})
		} else {
			t.Run("fail_tnt_"+test.name, func(t *testing.T) {
				assertConnectionTntFail(t, test.serverOpts, test.clientOpts)
			})
		}
	}
}

func TestOpts_PapSha256Auth(t *testing.T) {
	isTntSsl := isTestTntSsl()
	if !isTntSsl {
		t.Skip("TEST_TNT_SSL is not set")
	}

	isLess, err := test_helpers.IsTarantoolVersionLess(2, 11, 0)
	if err != nil {
		t.Fatalf("Could not check Tarantool version.")
	}
	if isLess {
		t.Skip("Skipping test for Tarantoo without pap-sha256 support")
	}

	sslOpts := SslOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
	}
	inst, err := serverTnt(sslOpts, sslOpts, PapSha256Auth)
	defer serverTntStop(inst)
	if err != nil {
		t.Errorf("An unexpected server error: %s", err)
	}

	clientOpts := opts
	clientOpts.Transport = "ssl"
	clientOpts.Ssl = sslOpts
	clientOpts.Auth = PapSha256Auth
	conn := test_helpers.ConnectWithValidation(t, tntHost, clientOpts)
	conn.Close()

	clientOpts.Auth = AutoAuth
	conn = test_helpers.ConnectWithValidation(t, tntHost, clientOpts)
	conn.Close()
}
