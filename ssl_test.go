//go:build !go_tarantool_ssl_disable
// +build !go_tarantool_ssl_disable

package tarantool_test

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

const tntHost = "127.0.0.1:3014"

func serverTnt(serverOpts SslTestOpts, auth Auth) (test_helpers.TarantoolInstance, error) {
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

	password := serverOpts.Password
	if password != "" {
		listen += fmt.Sprintf("ssl_password=%s&", password)
	}

	passwordFile := serverOpts.PasswordFile
	if passwordFile != "" {
		listen += fmt.Sprintf("ssl_password_file=%s&", passwordFile)
	}

	listen = listen[:len(listen)-1]

	return test_helpers.StartTarantool(
		test_helpers.StartOpts{
			Dialer: OpenSslDialer{
				Address:         tntHost,
				Auth:            auth,
				User:            "test",
				Password:        "test",
				SslKeyFile:      serverOpts.KeyFile,
				SslCertFile:     serverOpts.CertFile,
				SslCaFile:       serverOpts.CaFile,
				SslCiphers:      serverOpts.Ciphers,
				SslPassword:     serverOpts.Password,
				SslPasswordFile: serverOpts.PasswordFile,
			},
			Auth:         auth,
			InitScript:   "config.lua",
			Listen:       listen,
			SslCertsDir:  "testdata",
			WaitStart:    100 * time.Millisecond,
			ConnectRetry: 10,
			RetryTimeout: 500 * time.Millisecond,
		},
	)
}

func serverTntStop(inst test_helpers.TarantoolInstance) {
	test_helpers.StopTarantoolWithCleanup(inst)
}

func checkTntConn(dialer Dialer) error {
	ctx, cancel := test_helpers.GetConnectContext()
	defer cancel()
	conn, err := Connect(ctx, dialer, Opts{
		Timeout:    500 * time.Millisecond,
		SkipSchema: true,
	})
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func assertConnectionTntFail(t testing.TB, serverOpts SslTestOpts, dialer OpenSslDialer) {
	t.Helper()

	inst, err := serverTnt(serverOpts, AutoAuth)
	defer serverTntStop(inst)
	if err != nil {
		t.Fatalf("An unexpected server error %q", err.Error())
	}

	err = checkTntConn(dialer)
	if err == nil {
		t.Errorf("An unexpected connection to the server")
	}
}

func assertConnectionTntOk(t testing.TB, serverOpts SslTestOpts, dialer OpenSslDialer) {
	t.Helper()

	inst, err := serverTnt(serverOpts, AutoAuth)
	defer serverTntStop(inst)
	if err != nil {
		t.Fatalf("An unexpected server error %q", err.Error())
	}

	err = checkTntConn(dialer)
	if err != nil {
		t.Errorf("An unexpected connection error %q", err.Error())
	}
}

type sslTest struct {
	name       string
	ok         bool
	serverOpts SslTestOpts
	clientOpts SslTestOpts
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
var sslTests = []sslTest{
	{
		"key_crt_server",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslTestOpts{},
	},
	{
		"key_crt_server_and_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"key_crt_ca_server",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{},
	},
	{
		"key_crt_ca_server_key_crt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"key_crt_ca_server_and_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_key",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "any_invalid_path",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_crt",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "any_invalid_path",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_invalid_path_ca",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "any_invalid_path",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_key",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/empty",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_crt",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/empty",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_server_and_client_empty_ca",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/empty",
		},
	},
	{
		"key_crt_server_and_key_crt_ca_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_ciphers_server_key_crt_ca_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
	},
	{
		"key_crt_ca_ciphers_server_and_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
	},
	{
		"non_equal_ciphers_client",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "ECDHE-RSA-AES256-GCM-SHA384",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
			Ciphers:  "TLS_AES_128_GCM_SHA256",
		},
	},
	{
		"pass_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.enc.key",
			CertFile: "testdata/localhost.crt",
			Password: "mysslpassword",
		},
	},
	{
		"passfile_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			PasswordFile: "testdata/passwords",
		},
	},
	{
		"pass_and_passfile_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			Password:     "mysslpassword",
			PasswordFile: "testdata/passwords",
		},
	},
	{
		"inv_pass_and_passfile_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			Password:     "invalidpassword",
			PasswordFile: "testdata/passwords",
		},
	},
	{
		"pass_and_inv_passfile_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			Password:     "mysslpassword",
			PasswordFile: "testdata/invalidpasswords",
		},
	},
	{
		"pass_and_not_existing_passfile_key_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			Password:     "mysslpassword",
			PasswordFile: "testdata/notafile",
		},
	},
	{
		"inv_pass_and_inv_passfile_key_encrypt_client",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			Password:     "invalidpassword",
			PasswordFile: "testdata/invalidpasswords",
		},
	},
	{
		"not_existing_passfile_key_encrypt_client",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.enc.key",
			CertFile:     "testdata/localhost.crt",
			PasswordFile: "testdata/notafile",
		},
	},
	{
		"no_pass_key_encrypt_client",
		false,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.enc.key",
			CertFile: "testdata/localhost.crt",
		},
	},
	{
		"pass_key_non_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			Password: "invalidpassword",
		},
	},
	{
		"passfile_key_non_encrypt_client",
		true,
		SslTestOpts{
			KeyFile:  "testdata/localhost.key",
			CertFile: "testdata/localhost.crt",
			CaFile:   "testdata/ca.crt",
		},
		SslTestOpts{
			KeyFile:      "testdata/localhost.key",
			CertFile:     "testdata/localhost.crt",
			PasswordFile: "testdata/invalidpasswords",
		},
	},
}

func isTestTntSsl() bool {
	testTntSsl, exists := os.LookupEnv("TEST_TNT_SSL")
	return exists &&
		(testTntSsl == "1" || strings.ToUpper(testTntSsl) == "TRUE")
}

func makeOpenSslDialer(opts SslTestOpts) OpenSslDialer {
	return OpenSslDialer{
		Address:         tntHost,
		User:            "test",
		Password:        "test",
		SslKeyFile:      opts.KeyFile,
		SslCertFile:     opts.CertFile,
		SslCaFile:       opts.CaFile,
		SslCiphers:      opts.Ciphers,
		SslPassword:     opts.Password,
		SslPasswordFile: opts.PasswordFile,
	}
}

func TestSslOpts(t *testing.T) {
	isTntSsl := isTestTntSsl()

	for _, test := range sslTests {
		if !isTntSsl {
			continue
		}
		dialer := makeOpenSslDialer(test.clientOpts)
		if test.ok {
			t.Run("ok_tnt_"+test.name, func(t *testing.T) {
				assertConnectionTntOk(t, test.serverOpts, dialer)
			})
		} else {
			t.Run("fail_tnt_"+test.name, func(t *testing.T) {
				assertConnectionTntFail(t, test.serverOpts, dialer)
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
		t.Fatalf("Could not check Tarantool version: %s", err)
	}
	if isLess {
		t.Skip("Skipping test for Tarantool without pap-sha256 support")
	}

	sslOpts := SslTestOpts{
		KeyFile:  "testdata/localhost.key",
		CertFile: "testdata/localhost.crt",
	}

	inst, err := serverTnt(sslOpts, PapSha256Auth)
	defer serverTntStop(inst)
	if err != nil {
		t.Fatalf("An unexpected server error %q", err.Error())
	}

	client := OpenSslDialer{
		Address:              tntHost,
		Auth:                 PapSha256Auth,
		User:                 "test",
		Password:             "test",
		RequiredProtocolInfo: ProtocolInfo{},
		SslKeyFile:           sslOpts.KeyFile,
		SslCertFile:          sslOpts.CertFile,
	}

	conn := test_helpers.ConnectWithValidation(t, client, opts)
	conn.Close()

	client.Auth = AutoAuth
	conn = test_helpers.ConnectWithValidation(t, client, opts)
	conn.Close()
}
