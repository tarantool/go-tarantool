//go:build !go_tarantool_ssl_disable
// +build !go_tarantool_ssl_disable

package tarantool

import (
	"bufio"
	"context"
	"errors"
	"io/ioutil"
	"net"
	"os"
	"strings"

	"github.com/tarantool/go-openssl"
)

func sslDialContext(ctx context.Context, network, address string,
	opts sslOpts) (connection net.Conn, err error) {
	var sslCtx interface{}
	if sslCtx, err = sslCreateContext(opts); err != nil {
		return
	}

	return openssl.DialContext(ctx, network, address, sslCtx.(*openssl.Ctx), 0)
}

// interface{} is a hack. It helps to avoid dependency of go-openssl in build
// of tests with the tag 'go_tarantool_ssl_disable'.
func sslCreateContext(opts sslOpts) (ctx interface{}, err error) {
	var sslCtx *openssl.Ctx

	// Require TLSv1.2, because other protocol versions don't seem to
	// support the GOST cipher.
	if sslCtx, err = openssl.NewCtxWithVersion(openssl.TLSv1_2); err != nil {
		return
	}
	ctx = sslCtx
	sslCtx.SetMaxProtoVersion(openssl.TLS1_2_VERSION)
	sslCtx.SetMinProtoVersion(openssl.TLS1_2_VERSION)

	if opts.CertFile != "" {
		if err = sslLoadCert(sslCtx, opts.CertFile); err != nil {
			return
		}
	}

	if opts.KeyFile != "" {
		if err = sslLoadKey(sslCtx, opts.KeyFile, opts.Password, opts.PasswordFile); err != nil {
			return
		}
	}

	if opts.CaFile != "" {
		if err = sslCtx.LoadVerifyLocations(opts.CaFile, ""); err != nil {
			return
		}
		verifyFlags := openssl.VerifyPeer | openssl.VerifyFailIfNoPeerCert
		sslCtx.SetVerify(verifyFlags, nil)
	}

	if opts.Ciphers != "" {
		sslCtx.SetCipherList(opts.Ciphers)
	}

	return
}

func sslLoadCert(ctx *openssl.Ctx, certFile string) (err error) {
	var certBytes []byte
	if certBytes, err = ioutil.ReadFile(certFile); err != nil {
		return
	}

	certs := openssl.SplitPEM(certBytes)
	if len(certs) == 0 {
		err = errors.New("No PEM certificate found in " + certFile)
		return
	}
	first, certs := certs[0], certs[1:]

	var cert *openssl.Certificate
	if cert, err = openssl.LoadCertificateFromPEM(first); err != nil {
		return
	}
	if err = ctx.UseCertificate(cert); err != nil {
		return
	}

	for _, pem := range certs {
		if cert, err = openssl.LoadCertificateFromPEM(pem); err != nil {
			break
		}
		if err = ctx.AddChainCertificate(cert); err != nil {
			break
		}
	}
	return
}

func sslLoadKey(ctx *openssl.Ctx, keyFile string, password string,
	passwordFile string) error {
	var keyBytes []byte
	var err, firstDecryptErr error

	if keyBytes, err = ioutil.ReadFile(keyFile); err != nil {
		return err
	}

	// If the key is encrypted and password is not provided,
	// openssl.LoadPrivateKeyFromPEM(keyBytes) asks to enter PEM pass phrase
	// interactively. On the other hand,
	// openssl.LoadPrivateKeyFromPEMWithPassword(keyBytes, password) works fine
	// for non-encrypted key with any password, including empty string. If
	// the key is encrypted, we fast fail with password error instead of
	// requesting the pass phrase interactively.
	passwords := []string{password}
	if passwordFile != "" {
		file, err := os.Open(passwordFile)
		if err == nil {
			defer file.Close()

			scanner := bufio.NewScanner(file)
			// Tarantool itself tries each password file line.
			for scanner.Scan() {
				password = strings.TrimSpace(scanner.Text())
				passwords = append(passwords, password)
			}
		} else {
			firstDecryptErr = err
		}
	}

	for _, password := range passwords {
		key, err := openssl.LoadPrivateKeyFromPEMWithPassword(keyBytes, password)
		if err == nil {
			return ctx.UsePrivateKey(key)
		} else if firstDecryptErr == nil {
			firstDecryptErr = err
		}
	}

	return firstDecryptErr
}
