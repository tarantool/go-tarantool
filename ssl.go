//go:build !go_tarantool_ssl_disable
// +build !go_tarantool_ssl_disable

package tarantool

import (
	"errors"
	"io/ioutil"
	"net"
	"time"

	"github.com/tarantool/go-openssl"
)

func sslDialTimeout(network, address string, timeout time.Duration,
	opts SslOpts) (connection net.Conn, err error) {
	var ctx interface{}
	if ctx, err = sslCreateContext(opts); err != nil {
		return
	}

	return openssl.DialTimeout(network, address, timeout, ctx.(*openssl.Ctx), 0)
}

// interface{} is a hack. It helps to avoid dependency of go-openssl in build
// of tests with the tag 'go_tarantool_ssl_disable'.
func sslCreateContext(opts SslOpts) (ctx interface{}, err error) {
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
		if err = sslLoadKey(sslCtx, opts.KeyFile); err != nil {
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

func sslLoadKey(ctx *openssl.Ctx, keyFile string) (err error) {
	var keyBytes []byte
	if keyBytes, err = ioutil.ReadFile(keyFile); err != nil {
		return
	}

	var key openssl.PrivateKey
	if key, err = openssl.LoadPrivateKeyFromPEM(keyBytes); err != nil {
		return
	}

	return ctx.UsePrivateKey(key)
}
