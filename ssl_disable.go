//go:build go_tarantool_ssl_disable
// +build go_tarantool_ssl_disable

package tarantool

import (
	"context"
	"errors"
	"net"
)

func sslDialContext(ctx context.Context, network, address string,
	opts SslOpts) (connection net.Conn, err error) {
	return nil, errors.New("SSL support is disabled.")
}

func sslCreateContext(opts SslOpts) (ctx interface{}, err error) {
	return nil, errors.New("SSL support is disabled.")
}
