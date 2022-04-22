package tarantool

import (
	"net"
	"time"
)

func (schema *Schema) ResolveSpaceIndex(s interface{}, i interface{}) (spaceNo, indexNo uint32, err error) {
	return schema.resolveSpaceIndex(s, i)
}

func SslDialTimeout(network, address string, timeout time.Duration,
	opts SslOpts) (connection net.Conn, err error) {
	return sslDialTimeout(network, address, timeout, opts)
}

func SslCreateContext(opts SslOpts) (ctx interface{}, err error) {
	return sslCreateContext(opts)
}
