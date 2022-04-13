package tarantool

import (
	"net"
	"time"

	"gopkg.in/vmihailenco/msgpack.v2"
)

func SslDialTimeout(network, address string, timeout time.Duration,
	opts SslOpts) (connection net.Conn, err error) {
	return sslDialTimeout(network, address, timeout, opts)
}

func SslCreateContext(opts SslOpts) (ctx interface{}, err error) {
	return sslCreateContext(opts)
}

// RefImplPingBody is reference implementation for filling of a ping
// request's body.
func RefImplPingBody(enc *msgpack.Encoder) error {
	return fillPing(enc)
}

// RefImplSelectBody is reference implementation for filling of a select
// request's body.
func RefImplSelectBody(enc *msgpack.Encoder, space, index, offset, limit, iterator uint32, key interface{}) error {
	return fillSelect(enc, space, index, offset, limit, iterator, key)
}

// RefImplInsertBody is reference implementation for filling of an insert
// request's body.
func RefImplInsertBody(enc *msgpack.Encoder, space uint32, tuple interface{}) error {
	return fillInsert(enc, space, tuple)
}

// RefImplReplaceBody is reference implementation for filling of a replace
// request's body.
func RefImplReplaceBody(enc *msgpack.Encoder, space uint32, tuple interface{}) error {
	return fillInsert(enc, space, tuple)
}

// RefImplDeleteBody is reference implementation for filling of a delete
// request's body.
func RefImplDeleteBody(enc *msgpack.Encoder, space, index uint32, key interface{}) error {
	return fillDelete(enc, space, index, key)
}

// RefImplUpdateBody is reference implementation for filling of an update
// request's body.
func RefImplUpdateBody(enc *msgpack.Encoder, space, index uint32, key, ops interface{}) error {
	return fillUpdate(enc, space, index, key, ops)
}

// RefImplUpsertBody is reference implementation for filling of an upsert
// request's body.
func RefImplUpsertBody(enc *msgpack.Encoder, space uint32, tuple, ops interface{}) error {
	return fillUpsert(enc, space, tuple, ops)
}

// RefImplCallBody is reference implementation for filling of a call or call17
// request's body.
func RefImplCallBody(enc *msgpack.Encoder, function string, args interface{}) error {
	return fillCall(enc, function, args)
}

// RefImplEvalBody is reference implementation for filling of an eval
// request's body.
func RefImplEvalBody(enc *msgpack.Encoder, expr string, args interface{}) error {
	return fillEval(enc, expr, args)
}

// RefImplExecuteBody is reference implementation for filling of an execute
// request's body.
func RefImplExecuteBody(enc *msgpack.Encoder, expr string, args interface{}) error {
	return fillExecute(enc, expr, args)
}
