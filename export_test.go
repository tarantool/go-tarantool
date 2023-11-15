package tarantool

import (
	"context"
	"net"
	"time"

	"github.com/vmihailenco/msgpack/v5"
)

func SslDialContext(ctx context.Context, network, address string,
	opts SslOpts) (connection net.Conn, err error) {
	return sslDialContext(ctx, network, address, opts)
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
func RefImplSelectBody(enc *msgpack.Encoder, res SchemaResolver, space, index interface{},
	offset, limit uint32, iterator Iter, key, after interface{}, fetchPos bool) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	indexEnc, err := newIndexEncoder(res, index, spaceEnc.Id)
	if err != nil {
		return err
	}
	return fillSelect(enc, spaceEnc, indexEnc, offset, limit, iterator, key, after, fetchPos)
}

// RefImplInsertBody is reference implementation for filling of an insert
// request's body.
func RefImplInsertBody(enc *msgpack.Encoder, res SchemaResolver, space,
	tuple interface{}) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	return fillInsert(enc, spaceEnc, tuple)
}

// RefImplReplaceBody is reference implementation for filling of a replace
// request's body.
func RefImplReplaceBody(enc *msgpack.Encoder, res SchemaResolver, space,
	tuple interface{}) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	return fillInsert(enc, spaceEnc, tuple)
}

// RefImplDeleteBody is reference implementation for filling of a delete
// request's body.
func RefImplDeleteBody(enc *msgpack.Encoder, res SchemaResolver, space, index,
	key interface{}) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	indexEnc, err := newIndexEncoder(res, index, spaceEnc.Id)
	if err != nil {
		return err
	}
	return fillDelete(enc, spaceEnc, indexEnc, key)
}

// RefImplUpdateBody is reference implementation for filling of an update
// request's body.
func RefImplUpdateBody(enc *msgpack.Encoder, res SchemaResolver, space, index,
	key interface{}, ops *Operations) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	indexEnc, err := newIndexEncoder(res, index, spaceEnc.Id)
	if err != nil {
		return err
	}
	return fillUpdate(enc, spaceEnc, indexEnc, key, ops)
}

// RefImplUpsertBody is reference implementation for filling of an upsert
// request's body.
func RefImplUpsertBody(enc *msgpack.Encoder, res SchemaResolver, space,
	tuple interface{}, ops *Operations) error {
	spaceEnc, err := newSpaceEncoder(res, space)
	if err != nil {
		return err
	}
	return fillUpsert(enc, spaceEnc, tuple, ops)
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

// RefImplPrepareBody is reference implementation for filling of an prepare
// request's body.
func RefImplPrepareBody(enc *msgpack.Encoder, expr string) error {
	return fillPrepare(enc, expr)
}

// RefImplUnprepareBody is reference implementation for filling of an execute prepared
// request's body.
func RefImplExecutePreparedBody(enc *msgpack.Encoder, stmt Prepared, args interface{}) error {
	return fillExecutePrepared(enc, stmt, args)
}

// RefImplUnprepareBody is reference implementation for filling of an unprepare
// request's body.
func RefImplUnprepareBody(enc *msgpack.Encoder, stmt Prepared) error {
	return fillUnprepare(enc, stmt)
}

// RefImplBeginBody is reference implementation for filling of an begin
// request's body.
func RefImplBeginBody(enc *msgpack.Encoder, txnIsolation TxnIsolationLevel,
	timeout time.Duration) error {
	return fillBegin(enc, txnIsolation, timeout)
}

// RefImplCommitBody is reference implementation for filling of an commit
// request's body.
func RefImplCommitBody(enc *msgpack.Encoder) error {
	return fillCommit(enc)
}

// RefImplRollbackBody is reference implementation for filling of an rollback
// request's body.
func RefImplRollbackBody(enc *msgpack.Encoder) error {
	return fillRollback(enc)
}

// RefImplIdBody is reference implementation for filling of an id
// request's body.
func RefImplIdBody(enc *msgpack.Encoder, protocolInfo ProtocolInfo) error {
	return fillId(enc, protocolInfo)
}

// RefImplWatchOnceBody is reference implementation for filling of an watchOnce
// request's body.
func RefImplWatchOnceBody(enc *msgpack.Encoder, key string) error {
	return fillWatchOnce(enc, key)
}
