package tarantool

import (
	"context"
	"errors"
	"time"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
)

type TxnIsolationLevel uint

const (
	// By default, the isolation level of Tarantool is serializable.
	DefaultIsolationLevel TxnIsolationLevel = 0
	// The ReadCommittedLevel isolation level makes visible all transactions
	// that started commit (stream.Do(NewCommitRequest()) was called).
	ReadCommittedLevel TxnIsolationLevel = 1
	// The ReadConfirmedLevel isolation level makes visible all transactions
	// that finished the commit (stream.Do(NewCommitRequest()) was returned).
	ReadConfirmedLevel TxnIsolationLevel = 2
	// If the BestEffortLevel (serializable) isolation level becomes unreachable,
	// the transaction is marked as «conflicted» and can no longer be committed.
	BestEffortLevel TxnIsolationLevel = 3
)

var (
	errUnknownStreamRequest = errors.New("the passed connected request doesn't belong " +
		"to the current connection or connection pool")
)

type Stream struct {
	Id   uint64
	Conn *Connection
}

// BeginRequest helps you to create a begin request object for execution
// by a Stream.
// Begin request can not be processed out of stream.
type BeginRequest struct {
	baseRequest
	txnIsolation TxnIsolationLevel
	timeout      time.Duration
	isSync       bool
	isSyncSet    bool
}

// NewBeginRequest returns a new BeginRequest.
func NewBeginRequest() *BeginRequest {
	req := new(BeginRequest)
	req.rtype = iproto.IPROTO_BEGIN
	req.txnIsolation = DefaultIsolationLevel
	return req
}

// TxnIsolation sets the the transaction isolation level for transaction manager.
// By default, the isolation level of Tarantool is serializable.
func (req *BeginRequest) TxnIsolation(txnIsolation TxnIsolationLevel) *BeginRequest {
	req.txnIsolation = txnIsolation
	return req
}

// Timeout allows to set up a timeout for call BeginRequest.
func (req *BeginRequest) Timeout(timeout time.Duration) *BeginRequest {
	req.timeout = timeout
	return req
}

// IsSync allows to set up a IsSync flag for call BeginRequest.
func (req *BeginRequest) IsSync(isSync bool) *BeginRequest {
	req.isSync = isSync
	req.isSyncSet = true
	return req
}

// Body fills an msgpack.Encoder with the begin request body.
func (req *BeginRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	var (
		mapLen            = 0
		hasTimeout        = req.timeout > 0
		hasIsolationLevel = req.txnIsolation != DefaultIsolationLevel
	)

	if hasTimeout {
		mapLen++
	}

	if hasIsolationLevel {
		mapLen++
	}

	if req.isSyncSet {
		mapLen++
	}

	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}

	if hasTimeout {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_TIMEOUT)); err != nil {
			return err
		}

		if err := enc.Encode(req.timeout.Seconds()); err != nil {
			return err
		}
	}

	if hasIsolationLevel {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_TXN_ISOLATION)); err != nil {
			return err
		}

		if err := enc.EncodeUint(uint64(req.txnIsolation)); err != nil {
			return err
		}
	}

	if req.isSyncSet {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_IS_SYNC)); err != nil {
			return err
		}

		if err := enc.EncodeBool(req.isSync); err != nil {
			return err
		}
	}

	return nil
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *BeginRequest) Context(ctx context.Context) *BeginRequest {
	req.ctx = ctx
	return req
}

// CommitRequest helps you to create a commit request object for execution
// by a Stream.
// Commit request can not be processed out of stream.
type CommitRequest struct {
	baseRequest

	isSync    bool
	isSyncSet bool
}

// NewCommitRequest returns a new CommitRequest.
func NewCommitRequest() *CommitRequest {
	req := new(CommitRequest)
	req.rtype = iproto.IPROTO_COMMIT
	return req
}

// IsSync allows to set up a IsSync flag for call BeginRequest.
func (req *CommitRequest) IsSync(isSync bool) *CommitRequest {
	req.isSync = isSync
	req.isSyncSet = true
	return req
}

// Body fills an msgpack.Encoder with the commit request body.
func (req *CommitRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	var (
		mapLen = 0
	)

	if req.isSyncSet {
		mapLen++
	}

	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}

	if req.isSyncSet {
		if err := enc.EncodeUint(uint64(iproto.IPROTO_IS_SYNC)); err != nil {
			return err
		}

		if err := enc.EncodeBool(req.isSync); err != nil {
			return err
		}
	}

	return nil
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *CommitRequest) Context(ctx context.Context) *CommitRequest {
	req.ctx = ctx
	return req
}

// RollbackRequest helps you to create a rollback request object for execution
// by a Stream.
// Rollback request can not be processed out of stream.
type RollbackRequest struct {
	baseRequest
}

// NewRollbackRequest returns a new RollbackRequest.
func NewRollbackRequest() *RollbackRequest {
	req := new(RollbackRequest)
	req.rtype = iproto.IPROTO_ROLLBACK
	return req
}

// Body fills an msgpack.Encoder with the rollback request body.
func (req *RollbackRequest) Body(_ SchemaResolver, enc *msgpack.Encoder) error {
	return enc.EncodeMapLen(0)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *RollbackRequest) Context(ctx context.Context) *RollbackRequest {
	req.ctx = ctx
	return req
}

// Do verifies, sends the request and returns a future.
//
// An error is returned if the request was formed incorrectly, or failure to
// create the future.
func (s *Stream) Do(req Request) *Future {
	if connectedReq, ok := req.(ConnectedRequest); ok {
		if connectedReq.Conn() != s.Conn {
			fut := NewFuture(req)
			fut.SetError(errUnknownStreamRequest)
			return fut
		}
	}
	return s.Conn.send(req, s.Id)
}
