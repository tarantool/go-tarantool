package tarantool

import (
	"context"
	"fmt"
	"time"

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

type Stream struct {
	Id   uint64
	Conn *Connection
}

func fillBegin(enc *msgpack.Encoder, txnIsolation TxnIsolationLevel, timeout time.Duration) error {
	hasTimeout := timeout > 0
	hasIsolationLevel := txnIsolation != DefaultIsolationLevel
	mapLen := 0
	if hasTimeout {
		mapLen += 1
	}
	if hasIsolationLevel {
		mapLen += 1
	}

	err := enc.EncodeMapLen(mapLen)
	if err != nil {
		return err
	}

	if hasTimeout {
		err = enc.EncodeUint(KeyTimeout)
		if err != nil {
			return err
		}

		err = enc.Encode(timeout.Seconds())
		if err != nil {
			return err
		}
	}

	if hasIsolationLevel {
		err = enc.EncodeUint(KeyTxnIsolation)
		if err != nil {
			return err
		}

		err = enc.EncodeUint(uint64(txnIsolation))
		if err != nil {
			return err
		}
	}

	return err
}

func fillCommit(enc *msgpack.Encoder) error {
	return enc.EncodeMapLen(0)
}

func fillRollback(enc *msgpack.Encoder) error {
	return enc.EncodeMapLen(0)
}

// BeginRequest helps you to create a begin request object for execution
// by a Stream.
// Begin request can not be processed out of stream.
type BeginRequest struct {
	baseRequest
	txnIsolation TxnIsolationLevel
	timeout      time.Duration
}

// NewBeginRequest returns a new BeginRequest.
func NewBeginRequest() *BeginRequest {
	req := new(BeginRequest)
	req.requestCode = BeginRequestCode
	req.txnIsolation = DefaultIsolationLevel
	return req
}

// TxnIsolation sets the the transaction isolation level for transaction manager.
// By default, the isolation level of Tarantool is serializable.
func (req *BeginRequest) TxnIsolation(txnIsolation TxnIsolationLevel) *BeginRequest {
	req.txnIsolation = txnIsolation
	return req
}

// WithTimeout allows to set up a timeout for call BeginRequest.
func (req *BeginRequest) Timeout(timeout time.Duration) *BeginRequest {
	req.timeout = timeout
	return req
}

// Body fills an msgpack.Encoder with the begin request body.
func (req *BeginRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillBegin(enc, req.txnIsolation, req.timeout)
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
}

// NewCommitRequest returns a new CommitRequest.
func NewCommitRequest() *CommitRequest {
	req := new(CommitRequest)
	req.requestCode = CommitRequestCode
	return req
}

// Body fills an msgpack.Encoder with the commit request body.
func (req *CommitRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillCommit(enc)
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
	req.requestCode = RollbackRequestCode
	return req
}

// Body fills an msgpack.Encoder with the rollback request body.
func (req *RollbackRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillRollback(enc)
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
			fut := NewFuture()
			fut.SetError(fmt.Errorf("the passed connected request doesn't belong to the current connection or connection pool"))
			return fut
		}
	}
	return s.Conn.send(req, s.Id)
}
