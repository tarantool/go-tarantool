package tarantool

import (
	"context"
	"fmt"

	"gopkg.in/vmihailenco/msgpack.v2"
)

// PreparedID is a type for Prepared Statement ID
type PreparedID uint64

// Prepared is a type for handling prepared statements
//
// Since 1.7.0
type Prepared struct {
	StatementID PreparedID
	MetaData    []ColumnMetaData
	ParamCount  uint64
	Conn        *Connection
}

func fillPrepare(enc *msgpack.Encoder, expr string) error {
	enc.EncodeMapLen(1)
	enc.EncodeUint(KeySQLText)
	return enc.EncodeString(expr)
}

func fillUnprepare(enc *msgpack.Encoder, stmt Prepared) error {
	enc.EncodeMapLen(1)
	enc.EncodeUint(KeyStmtID)
	return enc.EncodeUint(uint(stmt.StatementID))
}

func fillExecutePrepared(enc *msgpack.Encoder, stmt Prepared, args interface{}) error {
	enc.EncodeMapLen(2)
	enc.EncodeUint(KeyStmtID)
	enc.EncodeUint(uint(stmt.StatementID))
	enc.EncodeUint(KeySQLBind)
	return encodeSQLBind(enc, args)
}

// NewPreparedFromResponse constructs a Prepared object.
func NewPreparedFromResponse(conn *Connection, resp *Response) (*Prepared, error) {
	if resp == nil {
		return nil, fmt.Errorf("pased nil response")
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("response Data is nil")
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("response Data format is wrong")
	}
	stmt, ok := resp.Data[0].(*Prepared)
	if !ok {
		return nil, fmt.Errorf("response Data format is wrong")
	}
	stmt.Conn = conn
	return stmt, nil
}

// PrepareRequest helps you to create a prepare request object for execution
// by a Connection.
type PrepareRequest struct {
	baseRequest
	expr string
}

// NewPrepareRequest returns a new empty PrepareRequest.
func NewPrepareRequest(expr string) *PrepareRequest {
	req := new(PrepareRequest)
	req.requestCode = PrepareRequestCode
	req.expr = expr
	return req
}

// Body fills an encoder with the execute request body.
func (req *PrepareRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillPrepare(enc, req.expr)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *PrepareRequest) Context(ctx context.Context) *PrepareRequest {
	req.ctx = ctx
	return req
}

// UnprepareRequest helps you to create an unprepare request object for
// execution by a Connection.
type UnprepareRequest struct {
	baseRequest
	stmt *Prepared
}

// NewUnprepareRequest returns a new empty UnprepareRequest.
func NewUnprepareRequest(stmt *Prepared) *UnprepareRequest {
	req := new(UnprepareRequest)
	req.requestCode = PrepareRequestCode
	req.stmt = stmt
	return req
}

// Conn returns the Connection object the request belongs to
func (req *UnprepareRequest) Conn() *Connection {
	return req.stmt.Conn
}

// Body fills an encoder with the execute request body.
func (req *UnprepareRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillUnprepare(enc, *req.stmt)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *UnprepareRequest) Context(ctx context.Context) *UnprepareRequest {
	req.ctx = ctx
	return req
}

// ExecutePreparedRequest helps you to create an execute prepared request
// object for execution by a Connection.
type ExecutePreparedRequest struct {
	baseRequest
	stmt *Prepared
	args interface{}
}

// NewExecutePreparedRequest returns a new empty preparedExecuteRequest.
func NewExecutePreparedRequest(stmt *Prepared) *ExecutePreparedRequest {
	req := new(ExecutePreparedRequest)
	req.requestCode = ExecuteRequestCode
	req.stmt = stmt
	req.args = []interface{}{}
	return req
}

// Conn returns the Connection object the request belongs to
func (req *ExecutePreparedRequest) Conn() *Connection {
	return req.stmt.Conn
}

// Args sets the args for execute the prepared request.
// Note: default value is empty.
func (req *ExecutePreparedRequest) Args(args interface{}) *ExecutePreparedRequest {
	req.args = args
	return req
}

// Body fills an encoder with the execute request body.
func (req *ExecutePreparedRequest) Body(res SchemaResolver, enc *msgpack.Encoder) error {
	return fillExecutePrepared(enc, *req.stmt, req.args)
}

// Context sets a passed context to the request.
//
// Pay attention that when using context with request objects,
// the timeout option for Connection does not affect the lifetime
// of the request. For those purposes use context.WithTimeout() as
// the root context.
func (req *ExecutePreparedRequest) Context(ctx context.Context) *ExecutePreparedRequest {
	req.ctx = ctx
	return req
}
