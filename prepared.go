package tarantool

import (
	"context"
	"fmt"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"
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
	enc.EncodeUint(uint64(iproto.IPROTO_SQL_TEXT))
	return enc.EncodeString(expr)
}

func fillUnprepare(enc *msgpack.Encoder, stmt Prepared) error {
	enc.EncodeMapLen(1)
	enc.EncodeUint(uint64(iproto.IPROTO_STMT_ID))
	return enc.EncodeUint(uint64(stmt.StatementID))
}

func fillExecutePrepared(enc *msgpack.Encoder, stmt Prepared, args interface{}) error {
	enc.EncodeMapLen(2)
	enc.EncodeUint(uint64(iproto.IPROTO_STMT_ID))
	enc.EncodeUint(uint64(stmt.StatementID))
	enc.EncodeUint(uint64(iproto.IPROTO_SQL_BIND))
	return encodeSQLBind(enc, args)
}

// NewPreparedFromResponse constructs a Prepared object.
func NewPreparedFromResponse(conn *Connection, resp Response) (*Prepared, error) {
	if resp == nil {
		return nil, fmt.Errorf("passed nil response")
	}
	data, err := resp.Decode()
	if err != nil {
		return nil, fmt.Errorf("decode response body error: %s", err.Error())
	}
	if data == nil {
		return nil, fmt.Errorf("response Data is nil")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("response Data format is wrong")
	}
	stmt, ok := data[0].(*Prepared)
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
	req.rtype = iproto.IPROTO_PREPARE
	req.expr = expr
	return req
}

// Body fills an msgpack.Encoder with the execute request body.
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

// Response creates a response for the PrepareRequest.
func (req *PrepareRequest) Response(header Header, body io.Reader) (Response, error) {
	baseResp, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}
	return &PrepareResponse{BaseResponse: baseResp}, nil
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
	req.rtype = iproto.IPROTO_PREPARE
	req.stmt = stmt
	return req
}

// Conn returns the Connection object the request belongs to
func (req *UnprepareRequest) Conn() *Connection {
	return req.stmt.Conn
}

// Body fills an msgpack.Encoder with the execute request body.
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
	req.rtype = iproto.IPROTO_EXECUTE
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

// Body fills an msgpack.Encoder with the execute request body.
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

// Response creates a response for the ExecutePreparedRequest.
func (req *ExecutePreparedRequest) Response(header Header, body io.Reader) (Response, error) {
	baseResp, err := createBaseResponse(header, body)
	if err != nil {
		return nil, err
	}
	return &ExecuteResponse{BaseResponse: baseResp}, nil
}
