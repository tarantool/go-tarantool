package crud

import (
	"context"
	"fmt"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"github.com/tarantool/go-tarantool/v3"
)

const (
	// AtomicBatchGet - `get` operation type.
	AtomicBatchGet AtomicBatchOperationType = "get"
	// AtomicBatchInsert - `insert` operation type.
	AtomicBatchInsert AtomicBatchOperationType = "insert"
	// AtomicBatchReplace - `replace` operation type.
	AtomicBatchReplace AtomicBatchOperationType = "replace"
	// AtomicBatchUpdate - `update` operation type.
	AtomicBatchUpdate AtomicBatchOperationType = "update"
	// AtomicBatchUpsert - `upsert` operation type.
	AtomicBatchUpsert AtomicBatchOperationType = "upsert"
	// AtomicBatchDelete - `delete` operation type.
	AtomicBatchDelete AtomicBatchOperationType = "delete"
)

// AtomicBatchOperationType describes the type of a single operation
// in an atomic batch.
type AtomicBatchOperationType string

// AtomicBatchOperation describes a single CRUD operation in an atomic batch
// as a table {type, space, key/tuple/object, operations}.
type AtomicBatchOperation struct {
	// Type is an operation type.
	Type AtomicBatchOperationType
	// Space is a name of the space to perform the operation on.
	Space string
	// Key identifies a record for `get`, `update` and `delete` operations.
	Key option.Any
	// Tuple is used by `insert`, `replace` and `upsert` operations.
	Tuple option.Any
	// Object is used by `insert`, `replace` and `upsert` operations.
	Object option.Any
	// Operations is used by `update` and `upsert` operations.
	Operations []Operation
}

// EncodeMsgpack provides custom msgpack encoder.
func (op AtomicBatchOperation) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 6

	names := [optsCnt]string{typeOptName, spaceOptName, keyOptName, tupleOptName,
		objectOptName, operationsOptName}
	values := [optsCnt]any{string(op.Type), op.Space}
	exists := [optsCnt]bool{true, true}
	values[2], exists[2] = op.Key.Get()
	values[3], exists[3] = op.Tuple.Get()
	values[4], exists[4] = op.Object.Get()
	if len(op.Operations) > 0 {
		values[5] = op.Operations
		exists[5] = true
	}

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// AtomicBatchOpts describes options for `crud.atomic_batch` method.
type AtomicBatchOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// Noreturn suppresses successfully processed data (first return value is `nil`).
	// Disabled by default.
	Noreturn option.Bool
	// Fields is field names for getting only a subset of fields, given as
	// a map of space names to arrays of field names.
	Fields option.Any
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts AtomicBatchOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 3

	names := [optsCnt]string{timeoutOptName, noreturnOptName, fieldsOptName}
	values := [optsCnt]any{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.Noreturn.Get()
	values[2], exists[2] = opts.Fields.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// AtomicBatchRequest helps you to create request object to call
// `crud.atomic_batch` for execution by a Connection.
type AtomicBatchRequest struct {
	baseRequest

	operations []AtomicBatchOperation
	opts       AtomicBatchOpts
}

// NewAtomicBatchRequest returns a new empty AtomicBatchRequest.
func NewAtomicBatchRequest() AtomicBatchRequest {
	return AtomicBatchRequest{
		baseRequest: newBaseRequest("crud.atomic_batch"),
		operations:  []AtomicBatchOperation{},
		opts:        AtomicBatchOpts{},
	}
}

// Operations sets the operations for the AtomicBatchRequest request.
// Note: default value is an empty array.
func (req AtomicBatchRequest) Operations(operations []AtomicBatchOperation) AtomicBatchRequest {
	req.operations = operations
	return req
}

// Opts sets the options for the AtomicBatchRequest request.
// Note: default value is nil.
func (req AtomicBatchRequest) Opts(opts AtomicBatchOpts) AtomicBatchRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req AtomicBatchRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	req.impl = req.impl.Args([]any{req.operations, req.opts})
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req AtomicBatchRequest) Context(ctx context.Context) AtomicBatchRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// AtomicBatchOpDescriptor describes a single operation in an atomic batch
// result as a table {type, space}.
type AtomicBatchOpDescriptor struct {
	// Type is an operation type.
	Type string `msgpack:"type"`
	// Space is a name of the space the operation was performed on.
	Space string `msgpack:"space"`
}

// AtomicBatchResult describes CRUD result as an object containing metadata,
// operation descriptors and data rows.
type AtomicBatchResult struct {
	// Metadata is a map of metadata by space names used in the batch.
	Metadata map[string][]FieldFormat
	// Ops is a list of operation descriptors aligned with Data.
	Ops []AtomicBatchOpDescriptor
	// Data is an ordered list of operation results.
	Data []any
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *AtomicBatchResult) DecodeMsgpack(d *msgpack.Decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen == 0 {
		return fmt.Errorf("unexpected empty response array")
	}

	// DecodeMapLen processes `nil` as zero length map,
	// so in `return nil, err` case we don't miss error info.
	// https://github.com/vmihailenco/msgpack/blob/3f7bd806fea698e7a9fe80979aa3512dea0a7368/decode_map.go#L79-L81
	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}

	for range l {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}

		switch key {
		case "metadata":
			if err = d.Decode(&r.Metadata); err != nil {
				return err
			}
		case "ops":
			if err = d.Decode(&r.Ops); err != nil {
				return err
			}
		case "data":
			if err = d.Decode(&r.Data); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	if arrLen > 1 {
		code, err := d.PeekCode()
		if err != nil {
			return err
		}

		switch {
		case msgpackIsArray(code):
			crudErr := newErrorMany(nil)
			if err := d.Decode(&crudErr); err != nil {
				return err
			}
			if crudErr != nil {
				return crudErr
			}
		case code != msgpcode.Nil:
			crudErr := newError(nil)
			if err := d.Decode(&crudErr); err != nil {
				return err
			}
			if crudErr != nil {
				return crudErr
			}
		default:
			if err := d.DecodeNil(); err != nil {
				return err
			}
		}
	}

	for i := 2; i < arrLen; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}

	return nil
}
