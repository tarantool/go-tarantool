package crud

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// StorageCallOpts describes options for `crud.storage_call` method.
type StorageCallOpts struct {
	// BucketId is an explicit bucket ID from 1 to the bucket count
	// configured for the selected router.
	BucketId option.Uint
	// SpaceName is a space used to calculate a bucket ID from its primary key.
	SpaceName option.String
	// Key is a primary key value for SpaceName.
	Key option.Any
	// Timeout is a common time budget used by CRUD for routing,
	// sending the call and waiting for its response (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts StorageCallOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 5

	names := [optsCnt]string{bucketIdOptName, spaceNameOptName, keyOptName,
		timeoutOptName, vshardRouterOptName}
	values := [optsCnt]any{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.BucketId.Get()
	values[1], exists[1] = opts.SpaceName.Get()
	values[2], exists[2] = opts.Key.Get()
	values[3], exists[3] = opts.Timeout.Get()
	values[4], exists[4] = opts.VshardRouter.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// StorageCallRequest helps you to create request object to call
// `crud.storage_call` for execution by a Connection.
type StorageCallRequest struct {
	baseRequest

	funcName string
	args     []any
	opts     StorageCallOpts
}

type storageCallArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint:unused
	FuncName string
	Args     []any
	Opts     StorageCallOpts
}

// NewStorageCallRequest returns a new empty StorageCallRequest.
func NewStorageCallRequest(funcName string) StorageCallRequest {
	return StorageCallRequest{
		baseRequest: newBaseRequest("crud.storage_call"),
		funcName:    funcName,
		opts:        StorageCallOpts{},
	}
}

// Args sets the function arguments for the StorageCallRequest request.
// Note: default value is an empty array.
func (req StorageCallRequest) Args(args []any) StorageCallRequest {
	req.args = args
	return req
}

// Opts sets the options for the StorageCallRequest request.
// Note: default value is nil.
func (req StorageCallRequest) Opts(opts StorageCallOpts) StorageCallRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req StorageCallRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if req.args == nil {
		req.args = []any{}
	}
	args := storageCallArgs{FuncName: req.funcName, Args: req.args, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req StorageCallRequest) Context(ctx context.Context) StorageCallRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// StorageCallResult describes result for `crud.storage_call` method.
type StorageCallResult struct {
	// Returns is an array of values returned by the target function.
	// Values are preserved as is, including `nil` (box.NULL) entries.
	Returns any
	// returnType contains the type of Returns.
	returnType reflect.Type
}

// NewStorageCallResult creates a StorageCallResult object with a custom
// return type for decoding.
func NewStorageCallResult(returnType reflect.Type) StorageCallResult {
	return StorageCallResult{
		returnType: returnType,
	}
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *StorageCallResult) DecodeMsgpack(d *msgpack.Decoder) error {
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
		case "returns":
			if r.Returns, err = decodeSlice(d, r.returnType); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	if arrLen > 1 {
		var crudErr *StorageCallError

		if err := d.Decode(&crudErr); err != nil {
			return err
		}

		if crudErr != nil {
			return crudErr
		}
	}

	for i := 2; i < arrLen; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}

	return nil
}
