package crud

import (
	"context"
	"fmt"
	"reflect"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// StorageCall describes a single call for the `crud.storage_call_many` method.
type StorageCall struct {
	// FuncName is the exact name of a persistent function in `box.func`.
	FuncName string
	// Args is an array of the target function arguments.
	Args option.Any
	// BucketId is an explicit bucket ID from 1 to the bucket count
	// configured for the selected router.
	BucketId option.Uint
	// SpaceName is a space used to calculate a bucket ID from its primary key.
	SpaceName option.String
	// Key is a primary key value for SpaceName.
	Key option.Any
}

// EncodeMsgpack provides custom msgpack encoder.
func (call StorageCall) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 5

	names := [optsCnt]string{funcNameOptName, argsOptName, bucketIdOptName,
		spaceNameOptName, keyOptName}
	values := [optsCnt]any{call.FuncName}
	exists := [optsCnt]bool{true}

	values[1], exists[1] = call.Args.Get()
	values[2], exists[2] = call.BucketId.Get()
	values[3], exists[3] = call.SpaceName.Get()
	values[4], exists[4] = call.Key.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// StorageCallManyOpts describes options for `crud.storage_call_many` method.
type StorageCallManyOpts = BaseOpts

// StorageCallManyRequest helps you to create request object to call
// `crud.storage_call_many` for execution by a Connection.
type StorageCallManyRequest struct {
	baseRequest

	calls []StorageCall
	opts  StorageCallManyOpts
}

type storageCallManyArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint:unused
	Calls    []StorageCall
	Opts     StorageCallManyOpts
}

// NewStorageCallManyRequest returns a new empty StorageCallManyRequest.
func NewStorageCallManyRequest(calls []StorageCall) StorageCallManyRequest {
	return StorageCallManyRequest{
		baseRequest: newBaseRequest("crud.storage_call_many"),
		calls:       calls,
		opts:        StorageCallManyOpts{},
	}
}

// Opts sets the options for the StorageCallManyRequest request.
// Note: default value is nil.
func (req StorageCallManyRequest) Opts(opts StorageCallManyOpts) StorageCallManyRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req StorageCallManyRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if req.calls == nil {
		req.calls = []StorageCall{}
	}
	args := storageCallManyArgs{Calls: req.calls, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req StorageCallManyRequest) Context(ctx context.Context) StorageCallManyRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// StorageCallManyResult describes result for `crud.storage_call_many` method.
type StorageCallManyResult struct {
	// Results is an ordered array with a result for each call. Every item
	// corresponds to the call with the same index and contains either
	// Returns or Error.
	Results []StorageCallManyResultItem
	// returnType contains the type of Returns for all items.
	returnType reflect.Type
}

// NewStorageCallManyResult creates a StorageCallManyResult object with a custom
// return type for decoding.
func NewStorageCallManyResult(returnType reflect.Type) StorageCallManyResult {
	return StorageCallManyResult{
		returnType: returnType,
	}
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *StorageCallManyResult) DecodeMsgpack(d *msgpack.Decoder) error {
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
		case "results":
			resultsLen, err := d.DecodeArrayLen()
			if err != nil {
				return err
			}

			results := make([]StorageCallManyResultItem, resultsLen)
			for i := range resultsLen {
				item := StorageCallManyResultItem{returnType: r.returnType}
				if err = d.Decode(&item); err != nil {
					return err
				}
				results[i] = item
			}

			r.Results = results
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

// StorageCallManyResultItem describes a result of a single call from the
// `crud.storage_call_many` method.
type StorageCallManyResultItem struct {
	// Returns is an array of values returned by the target function.
	// It is nil if the call ended with an error.
	Returns any
	// Error is an error returned by the target function.
	// It is nil if the call succeeded.
	Error *StorageCallError
	// returnType contains the type of Returns.
	returnType reflect.Type
}

// DecodeMsgpack provides custom msgpack decoder.
func (item *StorageCallManyResultItem) DecodeMsgpack(d *msgpack.Decoder) error {
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
			if item.Returns, err = decodeSlice(d, item.returnType); err != nil {
				return err
			}
		case "error":
			if err = d.Decode(&item.Error); err != nil {
				return err
			}
		default:
			if err = d.Skip(); err != nil {
				return err
			}
		}
	}

	return nil
}
