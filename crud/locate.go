package crud

import (
	"context"
	"fmt"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"github.com/tarantool/go-tarantool/v3"
)

// LocateOpts describes options for `crud.locate` method.
type LocateOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// RequestTimeout is a `vshard.call` request_timeout parameter.
	// Can be specified only with Mode set to `read`.
	RequestTimeout option.Float64
	// BucketId is a bucket ID.
	BucketId option.Uint
	// Mode is a parameter with `write`/`read` possible values,
	// if `write` is specified then operation is performed on master.
	Mode option.String
	// PreferReplica is a parameter to specify preferred target
	// as one of the replicas.
	PreferReplica option.Bool
	// Balance is a parameter to use replica according to vshard
	// load balancing policy.
	Balance option.Bool
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts LocateOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 7

	names := [optsCnt]string{timeoutOptName, requestTimeoutOptName, bucketIdOptName,
		modeOptName, preferReplicaOptName, balanceOptName, vshardRouterOptName}
	values := [optsCnt]any{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.RequestTimeout.Get()
	values[2], exists[2] = opts.BucketId.Get()
	values[3], exists[3] = opts.Mode.Get()
	values[4], exists[4] = opts.PreferReplica.Get()
	values[5], exists[5] = opts.Balance.Get()
	values[6], exists[6] = opts.VshardRouter.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// LocateRequest helps you to create request object to call `crud.locate`
// for execution by a Connection.
type LocateRequest struct {
	baseRequest

	space string
	key   Tuple
	opts  LocateOpts
}

type locateArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint:unused
	Space    string
	Key      Tuple
	Opts     LocateOpts
}

// NewLocateRequest returns a new empty LocateRequest.
func NewLocateRequest(space string) LocateRequest {
	return LocateRequest{
		baseRequest: newBaseRequest("crud.locate"),
		space:       space,
		opts:        LocateOpts{},
	}
}

// Key sets the key for the LocateRequest request.
// Note: default value is nil.
func (req LocateRequest) Key(key Tuple) LocateRequest {
	req.key = key
	return req
}

// Opts sets the options for the LocateRequest request.
// Note: default value is nil.
func (req LocateRequest) Opts(opts LocateOpts) LocateRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req LocateRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	args := locateArgs{Space: req.space, Key: req.key, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req LocateRequest) Context(ctx context.Context) LocateRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// LocateResult describes result for `crud.locate` method.
type LocateResult struct {
	// Value is a located storage engine name (`memtx` or `vinyl`).
	Value string
	// Found is false if the tuple is missing on the target storage.
	Found bool
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *LocateResult) DecodeMsgpack(d *msgpack.Decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen == 0 {
		return fmt.Errorf("unexpected empty response array")
	}

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if code == msgpcode.Nil {
		if err := d.DecodeNil(); err != nil {
			return err
		}
		r.Found = false
	} else {
		if r.Value, err = d.DecodeString(); err != nil {
			return err
		}
		r.Found = true
	}

	if arrLen > 1 {
		var crudErr *Error

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
