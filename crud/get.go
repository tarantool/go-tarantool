package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// GetOpts describes options for `crud.get` method.
type GetOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Fields is field names for getting only a subset of fields.
	Fields OptTuple
	// BucketId is a bucket ID.
	BucketId OptUint
	// Mode is a parameter with `write`/`read` possible values,
	// if `write` is specified then operation is performed on master.
	Mode OptString
	// PreferReplica is a parameter to specify preferred target
	// as one of the replicas.
	PreferReplica OptBool
	// Balance is a parameter to use replica according to vshard
	// load balancing policy.
	Balance OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts GetOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 7

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName, modeOptName,
		preferReplicaOptName, balanceOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[1], exists[1] = opts.BucketId.Get()
	values[2], exists[2] = opts.Mode.Get()
	values[3], exists[3] = opts.PreferReplica.Get()
	values[4], exists[4] = opts.Balance.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// GetRequest helps you to create request object to call `crud.get`
// for execution by a Connection.
type GetRequest struct {
	spaceRequest
	key  Tuple
	opts GetOpts
}

type getArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space    string
	Key      Tuple
	Opts     GetOpts
}

// MakeGetRequest returns a new empty GetRequest.
func MakeGetRequest(space string) GetRequest {
	req := GetRequest{}
	req.impl = newCall("crud.get")
	req.space = space
	req.opts = GetOpts{}
	return req
}

// Key sets the key for the GetRequest request.
// Note: default value is nil.
func (req GetRequest) Key(key Tuple) GetRequest {
	req.key = key
	return req
}

// Opts sets the options for the GetRequest request.
// Note: default value is nil.
func (req GetRequest) Opts(opts GetOpts) GetRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req GetRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	if req.key == nil {
		req.key = []interface{}{}
	}
	args := getArgs{Space: req.space, Key: req.key, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req GetRequest) Context(ctx context.Context) GetRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
