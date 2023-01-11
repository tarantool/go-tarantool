package crud

import (
	"context"

	"github.com/tarantool/go-tarantool"
)

// SelectResult describes result for `crud.select` method.
type SelectResult = Result

// SelectOpts describes options for `crud.select` method.
type SelectOpts struct {
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
	// First describes the maximum count of the objects to return.
	First OptInt
	// After is a tuple after which objects should be selected.
	After OptTuple
	// BatchSize is a number of tuples to process per one request to storage.
	BatchSize OptUint
	// ForceMapCall describes the map call is performed without any
	// optimizations even if full primary key equal condition is specified.
	ForceMapCall OptBool
	// Fullscan describes if a critical log entry will be skipped on
	// potentially long select.
	Fullscan OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SelectOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 12

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Fields, opts.BucketId,
		opts.Mode, opts.PreferReplica, opts.Balance,
		opts.First, opts.After, opts.BatchSize,
		opts.ForceMapCall, opts.Fullscan}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName,
		modeOptName, preferReplicaOptName, balanceOptName,
		firstOptName, afterOptName, batchSizeOptName,
		forceMapCallOptName, fullscanOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// SelectRequest helps you to create request object to call `crud.select`
// for execution by a Connection.
type SelectRequest struct {
	spaceRequest
	conditions []Condition
	opts       SelectOpts
}

type selectArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Space      string
	Conditions []Condition
	Opts       SelectOpts
}

// NewSelectRequest returns a new empty SelectRequest.
func NewSelectRequest(space string) *SelectRequest {
	req := new(SelectRequest)
	req.initImpl("crud.select")
	req.setSpace(space)
	req.conditions = nil
	req.opts = SelectOpts{}
	return req
}

// Conditions sets the conditions for the SelectRequest request.
// Note: default value is nil.
func (req *SelectRequest) Conditions(conditions []Condition) *SelectRequest {
	req.conditions = conditions
	return req
}

// Opts sets the options for the SelectRequest request.
// Note: default value is nil.
func (req *SelectRequest) Opts(opts SelectOpts) *SelectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req *SelectRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := selectArgs{Space: req.space, Conditions: req.conditions, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *SelectRequest) Context(ctx context.Context) *SelectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
