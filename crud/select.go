package crud

import (
	"context"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// SelectOpts describes options for `crud.select` method.
type SelectOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptFloat64
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
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata OptBool
	// YieldEvery describes number of tuples processed to yield after.
	// Should be positive.
	YieldEvery OptUint
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SelectOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 14

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName,
		modeOptName, preferReplicaOptName, balanceOptName,
		firstOptName, afterOptName, batchSizeOptName,
		forceMapCallOptName, fullscanOptName, fetchLatestMetadataOptName,
		yieldEveryOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.BucketId.Get()
	values[4], exists[4] = opts.Mode.Get()
	values[5], exists[5] = opts.PreferReplica.Get()
	values[6], exists[6] = opts.Balance.Get()
	values[7], exists[7] = opts.First.Get()
	values[8], exists[8] = opts.After.Get()
	values[9], exists[9] = opts.BatchSize.Get()
	values[10], exists[10] = opts.ForceMapCall.Get()
	values[11], exists[11] = opts.Fullscan.Get()
	values[12], exists[12] = opts.FetchLatestMetadata.Get()
	values[13], exists[13] = opts.YieldEvery.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// SelectRequest helps you to create request object to call `crud.select`
// for execution by a Connection.
type SelectRequest struct {
	spaceRequest
	conditions []Condition
	opts       SelectOpts
}

type selectArgs struct {
	_msgpack   struct{} `msgpack:",asArray"` // nolint: structcheck,unused
	Space      string
	Conditions []Condition
	Opts       SelectOpts
}

// MakeSelectRequest returns a new empty SelectRequest.
func MakeSelectRequest(space string) SelectRequest {
	req := SelectRequest{}
	req.impl = newCall("crud.select")
	req.space = space
	req.conditions = nil
	req.opts = SelectOpts{}
	return req
}

// Conditions sets the conditions for the SelectRequest request.
// Note: default value is nil.
func (req SelectRequest) Conditions(conditions []Condition) SelectRequest {
	req.conditions = conditions
	return req
}

// Opts sets the options for the SelectRequest request.
// Note: default value is nil.
func (req SelectRequest) Opts(opts SelectOpts) SelectRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req SelectRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	args := selectArgs{Space: req.space, Conditions: req.conditions, Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req SelectRequest) Context(ctx context.Context) SelectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
