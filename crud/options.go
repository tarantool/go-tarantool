package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

const (
	timeoutOptName                       = "timeout"
	vshardRouterOptName                  = "vshard_router"
	fieldsOptName                        = "fields"
	bucketIdOptName                      = "bucket_id"
	skipNullabilityCheckOnFlattenOptName = "skip_nullability_check_on_flatten"
	stopOnErrorOptName                   = "stop_on_error"
	rollbackOnErrorOptName               = "rollback_on_error"
	modeOptName                          = "mode"
	preferReplicaOptName                 = "prefer_replica"
	balanceOptName                       = "balance"
	yieldEveryOptName                    = "yield_every"
	forceMapCallOptName                  = "force_map_call"
	fullscanOptName                      = "fullscan"
	firstOptName                         = "first"
	afterOptName                         = "after"
	batchSizeOptName                     = "batch_size"
)

// OptUint is an optional uint.
type OptUint struct {
	value uint
	exist bool
}

// MakeOptUint creates an optional uint from value.
func MakeOptUint(value uint) OptUint {
	return OptUint{
		value: value,
		exist: true,
	}
}

// Get returns the integer value or an error if not present.
func (opt OptUint) Get() (uint, bool) {
	return opt.value, opt.exist
}

// OptInt is an optional int.
type OptInt struct {
	value int
	exist bool
}

// MakeOptInt creates an optional int from value.
func MakeOptInt(value int) OptInt {
	return OptInt{
		value: value,
		exist: true,
	}
}

// Get returns the integer value or an error if not present.
func (opt OptInt) Get() (int, bool) {
	return opt.value, opt.exist
}

// OptString is an optional string.
type OptString struct {
	value string
	exist bool
}

// MakeOptString creates an optional string from value.
func MakeOptString(value string) OptString {
	return OptString{
		value: value,
		exist: true,
	}
}

// Get returns the string value or an error if not present.
func (opt OptString) Get() (string, bool) {
	return opt.value, opt.exist
}

// OptBool is an optional bool.
type OptBool struct {
	value bool
	exist bool
}

// MakeOptBool creates an optional bool from value.
func MakeOptBool(value bool) OptBool {
	return OptBool{
		value: value,
		exist: true,
	}
}

// Get returns the boolean value or an error if not present.
func (opt OptBool) Get() (bool, bool) {
	return opt.value, opt.exist
}

// OptTuple is an optional tuple.
type OptTuple struct {
	tuple interface{}
}

// MakeOptTuple creates an optional tuple from tuple.
func MakeOptTuple(tuple interface{}) OptTuple {
	return OptTuple{tuple}
}

// Get returns the tuple value or an error if not present.
func (o *OptTuple) Get() (interface{}, bool) {
	return o.tuple, o.tuple != nil
}

// BaseOpts describes base options for CRUD operations.
type BaseOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts BaseOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 2

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// SimpleOperationOpts describes options for simple CRUD operations.
// It also covers `upsert_object` options.
type SimpleOperationOpts struct {
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
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SimpleOperationOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 4

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.BucketId.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// SimpleOperationObjectOpts describes options for simple CRUD
// operations with objects. It doesn't cover `upsert_object` options.
type SimpleOperationObjectOpts struct {
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
	// SkipNullabilityCheckOnFlatten is a parameter to allow
	// setting null values to non-nullable fields.
	SkipNullabilityCheckOnFlatten OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SimpleOperationObjectOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 5

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName, skipNullabilityCheckOnFlattenOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.BucketId.Get()
	values[4], exists[4] = opts.SkipNullabilityCheckOnFlatten.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// OperationManyOpts describes options for CRUD operations with many tuples.
// It also covers `upsert_object_many` options.
type OperationManyOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Fields is field names for getting only a subset of fields.
	Fields OptTuple
	// StopOnError is a parameter to stop on a first error and report
	// error regarding the failed operation and error about what tuples
	// were not performed.
	StopOnError OptBool
	// RollbackOnError is a parameter because of what any failed operation
	// will lead to rollback on a storage, where the operation is failed.
	RollbackOnError OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts OperationManyOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 5

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.StopOnError.Get()
	values[4], exists[4] = opts.RollbackOnError.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// OperationObjectManyOpts describes options for CRUD operations
// with many objects. It doesn't cover `upsert_object_many` options.
type OperationObjectManyOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Fields is field names for getting only a subset of fields.
	Fields OptTuple
	// StopOnError is a parameter to stop on a first error and report
	// error regarding the failed operation and error about what tuples
	// were not performed.
	StopOnError OptBool
	// RollbackOnError is a parameter because of what any failed operation
	// will lead to rollback on a storage, where the operation is failed.
	RollbackOnError OptBool
	// SkipNullabilityCheckOnFlatten is a parameter to allow
	// setting null values to non-nullable fields.
	SkipNullabilityCheckOnFlatten OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts OperationObjectManyOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 6

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName,
		skipNullabilityCheckOnFlattenOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.StopOnError.Get()
	values[4], exists[4] = opts.RollbackOnError.Get()
	values[5], exists[5] = opts.SkipNullabilityCheckOnFlatten.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// BorderOpts describes options for `crud.min` and `crud.max`.
type BorderOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptUint
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Fields is field names for getting only a subset of fields.
	Fields OptTuple
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts BorderOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 3

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName, fieldsOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

func encodeOptions(enc *msgpack.Encoder,
	names []string, values []interface{}, exists []bool) error {
	mapLen := 0

	for _, exist := range exists {
		if exist {
			mapLen += 1
		}
	}

	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}

	if mapLen > 0 {
		for i, name := range names {
			if exists[i] {
				enc.EncodeString(name)
				enc.Encode(values[i])
			}
		}
	}

	return nil
}
