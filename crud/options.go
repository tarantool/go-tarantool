package crud

import (
	"errors"

	"github.com/markphelps/optional"
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

type option interface {
	getInterface() (interface{}, error)
}

// OptUint is an optional uint.
type OptUint struct {
	optional.Uint
}

// NewOptUint creates an optional uint from value.
func NewOptUint(value uint) OptUint {
	return OptUint{optional.NewUint(value)}
}

func (opt OptUint) getInterface() (interface{}, error) {
	return opt.Get()
}

// OptInt is an optional int.
type OptInt struct {
	optional.Int
}

// NewOptInt creates an optional int from value.
func NewOptInt(value int) OptInt {
	return OptInt{optional.NewInt(value)}
}

func (opt OptInt) getInterface() (interface{}, error) {
	return opt.Get()
}

// OptString is an optional string.
type OptString struct {
	optional.String
}

// NewOptString creates an optional string from value.
func NewOptString(value string) OptString {
	return OptString{optional.NewString(value)}
}

func (opt OptString) getInterface() (interface{}, error) {
	return opt.Get()
}

// OptBool is an optional bool.
type OptBool struct {
	optional.Bool
}

// NewOptBool creates an optional bool from value.
func NewOptBool(value bool) OptBool {
	return OptBool{optional.NewBool(value)}
}

func (opt OptBool) getInterface() (interface{}, error) {
	return opt.Get()
}

// OptTuple is an optional tuple.
type OptTuple struct {
	tuple []interface{}
}

// NewOptTuple creates an optional tuple from tuple.
func NewOptTuple(tuple []interface{}) OptTuple {
	return OptTuple{tuple}
}

// Get returns the tuple value or an error if not present.
func (o *OptTuple) Get() ([]interface{}, error) {
	if o.tuple == nil {
		return nil, errors.New("value not present")
	}
	return o.tuple, nil
}

func (opt OptTuple) getInterface() (interface{}, error) {
	return opt.Get()
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
func (opts BaseOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 2

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// SimpleOperationOpts describes options for simple CRUD operations.
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
func (opts SimpleOperationOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 4

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Fields, opts.BucketId}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// SimpleOperationObjectOpts describes options for simple CRUD
// operations with objects.
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
func (opts SimpleOperationObjectOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 5

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Fields, opts.BucketId, opts.SkipNullabilityCheckOnFlatten}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName, skipNullabilityCheckOnFlattenOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// OperationManyOpts describes options for CRUD operations with many tuples.
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
func (opts OperationManyOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 5

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Fields, opts.StopOnError, opts.RollbackOnError}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

// OperationObjectManyOpts describes options for CRUD operations
// with many objects.
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
func (opts OperationObjectManyOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 6

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter,
		opts.Fields, opts.StopOnError, opts.RollbackOnError,
		opts.SkipNullabilityCheckOnFlatten}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName,
		skipNullabilityCheckOnFlattenOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
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
func (opts BorderOpts) EncodeMsgpack(enc *encoder) error {
	const optsCnt = 3

	options := [optsCnt]option{opts.Timeout, opts.VshardRouter, opts.Fields}
	names := [optsCnt]string{timeoutOptName, vshardRouterOptName, fieldsOptName}
	values := [optsCnt]interface{}{}

	return encodeOptions(enc, options[:], names[:], values[:])
}

func encodeOptions(enc *encoder, options []option, names []string, values []interface{}) error {
	mapLen := 0

	for i, opt := range options {
		if value, err := opt.getInterface(); err == nil {
			values[i] = value
			mapLen += 1
		}
	}

	if err := enc.EncodeMapLen(mapLen); err != nil {
		return err
	}

	if mapLen > 0 {
		for i, name := range names {
			if values[i] != nil {
				enc.EncodeString(name)
				enc.Encode(values[i])
			}
		}
	}

	return nil
}
