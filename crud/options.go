package crud

import (
	"github.com/tarantool/go-option"
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
	fetchLatestMetadataOptName           = "fetch_latest_metadata"
	noreturnOptName                      = "noreturn"
	cachedOptName                        = "cached"
)

// BaseOpts describes base options for CRUD operations.
type BaseOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
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
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
	// Fields is field names for getting only a subset of fields.
	Fields option.Any
	// BucketId is a bucket ID.
	BucketId option.Uint
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata option.Bool
	// Noreturn suppresses successfully processed data (first return value is `nil`).
	// Disabled by default.
	Noreturn option.Bool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SimpleOperationOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 6

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName, fetchLatestMetadataOptName,
		noreturnOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.BucketId.Get()
	values[4], exists[4] = opts.FetchLatestMetadata.Get()
	values[5], exists[5] = opts.Noreturn.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// SimpleOperationObjectOpts describes options for simple CRUD
// operations with objects. It doesn't cover `upsert_object` options.
type SimpleOperationObjectOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
	// Fields is field names for getting only a subset of fields.
	Fields option.Any
	// BucketId is a bucket ID.
	BucketId option.Uint
	// SkipNullabilityCheckOnFlatten is a parameter to allow
	// setting null values to non-nullable fields.
	SkipNullabilityCheckOnFlatten option.Bool
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata option.Bool
	// Noreturn suppresses successfully processed data (first return value is `nil`).
	// Disabled by default.
	Noreturn option.Bool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SimpleOperationObjectOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 7

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, bucketIdOptName, skipNullabilityCheckOnFlattenOptName,
		fetchLatestMetadataOptName, noreturnOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.BucketId.Get()
	values[4], exists[4] = opts.SkipNullabilityCheckOnFlatten.Get()
	values[5], exists[5] = opts.FetchLatestMetadata.Get()
	values[6], exists[6] = opts.Noreturn.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// OperationManyOpts describes options for CRUD operations with many tuples.
// It also covers `upsert_object_many` options.
type OperationManyOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
	// Fields is field names for getting only a subset of fields.
	Fields option.Any
	// StopOnError is a parameter to stop on a first error and report
	// error regarding the failed operation and error about what tuples
	// were not performed.
	StopOnError option.Bool
	// RollbackOnError is a parameter because of what any failed operation
	// will lead to rollback on a storage, where the operation is failed.
	RollbackOnError option.Bool
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata option.Bool
	// Noreturn suppresses successfully processed data (first return value is `nil`).
	// Disabled by default.
	Noreturn option.Bool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts OperationManyOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 7

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName,
		fetchLatestMetadataOptName, noreturnOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.StopOnError.Get()
	values[4], exists[4] = opts.RollbackOnError.Get()
	values[5], exists[5] = opts.FetchLatestMetadata.Get()
	values[6], exists[6] = opts.Noreturn.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// OperationObjectManyOpts describes options for CRUD operations
// with many objects. It doesn't cover `upsert_object_many` options.
type OperationObjectManyOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
	// Fields is field names for getting only a subset of fields.
	Fields option.Any
	// StopOnError is a parameter to stop on a first error and report
	// error regarding the failed operation and error about what tuples
	// were not performed.
	StopOnError option.Bool
	// RollbackOnError is a parameter because of what any failed operation
	// will lead to rollback on a storage, where the operation is failed.
	RollbackOnError option.Bool
	// SkipNullabilityCheckOnFlatten is a parameter to allow
	// setting null values to non-nullable fields.
	SkipNullabilityCheckOnFlatten option.Bool
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata option.Bool
	// Noreturn suppresses successfully processed data (first return value is `nil`).
	// Disabled by default.
	Noreturn option.Bool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts OperationObjectManyOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 8

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		fieldsOptName, stopOnErrorOptName, rollbackOnErrorOptName,
		skipNullabilityCheckOnFlattenOptName, fetchLatestMetadataOptName,
		noreturnOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.StopOnError.Get()
	values[4], exists[4] = opts.RollbackOnError.Get()
	values[5], exists[5] = opts.SkipNullabilityCheckOnFlatten.Get()
	values[6], exists[6] = opts.FetchLatestMetadata.Get()
	values[7], exists[7] = opts.Noreturn.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// BorderOpts describes options for `crud.min` and `crud.max`.
type BorderOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout option.Float64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter option.String
	// Fields is field names for getting only a subset of fields.
	Fields option.Any
	// FetchLatestMetadata guarantees the up-to-date metadata (space format)
	// in first return value, otherwise it may not take into account
	// the latest migration of the data format. Performance overhead is up to 15%.
	// Disabled by default.
	FetchLatestMetadata option.Bool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts BorderOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 4

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName, fieldsOptName,
		fetchLatestMetadataOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Fields.Get()
	values[3], exists[3] = opts.FetchLatestMetadata.Get()

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
				if err := enc.EncodeString(name); err != nil {
					return err
				}
				if err := enc.Encode(values[i]); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
