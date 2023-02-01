package crud

import (
	"context"

	"github.com/markphelps/optional"
	"github.com/tarantool/go-tarantool"
)

type SelectOpts struct {
	Timeout       optional.Uint
	First         optional.Int
	After         interface{}
	BatchSize     optional.Uint
	BucketId      optional.Uint
	Fields        []string
	Fullscan      optional.Bool
	Mode          optional.String
	PreferReplica optional.Bool
	Balance       optional.Bool
	VshardRouter  optional.String
}

func (opts *SelectOpts) convertToMap() map[string]interface{} {
	optsMap := make(map[string]interface{})

	if value, err := opts.Timeout.Get(); err == nil {
		optsMap["timeout"] = value
	}

	if value, err := opts.First.Get(); err == nil {
		optsMap["first"] = value
	}

	optsMap["after"] = opts.After
	optsMap["fields"] = opts.Fields

	if value, err := opts.BatchSize.Get(); err == nil {
		optsMap["batch_size"] = value
	}

	if value, err := opts.BucketId.Get(); err == nil {
		optsMap["bucket_id"] = value
	}

	if value, err := opts.Fullscan.Get(); err == nil {
		optsMap["fullscan"] = value
	}

	if value, err := opts.Mode.Get(); err == nil {
		optsMap["mode"] = value
	}

	if value, err := opts.PreferReplica.Get(); err == nil {
		optsMap["prefer_replica"] = value
	}

	if value, err := opts.Balance.Get(); err == nil {
		optsMap["balance"] = value
	}

	if value, err := opts.VshardRouter.Get(); err == nil {
		optsMap["vshard_router"] = value
	}

	return optsMap
}

type Condition struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Operator string
	KeyName  string
	KeyValue interface{}
}

// SelectRequest helps you to create request
// object to call `crud.select` for execution
// by a Connection.
type SelectRequest struct {
	spaceRequest
	conditions []Condition
	opts       SelectOpts
}

// NewSelectRequest returns a new empty SelectRequest.
func NewSelectRequest(space interface{}) *SelectRequest {
	req := new(SelectRequest)
	req.initImpl("crud.select")
	req.setSpace(space)
	req.conditions = []Condition{}
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
	req.impl.Args([]interface{}{req.space, req.conditions, req.opts.convertToMap()})
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req *SelectRequest) Context(ctx context.Context) *SelectRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
