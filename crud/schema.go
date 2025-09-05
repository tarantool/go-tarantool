package crud

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"github.com/tarantool/go-tarantool/v3"
)

func msgpackIsMap(code byte) bool {
	return code == msgpcode.Map16 || code == msgpcode.Map32 || msgpcode.IsFixedMap(code)
}

// SchemaOpts describes options for `crud.schema` method.
type SchemaOpts struct {
	// Timeout is a `vshard.call` timeout and vshard
	// master discovery timeout (in seconds).
	Timeout OptFloat64
	// VshardRouter is cartridge vshard group name or
	// vshard router instance.
	VshardRouter OptString
	// Cached defines whether router should reload storage schema on call.
	Cached OptBool
}

// EncodeMsgpack provides custom msgpack encoder.
func (opts SchemaOpts) EncodeMsgpack(enc *msgpack.Encoder) error {
	const optsCnt = 3

	names := [optsCnt]string{timeoutOptName, vshardRouterOptName,
		cachedOptName}
	values := [optsCnt]interface{}{}
	exists := [optsCnt]bool{}
	values[0], exists[0] = opts.Timeout.Get()
	values[1], exists[1] = opts.VshardRouter.Get()
	values[2], exists[2] = opts.Cached.Get()

	return encodeOptions(enc, names[:], values[:], exists[:])
}

// SchemaRequest helps you to create request object to call `crud.schema`
// for execution by a Connection.
type SchemaRequest struct {
	baseRequest
	space OptString
	opts  SchemaOpts
}

// MakeSchemaRequest returns a new empty SchemaRequest.
func MakeSchemaRequest() SchemaRequest {
	req := SchemaRequest{}
	req.impl = newCall("crud.schema")
	return req
}

// Space sets the space name for the SchemaRequest request.
// Note: default value is nil.
func (req SchemaRequest) Space(space string) SchemaRequest {
	req.space = MakeOptString(space)
	return req
}

// Opts sets the options for the SchemaRequest request.
// Note: default value is nil.
func (req SchemaRequest) Opts(opts SchemaOpts) SchemaRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req SchemaRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if value, ok := req.space.Get(); ok {
		req.impl = req.impl.Args([]interface{}{value, req.opts})
	} else {
		req.impl = req.impl.Args([]interface{}{nil, req.opts})
	}

	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req SchemaRequest) Context(ctx context.Context) SchemaRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// Schema contains CRUD cluster schema definition.
type Schema map[string]SpaceSchema

// DecodeMsgpack provides custom msgpack decoder.
func (schema *Schema) DecodeMsgpack(d *msgpack.Decoder) error {
	var l int

	code, err := d.PeekCode()
	if err != nil {
		return err
	}

	if msgpackIsArray(code) {
		// Process empty schema case.
		l, err = d.DecodeArrayLen()
		if err != nil {
			return err
		}
		if l != 0 {
			return fmt.Errorf("expected map or empty array, got non-empty array")
		}
		*schema = make(map[string]SpaceSchema, l)
	} else if msgpackIsMap(code) {
		l, err := d.DecodeMapLen()
		if err != nil {
			return err
		}
		*schema = make(map[string]SpaceSchema, l)

		for i := 0; i < l; i++ {
			key, err := d.DecodeString()
			if err != nil {
				return err
			}

			var spaceSchema SpaceSchema
			if err := d.Decode(&spaceSchema); err != nil {
				return err
			}

			(*schema)[key] = spaceSchema
		}
	} else {
		return fmt.Errorf("unexpected code=%d decoding map or empty array", code)
	}

	return nil
}

// SpaceSchema contains a single CRUD space schema definition.
type SpaceSchema struct {
	Format  []FieldFormat    `msgpack:"format"`
	Indexes map[uint32]Index `msgpack:"indexes"`
}

// Index contains a CRUD space index definition.
type Index struct {
	Id     uint32      `msgpack:"id"`
	Name   string      `msgpack:"name"`
	Type   string      `msgpack:"type"`
	Unique bool        `msgpack:"unique"`
	Parts  []IndexPart `msgpack:"parts"`
}

// IndexField contains a CRUD space index part definition.
type IndexPart struct {
	Fieldno     uint32 `msgpack:"fieldno"`
	Type        string `msgpack:"type"`
	ExcludeNull bool   `msgpack:"exclude_null"`
	IsNullable  bool   `msgpack:"is_nullable"`
}

// SchemaResult contains a schema request result for all spaces.
type SchemaResult struct {
	Value Schema
}

// DecodeMsgpack provides custom msgpack decoder.
func (result *SchemaResult) DecodeMsgpack(d *msgpack.Decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen == 0 {
		return fmt.Errorf("unexpected empty response array")
	}

	// DecodeMapLen inside Schema decode processes `nil` as zero length map,
	// so in `return nil, err` case we don't miss error info.
	// https://github.com/vmihailenco/msgpack/blob/3f7bd806fea698e7a9fe80979aa3512dea0a7368/decode_map.go#L79-L81
	if err = d.Decode(&result.Value); err != nil {
		return err
	}

	if arrLen > 1 {
		var crudErr *Error = nil

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

// SchemaResult contains a schema request result for a single space.
type SpaceSchemaResult struct {
	Value SpaceSchema
}

// DecodeMsgpack provides custom msgpack decoder.
func (result *SpaceSchemaResult) DecodeMsgpack(d *msgpack.Decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen == 0 {
		return fmt.Errorf("unexpected empty response array")
	}

	// DecodeMapLen inside SpaceSchema decode processes `nil` as zero length map,
	// so in `return nil, err` case we don't miss error info.
	// https://github.com/vmihailenco/msgpack/blob/3f7bd806fea698e7a9fe80979aa3512dea0a7368/decode_map.go#L79-L81
	if err = d.Decode(&result.Value); err != nil {
		return err
	}

	if arrLen > 1 {
		var crudErr *Error = nil

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
