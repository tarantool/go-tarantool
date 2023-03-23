package crud

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// StatusTable describes information for instance.
type StatusTable struct {
	Status   string
	IsMaster bool
	Message  string
}

// DecodeMsgpack provides custom msgpack decoder.
func (statusTable *StatusTable) DecodeMsgpack(d *decoder) error {
	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	for i := 0; i < l; i++ {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}

		switch key {
		case "status":
			if statusTable.Status, err = d.DecodeString(); err != nil {
				return err
			}
		case "is_master":
			if statusTable.IsMaster, err = d.DecodeBool(); err != nil {
				return err
			}
		case "message":
			if statusTable.Message, err = d.DecodeString(); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	return nil
}

// StorageInfoResult describes result for `crud.storage_info` method.
type StorageInfoResult struct {
	Info map[string]StatusTable
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *StorageInfoResult) DecodeMsgpack(d *decoder) error {
	_, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}

	info := make(map[string]StatusTable)
	for i := 0; i < l; i++ {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}

		statusTable := StatusTable{}
		if err := d.Decode(&statusTable); err != nil {
			return nil
		}

		info[key] = statusTable
	}

	r.Info = info

	return nil
}

// StorageInfoOpts describes options for `crud.storage_info` method.
type StorageInfoOpts = BaseOpts

// StorageInfoRequest helps you to create request object to call
// `crud.storage_info` for execution by a Connection.
type StorageInfoRequest struct {
	baseRequest
	opts StorageInfoOpts
}

type storageInfoArgs struct {
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Opts     StorageInfoOpts
}

// MakeStorageInfoRequest returns a new empty StorageInfoRequest.
func MakeStorageInfoRequest() StorageInfoRequest {
	req := StorageInfoRequest{}
	req.impl = newCall("crud.storage_info")
	req.opts = StorageInfoOpts{}
	return req
}

// Opts sets the options for the torageInfoRequest request.
// Note: default value is nil.
func (req StorageInfoRequest) Opts(opts StorageInfoOpts) StorageInfoRequest {
	req.opts = opts
	return req
}

// Body fills an encoder with the call request body.
func (req StorageInfoRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	args := storageInfoArgs{Opts: req.opts}
	req.impl = req.impl.Args(args)
	return req.impl.Body(res, enc)
}

// Context sets a passed context to CRUD request.
func (req StorageInfoRequest) Context(ctx context.Context) StorageInfoRequest {
	req.impl = req.impl.Context(ctx)

	return req
}
