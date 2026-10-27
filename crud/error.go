package crud

import (
	"reflect"
	"strings"

	"github.com/tarantool/go-option"
	"github.com/vmihailenco/msgpack/v5"
)

// Error describes CRUD error object.
type Error struct {
	// ClassName is an error class that implies its source (for example, "CountError").
	ClassName string
	// Err is the text of reason.
	Err string
	// File is a source code file where the error was caught.
	File string
	// Line is a number of line in the source code file where the error was caught.
	Line uint64
	// Stack is an information about the call stack when an error
	// occurs in a string format.
	Stack string
	// Str is the text of reason with error class.
	Str string
	// OperationData is the object/tuple with which an error occurred.
	OperationData any
	// OperationIndex is the index of the operation that failed in a batch.
	OperationIndex uint64
	// UnrefError is an optional error of releasing a bucket lock.
	UnrefError any
	// operationDataType contains the type of OperationData.
	operationDataType reflect.Type
}

// newError creates an Error object with a custom operation data type to decoding.
func newError(operationDataType reflect.Type) *Error {
	return &Error{operationDataType: operationDataType}
}

// decodeField decodes a single Error field from a msgpack map. It is reused
// by error types that extend Error with additional fields.
func (e *Error) decodeField(d *msgpack.Decoder, key string) error {
	var err error

	switch key {
	case "class_name":
		if e.ClassName, err = d.DecodeString(); err != nil {
			return err
		}
	case "err":
		if e.Err, err = d.DecodeString(); err != nil {
			return err
		}
	case "file":
		if e.File, err = d.DecodeString(); err != nil {
			return err
		}
	case "line":
		if e.Line, err = d.DecodeUint64(); err != nil {
			return err
		}
	case "stack":
		if e.Stack, err = d.DecodeString(); err != nil {
			return err
		}
	case "str":
		if e.Str, err = d.DecodeString(); err != nil {
			return err
		}
	case "operation_data":
		if e.operationDataType != nil {
			tuple := reflect.New(e.operationDataType)
			if err = d.DecodeValue(tuple); err != nil {
				return err
			}
			e.OperationData = tuple.Elem().Interface()
		} else {
			if err = d.Decode(&e.OperationData); err != nil {
				return err
			}
		}
	case "operation_index":
		if e.OperationIndex, err = d.DecodeUint64(); err != nil {
			return err
		}
	case "unref_error":
		if err = d.Decode(&e.UnrefError); err != nil {
			return err
		}
	default:
		return d.Skip()
	}

	return nil
}

// DecodeMsgpack provides custom msgpack decoder.
func (e *Error) DecodeMsgpack(d *msgpack.Decoder) error {
	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	for range l {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}
		if err := e.decodeField(d, key); err != nil {
			return err
		}
	}

	return nil
}

// Error converts an Error to a string.
func (e Error) Error() string {
	return e.Str
}

// ErrorMany describes CRUD error object for `_many` methods.
type ErrorMany struct {
	Errors []Error
	// operationDataType contains the type of OperationData for each Error.
	operationDataType reflect.Type
}

// newErrorMany creates an ErrorMany object with a custom operation data type to decoding.
func newErrorMany(operationDataType reflect.Type) *ErrorMany {
	return &ErrorMany{operationDataType: operationDataType}
}

// DecodeMsgpack provides custom msgpack decoder.
func (e *ErrorMany) DecodeMsgpack(d *msgpack.Decoder) error {
	l, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	var errs []Error
	for range l {
		crudErr := newError(e.operationDataType)
		if err := d.Decode(&crudErr); err != nil {
			return err
		}
		errs = append(errs, *crudErr)
	}

	if len(errs) > 0 {
		e.Errors = errs
	}

	return nil
}

// Error converts an Error to a string.
func (e ErrorMany) Error() string {
	var str []string
	for _, err := range e.Errors {
		str = append(str, err.Str)
	}

	return strings.Join(str, "\n")
}

// StorageCallError describes the error returned by `crud.storage_call`.
type StorageCallError struct {
	// Base is the common CRUD error information.
	Base Error
	// MayHaveSideEffects indicates whether the target function may have
	// committed changes before failing.
	MayHaveSideEffects option.Bool
	// FuncName is the name of the target function.
	FuncName option.String
	// BucketId is the bucket ID used to route the call.
	BucketId option.Uint
	// ReplicasetId is the ID of the replicaset where the error occurred.
	ReplicasetId option.String
}

// Error converts a StorageCallError to a string.
func (e StorageCallError) Error() string {
	return e.Base.Str
}

// DecodeMsgpack provides custom msgpack decoder.
func (e *StorageCallError) DecodeMsgpack(d *msgpack.Decoder) error {
	l, err := d.DecodeMapLen()
	if err != nil {
		return err
	}
	for range l {
		key, err := d.DecodeString()
		if err != nil {
			return err
		}

		switch key {
		case "may_have_side_effects":
			if err := d.Decode(&e.MayHaveSideEffects); err != nil {
				return err
			}
		case "func_name":
			if err := d.Decode(&e.FuncName); err != nil {
				return err
			}
		case "bucket_id":
			if err := d.Decode(&e.BucketId); err != nil {
				return err
			}
		case "replicaset_id":
			if err := d.Decode(&e.ReplicasetId); err != nil {
				return err
			}
		default:
			if err := e.Base.decodeField(d, key); err != nil {
				return err
			}
		}
	}

	return nil
}
