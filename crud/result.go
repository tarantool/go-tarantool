package crud

import (
	"fmt"
)

// FieldFormat contains field definition: {name='...',type='...'[,is_nullable=...]}.
type FieldFormat struct {
	Name       string
	Type       string
	IsNullable bool
}

// DecodeMsgpack provides custom msgpack decoder.
func (format *FieldFormat) DecodeMsgpack(d *decoder) error {
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
		case "name":
			if format.Name, err = d.DecodeString(); err != nil {
				return err
			}
		case "type":
			if format.Type, err = d.DecodeString(); err != nil {
				return err
			}
		case "is_nullable":
			if format.IsNullable, err = d.DecodeBool(); err != nil {
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

// Result describes CRUD result as an object containing metadata and rows.
type Result struct {
	Metadata []FieldFormat
	Rows     []interface{}
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *Result) DecodeMsgpack(d *decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen < 2 {
		return fmt.Errorf("array len doesn't match: %d", arrLen)
	}

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
		case "metadata":
			metadataLen, err := d.DecodeArrayLen()
			if err != nil {
				return err
			}

			metadata := make([]FieldFormat, metadataLen)

			for i := 0; i < metadataLen; i++ {
				fieldFormat := FieldFormat{}
				if err = d.Decode(&fieldFormat); err != nil {
					return err
				}

				metadata[i] = fieldFormat
			}

			r.Metadata = metadata
		case "rows":
			if err = d.Decode(&r.Rows); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	var crudErr *Error = nil

	if err := d.Decode(&crudErr); err != nil {
		return err
	}

	for i := 2; i < arrLen; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}

	if crudErr != nil {
		return crudErr
	}

	return nil
}

// ResultMany describes CRUD result as an object containing metadata and rows.
type ResultMany struct {
	Metadata []FieldFormat
	Rows     []interface{}
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *ResultMany) DecodeMsgpack(d *decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen < 2 {
		return fmt.Errorf("array len doesn't match: %d", arrLen)
	}

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
		case "metadata":
			metadataLen, err := d.DecodeArrayLen()
			if err != nil {
				return err
			}

			metadata := make([]FieldFormat, metadataLen)

			for i := 0; i < metadataLen; i++ {
				fieldFormat := FieldFormat{}
				if err = d.Decode(&fieldFormat); err != nil {
					return err
				}

				metadata[i] = fieldFormat
			}

			r.Metadata = metadata
		case "rows":
			if err = d.Decode(&r.Rows); err != nil {
				return err
			}
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	errLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	var errs []Error
	for i := 0; i < errLen; i++ {
		var crudErr *Error = nil

		if err := d.Decode(&crudErr); err != nil {
			return err
		} else if crudErr != nil {
			errs = append(errs, *crudErr)
		}
	}

	for i := 2; i < arrLen; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}

	if len(errs) > 0 {
		return &ErrorMany{Errors: errs}
	}

	return nil
}

// NumberResult describes CRUD result as an object containing number.
type NumberResult struct {
	Value uint64
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *NumberResult) DecodeMsgpack(d *decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrLen < 2 {
		return fmt.Errorf("array len doesn't match: %d", arrLen)
	}

	if r.Value, err = d.DecodeUint64(); err != nil {
		return err
	}

	var crudErr *Error = nil

	if err := d.Decode(&crudErr); err != nil {
		return err
	}

	for i := 2; i < arrLen; i++ {
		if err := d.Skip(); err != nil {
			return err
		}
	}

	if crudErr != nil {
		return crudErr
	}

	return nil
}

// BoolResult describes CRUD result as an object containing bool.
type BoolResult struct {
	Value bool
}

// DecodeMsgpack provides custom msgpack decoder.
func (r *BoolResult) DecodeMsgpack(d *decoder) error {
	arrLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}
	if arrLen < 2 {
		if r.Value, err = d.DecodeBool(); err != nil {
			return err
		}

		return nil
	}

	if _, err = d.DecodeInterface(); err != nil {
		return err
	}

	var crudErr *Error = nil

	if err := d.Decode(&crudErr); err != nil {
		return err
	}

	if crudErr != nil {
		return crudErr
	}

	return nil
}
