package crud

import "strings"

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
}

// DecodeMsgpack provides custom msgpack decoder.
func (e *Error) DecodeMsgpack(d *decoder) error {
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
		default:
			if err := d.Skip(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Error converts an Error to a string.
func (err Error) Error() string {
	return err.Str
}

// ErrorMany describes CRUD error object for `_many` methods.
type ErrorMany struct {
	Errors []Error
}

// Error converts an Error to a string.
func (errs ErrorMany) Error() string {
	var str []string
	for _, err := range errs.Errors {
		str = append(str, err.Str)
	}

	return strings.Join(str, "\n")
}
