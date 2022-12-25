package tarantool

import (
	"bytes"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"
)

const errorExtID = 3

const (
	keyErrorStack   = 0x00
	keyErrorType    = 0x00
	keyErrorFile    = 0x01
	keyErrorLine    = 0x02
	keyErrorMessage = 0x03
	keyErrorErrno   = 0x04
	keyErrorErrcode = 0x05
	keyErrorFields  = 0x06
)

// BoxError is a type representing Tarantool `box.error` object: a single
// MP_ERROR_STACK object with a link to the previous stack error.
// See https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_error/error/
//
// Since 1.10.0
type BoxError struct {
	// Type is error type that implies its source (for example, "ClientError").
	Type string
	// File is a source code file where the error was caught.
	File string
	// Line is a number of line in the source code file where the error was caught.
	Line uint64
	// Msg is the text of reason.
	Msg string
	// Errno is the ordinal number of the error.
	Errno uint64
	// Code is the number of the error as defined in `errcode.h`.
	Code uint64
	// Fields are additional fields depending on error type. For example, if
	// type is "AccessDeniedError", then it will include "object_type",
	// "object_name", "access_type".
	Fields map[string]interface{}
	// Prev is the previous error in stack.
	Prev *BoxError
}

// Error converts a BoxError to a string.
func (e *BoxError) Error() string {
	s := fmt.Sprintf("%s (%s, code 0x%x), see %s line %d",
		e.Msg, e.Type, e.Code, e.File, e.Line)

	if e.Prev != nil {
		return fmt.Sprintf("%s: %s", s, e.Prev)
	}

	return s
}

// Depth computes the count of errors in stack, including the current one.
func (e *BoxError) Depth() int {
	depth := int(0)

	cur := e
	for cur != nil {
		cur = cur.Prev
		depth++
	}

	return depth
}

func decodeBoxError(d *msgpack.Decoder) (*BoxError, error) {
	var l, larr, l1, l2 int
	var errorStack []BoxError
	var err error

	if l, err = d.DecodeMapLen(); err != nil {
		return nil, err
	}

	for ; l > 0; l-- {
		var cd int
		if cd, err = d.DecodeInt(); err != nil {
			return nil, err
		}
		switch cd {
		case keyErrorStack:
			if larr, err = d.DecodeArrayLen(); err != nil {
				return nil, err
			}

			errorStack = make([]BoxError, larr)

			for i := 0; i < larr; i++ {
				if l1, err = d.DecodeMapLen(); err != nil {
					return nil, err
				}

				for ; l1 > 0; l1-- {
					var cd1 int
					if cd1, err = d.DecodeInt(); err != nil {
						return nil, err
					}
					switch cd1 {
					case keyErrorType:
						if errorStack[i].Type, err = d.DecodeString(); err != nil {
							return nil, err
						}
					case keyErrorFile:
						if errorStack[i].File, err = d.DecodeString(); err != nil {
							return nil, err
						}
					case keyErrorLine:
						if errorStack[i].Line, err = d.DecodeUint64(); err != nil {
							return nil, err
						}
					case keyErrorMessage:
						if errorStack[i].Msg, err = d.DecodeString(); err != nil {
							return nil, err
						}
					case keyErrorErrno:
						if errorStack[i].Errno, err = d.DecodeUint64(); err != nil {
							return nil, err
						}
					case keyErrorErrcode:
						if errorStack[i].Code, err = d.DecodeUint64(); err != nil {
							return nil, err
						}
					case keyErrorFields:
						var mapk string
						var mapv interface{}

						errorStack[i].Fields = make(map[string]interface{})

						if l2, err = d.DecodeMapLen(); err != nil {
							return nil, err
						}
						for ; l2 > 0; l2-- {
							if mapk, err = d.DecodeString(); err != nil {
								return nil, err
							}
							if mapv, err = d.DecodeInterface(); err != nil {
								return nil, err
							}
							errorStack[i].Fields[mapk] = mapv
						}
					default:
						if err = d.Skip(); err != nil {
							return nil, err
						}
					}
				}

				if i > 0 {
					errorStack[i-1].Prev = &errorStack[i]
				}
			}
		default:
			if err = d.Skip(); err != nil {
				return nil, err
			}
		}
	}

	if len(errorStack) == 0 {
		return nil, fmt.Errorf("msgpack: unexpected empty BoxError stack on decode")
	}

	return &errorStack[0], nil
}

func encodeBoxError(enc *msgpack.Encoder, boxError *BoxError) error {
	if boxError == nil {
		return fmt.Errorf("msgpack: unexpected nil BoxError on encode")
	}

	if err := enc.EncodeMapLen(1); err != nil {
		return err
	}
	if err := enc.EncodeUint(keyErrorStack); err != nil {
		return err
	}

	var stackDepth = boxError.Depth()
	if err := enc.EncodeArrayLen(stackDepth); err != nil {
		return err
	}

	for ; stackDepth > 0; stackDepth-- {
		fieldsLen := len(boxError.Fields)

		if fieldsLen > 0 {
			if err := enc.EncodeMapLen(7); err != nil {
				return err
			}
		} else {
			if err := enc.EncodeMapLen(6); err != nil {
				return err
			}
		}

		if err := enc.EncodeUint(keyErrorType); err != nil {
			return err
		}
		if err := enc.EncodeString(boxError.Type); err != nil {
			return err
		}

		if err := enc.EncodeUint(keyErrorFile); err != nil {
			return err
		}
		if err := enc.EncodeString(boxError.File); err != nil {
			return err
		}

		if err := enc.EncodeUint(keyErrorLine); err != nil {
			return err
		}
		if err := enc.EncodeUint64(boxError.Line); err != nil {
			return err
		}

		if err := enc.EncodeUint(keyErrorMessage); err != nil {
			return err
		}
		if err := enc.EncodeString(boxError.Msg); err != nil {
			return err
		}

		if err := enc.EncodeUint(keyErrorErrno); err != nil {
			return err
		}
		if err := enc.EncodeUint64(boxError.Errno); err != nil {
			return err
		}

		if err := enc.EncodeUint(keyErrorErrcode); err != nil {
			return err
		}
		if err := enc.EncodeUint64(boxError.Code); err != nil {
			return err
		}

		if fieldsLen > 0 {
			if err := enc.EncodeUint(keyErrorFields); err != nil {
				return err
			}

			if err := enc.EncodeMapLen(fieldsLen); err != nil {
				return err
			}

			for k, v := range boxError.Fields {
				if err := enc.EncodeString(k); err != nil {
					return err
				}
				if err := enc.Encode(v); err != nil {
					return err
				}
			}
		}

		if stackDepth > 1 {
			boxError = boxError.Prev
		}
	}

	return nil
}

// UnmarshalMsgpack deserializes a BoxError value from a MessagePack
// representation.
func (e *BoxError) UnmarshalMsgpack(b []byte) error {
	if e == nil {
		panic("cannot unmarshal to a nil pointer")
	}

	buf := bytes.NewBuffer(b)
	dec := msgpack.NewDecoder(buf)

	if val, err := decodeBoxError(dec); err != nil {
		return err
	} else {
		*e = *val
		return nil
	}
}

// MarshalMsgpack serializes the BoxError into a MessagePack representation.
func (e *BoxError) MarshalMsgpack() ([]byte, error) {
	var buf bytes.Buffer

	enc := msgpack.NewEncoder(&buf)
	if err := encodeBoxError(enc, e); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func init() {
	msgpack.RegisterExt(errorExtID, (*BoxError)(nil))
}
