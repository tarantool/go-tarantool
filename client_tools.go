package tarantool

import (
	"github.com/vmihailenco/msgpack/v5"
)

// IntKey is utility type for passing integer key to Select*, Update*,
// Delete* and GetTyped. It serializes to array with single integer element.
type IntKey struct {
	I int
}

func (k IntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeInt(int64(k.I))
	return nil
}

// UintKey is utility type for passing unsigned integer key to Select*,
// Update*, Delete* and GetTyped. It serializes to array with single unsigned
// integer element.
type UintKey struct {
	I uint
}

func (k UintKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeUint(uint64(k.I))
	return nil
}

// StringKey is utility type for passing string key to Select*, Update*,
// Delete* and GetTyped. It serializes to array with single string element.
type StringKey struct {
	S string
}

func (k StringKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeString(k.S)
	return nil
}

// IntIntKey is utility type for passing two integer keys to Select*, Update*,
// Delete* and GetTyped. It serializes to array with two integer elements.
type IntIntKey struct {
	I1, I2 int
}

func (k IntIntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(2)
	enc.EncodeInt(int64(k.I1))
	enc.EncodeInt(int64(k.I2))
	return nil
}

// operation - is update operation.
type operation struct {
	Op    string
	Field int
	Arg   interface{}
	// Pos, Len, Replace fields used in the Splice operation.
	Pos     int
	Len     int
	Replace string
}

func (o operation) EncodeMsgpack(enc *msgpack.Encoder) error {
	isSpliceOperation := o.Op == spliceOperator
	argsLen := 3
	if isSpliceOperation {
		argsLen = 5
	}
	if err := enc.EncodeArrayLen(argsLen); err != nil {
		return err
	}
	if err := enc.EncodeString(o.Op); err != nil {
		return err
	}
	if err := enc.EncodeInt(int64(o.Field)); err != nil {
		return err
	}

	if isSpliceOperation {
		if err := enc.EncodeInt(int64(o.Pos)); err != nil {
			return err
		}
		if err := enc.EncodeInt(int64(o.Len)); err != nil {
			return err
		}
		return enc.EncodeString(o.Replace)
	}

	return enc.Encode(o.Arg)
}

const (
	appendOperator      = "+"
	subtractionOperator = "-"
	bitwiseAndOperator  = "&"
	bitwiseOrOperator   = "|"
	bitwiseXorOperator  = "^"
	spliceOperator      = ":"
	insertOperator      = "!"
	deleteOperator      = "#"
	assignOperator      = "="
)

// Operations is a collection of update operations.
type Operations struct {
	ops []operation
}

// EncodeMsgpack encodes Operations as an array of operations.
func (ops *Operations) EncodeMsgpack(enc *msgpack.Encoder) error {
	return enc.Encode(ops.ops)
}

// NewOperations returns a new empty collection of update operations.
func NewOperations() *Operations {
	return &Operations{[]operation{}}
}

func (ops *Operations) append(op string, field int, arg interface{}) *Operations {
	ops.ops = append(ops.ops, operation{Op: op, Field: field, Arg: arg})
	return ops
}

func (ops *Operations) appendSplice(op string, field, pos, len int, replace string) *Operations {
	ops.ops = append(ops.ops, operation{Op: op, Field: field, Pos: pos, Len: len, Replace: replace})
	return ops
}

// Add adds an additional operation to the collection of update operations.
func (ops *Operations) Add(field int, arg interface{}) *Operations {
	return ops.append(appendOperator, field, arg)
}

// Subtract adds a subtraction operation to the collection of update operations.
func (ops *Operations) Subtract(field int, arg interface{}) *Operations {
	return ops.append(subtractionOperator, field, arg)
}

// BitwiseAnd adds a bitwise AND operation to the collection of update operations.
func (ops *Operations) BitwiseAnd(field int, arg interface{}) *Operations {
	return ops.append(bitwiseAndOperator, field, arg)
}

// BitwiseOr adds a bitwise OR operation to the collection of update operations.
func (ops *Operations) BitwiseOr(field int, arg interface{}) *Operations {
	return ops.append(bitwiseOrOperator, field, arg)
}

// BitwiseXor adds a bitwise XOR operation to the collection of update operations.
func (ops *Operations) BitwiseXor(field int, arg interface{}) *Operations {
	return ops.append(bitwiseXorOperator, field, arg)
}

// Splice adds a splice operation to the collection of update operations.
func (ops *Operations) Splice(field, pos, len int, replace string) *Operations {
	return ops.appendSplice(spliceOperator, field, pos, len, replace)
}

// Insert adds an insert operation to the collection of update operations.
func (ops *Operations) Insert(field int, arg interface{}) *Operations {
	return ops.append(insertOperator, field, arg)
}

// Delete adds a delete operation to the collection of update operations.
func (ops *Operations) Delete(field int, arg interface{}) *Operations {
	return ops.append(deleteOperator, field, arg)
}

// Assign adds an assign operation to the collection of update operations.
func (ops *Operations) Assign(field int, arg interface{}) *Operations {
	return ops.append(assignOperator, field, arg)
}
