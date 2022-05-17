package tarantool

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

// IntKey is utility type for passing integer key to Select*, Update* and Delete*.
// It serializes to array with single integer element.
type IntKey struct {
	I int
}

func (k IntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeInt(k.I)
	return nil
}

// UintKey is utility type for passing unsigned integer key to Select*, Update* and Delete*.
// It serializes to array with single integer element.
type UintKey struct {
	I uint
}

func (k UintKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeUint(k.I)
	return nil
}

// UintKey is utility type for passing string key to Select*, Update* and Delete*.
// It serializes to array with single string element.
type StringKey struct {
	S string
}

func (k StringKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(1)
	enc.EncodeString(k.S)
	return nil
}

// IntIntKey is utility type for passing two integer keys to Select*, Update* and Delete*.
// It serializes to array with two integer elements.
type IntIntKey struct {
	I1, I2 int
}

func (k IntIntKey) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(2)
	enc.EncodeInt(k.I1)
	enc.EncodeInt(k.I2)
	return nil
}

// Op - is update operation.
type Op struct {
	Op    string
	Field int
	Arg   interface{}
}

func (o Op) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(3)
	enc.EncodeString(o.Op)
	enc.EncodeInt(o.Field)
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
	ops []Op
}

// NewOperations returns a new empty collection of update operations.
func NewOperations() *Operations {
	ops := new(Operations)
	return ops
}

func (ops *Operations) append(op string, field int, arg interface{}) *Operations {
	ops.ops = append(ops.ops, Op{op, field, arg})
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
func (ops *Operations) Splice(field int, arg interface{}) *Operations {
	return ops.append(spliceOperator, field, arg)
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

type OpSplice struct {
	Op      string
	Field   int
	Pos     int
	Len     int
	Replace string
}

func (o OpSplice) EncodeMsgpack(enc *msgpack.Encoder) error {
	enc.EncodeArrayLen(5)
	enc.EncodeString(o.Op)
	enc.EncodeInt(o.Field)
	enc.EncodeInt(o.Pos)
	enc.EncodeInt(o.Len)
	enc.EncodeString(o.Replace)
	return nil
}
