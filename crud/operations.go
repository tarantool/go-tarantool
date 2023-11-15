package crud

import (
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// Add - operator for addition.
	Add Operator = "+"
	// Sub - operator for subtraction.
	Sub Operator = "-"
	// And - operator for bitwise AND.
	And Operator = "&"
	// Or - operator for bitwise OR.
	Or Operator = "|"
	// Xor - operator for bitwise XOR.
	Xor Operator = "^"
	// Splice - operator for string splice.
	Splice Operator = ":"
	// Insert - operator for insertion of a new field.
	Insert Operator = "!"
	// Delete - operator for deletion.
	Delete Operator = "#"
	// Assign - operator for assignment.
	Assign Operator = "="
)

// Operation describes CRUD operation as a table
// {operator, field_identifier, value}.
// Splice operation described as a table
// {operator, field_identifier, position, length, replace_string}.
type Operation struct {
	Operator Operator
	Field    interface{} // Number or string.
	Value    interface{}
	// Pos, Len, Replace fields used in the Splice operation.
	Pos     int
	Len     int
	Replace string
}

// EncodeMsgpack encodes Operation.
func (o Operation) EncodeMsgpack(enc *msgpack.Encoder) error {
	isSpliceOperation := o.Operator == Splice
	argsLen := 3
	if isSpliceOperation {
		argsLen = 5
	}
	if err := enc.EncodeArrayLen(argsLen); err != nil {
		return err
	}
	if err := enc.EncodeString(string(o.Operator)); err != nil {
		return err
	}
	if err := enc.Encode(o.Field); err != nil {
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

	return enc.Encode(o.Value)
}
