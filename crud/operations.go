package crud

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
type Operation struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Operator Operator
	Field    interface{} // Number or string.
	Value    interface{}
}
