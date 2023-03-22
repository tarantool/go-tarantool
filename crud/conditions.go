package crud

// Operator is a type to describe operator of operation.
type Operator string

const (
	// Eq - comparison operator for "equal".
	Eq Operator = "="
	// Lt - comparison operator for "less than".
	Lt Operator = "<"
	// Le - comparison operator for "less than or equal".
	Le Operator = "<="
	// Gt - comparison operator for "greater than".
	Gt Operator = ">"
	// Ge - comparison operator for "greater than or equal".
	Ge Operator = ">="
)

// Condition describes CRUD condition as a table
// {operator, field-identifier, value}.
type Condition struct {
	// Instruct msgpack to pack this struct as array, so no custom packer
	// is needed.
	_msgpack struct{} `msgpack:",asArray"` //nolint: structcheck,unused
	Operator Operator
	Field    string // Field name or index name.
	Value    interface{}
}
