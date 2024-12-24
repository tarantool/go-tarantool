package box

import "github.com/tarantool/go-tarantool/v2"

// Schema represents the schema-related operations in Tarantool.
// It holds a connection to interact with the Tarantool instance.
type Schema struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// NewSchema creates a new Schema instance with the provided Tarantool connection.
// It initializes a Schema object that can be used for schema-related operations
// such as managing users, tables, and other schema elements in the Tarantool instance.
func NewSchema(conn tarantool.Doer) *Schema {
	return &Schema{conn: conn} // Pass the connection to the Schema.
}

// User returns a new SchemaUser instance, allowing schema-related user operations.
func (s *Schema) User() *SchemaUser {
	return NewSchemaUser(s.conn)
}
