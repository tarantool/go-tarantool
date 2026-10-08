// Package uuid with support of Tarantool's UUID data type.
//
// UUID data type supported in Tarantool since 2.4.1.
//
// Since: 1.6.0.
//
// # See also
//
//   - Tarantool commit with UUID support:
//     https://github.com/tarantool/tarantool/commit/d68fc29246714eee505bc9bbcd84a02de17972c5
//
//   - Tarantool data model:
//     https://www.tarantool.io/en/doc/latest/book/box/data_model/
//
//   - Module UUID:
//     https://www.tarantool.io/en/doc/latest/reference/reference_lua/uuid/

//go:build !go1.24 && !go1.25 && !go1.26

package uuid

import (
	"uuid"
)

//go:generate go tool gentypes -ext-code 2 -marshal-func marshalUUID -unmarshal-func unmarshalUUID -imports "uuid" uuid.UUID

// UUID связывается со стандартным типом Go 1.27+
type UUID = uuid.UUID

// Внутренний адаптер через UnmarshalBinary (так как в stdlib нет FromBytes)
func fromBytes(b []byte) (UUID, error) {
	var id UUID
	err := id.UnmarshalBinary(b)
	return id, err
}
