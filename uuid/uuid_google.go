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

//go:build go1.24 || go1.25 || go1.26

package uuid

import (
	"github.com/google/uuid"
)

//go:generate go tool gentypes -ext-code 2 -marshal-func marshalUUID -unmarshal-func unmarshalUUID -imports "github.com/google/uuid" uuid.UUID

// UUID связывается с типом из внешней библиотеки
type UUID = uuid.UUID

// Внутренний адаптер для создания UUID из сырых байт msgpack
func fromBytes(b []byte) (UUID, error) {
	return uuid.FromBytes(b)
}
