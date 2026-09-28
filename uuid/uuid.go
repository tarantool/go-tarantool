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
package uuid

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// UUID external type.
const uuid_extID = 2

func marshalUUID(id UUID) ([]byte, error) {
	return id.MarshalBinary()
}

func unmarshalUUID(id *UUID, data []byte) error {
	return id.UnmarshalBinary(data)
}

// encodeUUID encodes a UUID value into the msgpack format.
func encodeUUID(e *msgpack.Encoder, v reflect.Value) error {
	id := v.Interface().(UUID)

	bytes, err := id.MarshalBinary()
	if err != nil {
		return fmt.Errorf("msgpack: can't marshal binary uuid: %w", err)
	}

	_, err = e.Writer().Write(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't write bytes to msgpack.Encoder writer: %w", err)
	}

	return nil
}

// decodeUUID decodes a UUID value from the msgpack format.
func decodeUUID(d *msgpack.Decoder, v reflect.Value) error {
	const bytesCount = 16
	bytes := make([]byte, bytesCount)

	n, err := d.Buffered().Read(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't read bytes on uuid decode: %w", err)
	}
	if n < bytesCount {
		return fmt.Errorf("msgpack: unexpected end of stream after %d uuid bytes", n)
	}

	// Используем пакетно-зависимую функцию парсинга байт
	id, err := fromBytes(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't create uuid from bytes: %w", err)
	}

	v.Set(reflect.ValueOf(id))
	return nil
}

func init() {
	// Регистрация типов msgpack v5 на основе нашего общего алиаса UUID
	msgpack.Register(reflect.TypeFor[UUID](), encodeUUID, decodeUUID)
	msgpack.RegisterExtEncoder(uuid_extID, UUID{},
		func(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
			uuid := v.Interface().(UUID)
			return uuid.MarshalBinary()
		})
	msgpack.RegisterExtDecoder(uuid_extID, UUID{},
		func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
			return decodeUUID(d, v)
		})
}
