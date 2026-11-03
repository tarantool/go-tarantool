package uuid

import (
	"fmt"
	"reflect"
	"uuid"

	"github.com/vmihailenco/msgpack/v5"
)

//go:generate go tool gentypes -ext-code 2 -marshal-func marshalUUID -unmarshal-func unmarshalUUID -imports "uuid" uuid.UUID

func marshalUUID(id uuid.UUID) ([]byte, error) {
	return id[:], nil
}

func unmarshalUUID(id *uuid.UUID, data []byte) error {
	if len(data) != len(id) {
		return fmt.Errorf("msgpack: invalid UUID length: %d", len(data))
	}
	copy(id[:], data)
	return nil
}

func init() {
	msgpack.RegisterExtEncoder(2, uuid.UUID{},
		func(_ *msgpack.Encoder, v reflect.Value) ([]byte, error) {
			return marshalUUID(v.Interface().(uuid.UUID))
		})
	msgpack.RegisterExtDecoder(2, uuid.UUID{},
		func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
			var id uuid.UUID
			if extLen != len(id) {
				return fmt.Errorf("msgpack: invalid UUID length: %d", extLen)
			}
			if err := d.ReadFull(id[:]); err != nil {
				return fmt.Errorf("msgpack: can't read UUID: %w", err)
			}
			v.Set(reflect.ValueOf(id))
			return nil
		})
}
