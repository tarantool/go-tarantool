package uuid

import (
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"gopkg.in/vmihailenco/msgpack.v2"
)

// UUID external type
// Supported since Tarantool 2.4.1. See more in commit messages.
// https://github.com/tarantool/tarantool/commit/d68fc29246714eee505bc9bbcd84a02de17972c5

const UUID_extId = 2

func encodeUUID(e *msgpack.Encoder, v reflect.Value) error {
	id := v.Interface().(uuid.UUID)

	bytes, err := id.MarshalBinary()
	if err != nil {
		return fmt.Errorf("msgpack: can't marshal binary uuid: %w", err)
	}

	_, err = e.Writer().Write(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't write bytes to encoder writer: %w", err)
	}

	return nil
}

func decodeUUID(d *msgpack.Decoder, v reflect.Value) error {
	var bytesCount int = 16
	bytes := make([]byte, bytesCount)

	n, err := d.Buffered().Read(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't read bytes on uuid decode: %w", err)
	}
	if n < bytesCount {
		return fmt.Errorf("msgpack: unexpected end of stream after %d uuid bytes", n)
	}

	id, err := uuid.FromBytes(bytes)
	if err != nil {
		return fmt.Errorf("msgpack: can't create uuid from bytes: %w", err)
	}

	v.Set(reflect.ValueOf(id))
	return nil
}

func init() {
	msgpack.Register(reflect.TypeOf((*uuid.UUID)(nil)).Elem(), encodeUUID, decodeUUID)
	msgpack.RegisterExt(UUID_extId, (*uuid.UUID)(nil))
}
