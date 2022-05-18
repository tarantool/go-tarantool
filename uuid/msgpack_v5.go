//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package uuid

import (
	"reflect"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder
type decoder = msgpack.Decoder

func init() {
	msgpack.Register(reflect.TypeOf((*uuid.UUID)(nil)).Elem(), encodeUUID, decodeUUID)
	msgpack.RegisterExtEncoder(UUID_extId, uuid.UUID{},
		func(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
			uuid := v.Interface().(uuid.UUID)
			return uuid.MarshalBinary()
		})
	msgpack.RegisterExtDecoder(UUID_extId, uuid.UUID{},
		func(d *msgpack.Decoder, v reflect.Value, extLen int) error {
			return decodeUUID(d, v)
		})
}
