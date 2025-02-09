package arrow

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

// ExtID represents the Arrow MessagePack extension type identifier.
const ExtID = 8

// Arrow struct wraps a raw arrow data buffer.
type Arrow struct {
	data []byte
}

// MakeArrow returns a new arrow.Arrow object that contains
// wrapped a raw arrow data buffer.
func MakeArrow(arrow []byte) (Arrow, error) {
	return Arrow{arrow}, nil
}

// Raw returns a []byte that contains Arrow raw data.
func (a Arrow) Raw() []byte {
	return a.data
}

// EncodeExt encodes an Arrow into a MessagePack extension.
func EncodeExt(_ *msgpack.Encoder, v reflect.Value) ([]byte, error) {
	arr, ok := v.Interface().(Arrow)
	if !ok {
		return []byte{}, fmt.Errorf("encode: not an Arrow type")
	}
	return arr.data, nil
}

// DecodeExt decodes a MessagePack extension into an Arrow.
func DecodeExt(d *msgpack.Decoder, v reflect.Value, extLen int) error {
	arrow := Arrow{
		data: make([]byte, extLen),
	}
	n, err := d.Buffered().Read(arrow.data)
	if err != nil {
		return fmt.Errorf("decode: can't read bytes on Arrow decode: %w", err)
	}
	if n < extLen || n != len(arrow.data) {
		return fmt.Errorf("decode: unexpected end of stream after %d Arrow bytes", n)
	}

	v.Set(reflect.ValueOf(arrow))
	return nil
}

func init() {
	msgpack.RegisterExtEncoder(ExtID, Arrow{}, EncodeExt)
	msgpack.RegisterExtDecoder(ExtID, Arrow{}, DecodeExt)
}
