package arrow

import (
	"fmt"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
)

//go:generate go tool gentypes -ext-code 8 Arrow

// Arrow MessagePack extension type.
const arrowExtId = 8

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

// MarshalMsgpack implements a custom msgpack marshaler for extension type.
func (a Arrow) MarshalMsgpack() ([]byte, error) {
	return a.data, nil
}

// UnmarshalMsgpack implements a custom msgpack unmarshaler for extension type.
func (a *Arrow) UnmarshalMsgpack(data []byte) error {
	a.data = data
	return nil
}

func arrowDecoder(d *msgpack.Decoder, v reflect.Value, extLen int) error {
	arrow := Arrow{
		data: make([]byte, extLen),
	}
	n, err := d.Buffered().Read(arrow.data)
	if err != nil {
		return fmt.Errorf("arrowDecoder: can't read bytes on Arrow decode: %w", err)
	}
	if n < extLen || n != len(arrow.data) {
		return fmt.Errorf("arrowDecoder: unexpected end of stream after %d Arrow bytes", n)
	}

	v.Set(reflect.ValueOf(arrow))
	return nil
}

func arrowEncoder(e *msgpack.Encoder, v reflect.Value) ([]byte, error) {
	arr, ok := v.Interface().(Arrow)
	if !ok {
		return []byte{}, fmt.Errorf("arrowEncoder: not an Arrow type")
	}
	return arr.data, nil
}

func init() {
	msgpack.RegisterExtDecoder(arrowExtId, Arrow{}, arrowDecoder)
	msgpack.RegisterExtEncoder(arrowExtId, Arrow{}, arrowEncoder)
}
