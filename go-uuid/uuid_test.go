package uuid_test

import (
	"bytes"
	"testing"
	"uuid"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	tarantooluuid "github.com/tarantool/go-tarantool/go-uuid"
)

var wireUUID = []byte{
	0xd8, 0x02, 0xc8, 0xf0, 0xfa, 0x1f, 0xda, 0x29, 0x43,
	0x8c, 0xa0, 0x40, 0x39, 0x3f, 0x11, 0x26, 0xad, 0x39,
}

func TestUUID(t *testing.T) {
	id := uuid.MustParse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	for _, value := range []any{id, &id} {
		encoded, err := msgpack.Marshal(value)
		require.NoError(t, err)
		require.Equal(t, wireUUID, encoded)
	}

	var decoded uuid.UUID
	require.NoError(t, msgpack.Unmarshal(wireUUID, &decoded))
	require.Equal(t, id, decoded)

	var pointer *uuid.UUID
	require.NoError(t, msgpack.Unmarshal(wireUUID, &pointer))
	require.Equal(t, id, *pointer)

	var untyped any
	require.NoError(t, msgpack.Unmarshal(wireUUID, &untyped))
	require.Equal(t, id, untyped)

	var zero uuid.UUID
	encoded, err := msgpack.Marshal(zero)
	require.NoError(t, err)
	require.Equal(t, append([]byte{0xd8, 0x02}, make([]byte, 16)...), encoded)
}

type fragmentedReader struct{ *bytes.Reader }

func (r fragmentedReader) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(1, len(p))])
}

func TestUUIDFragmentedRead(t *testing.T) {
	reader := fragmentedReader{bytes.NewReader(wireUUID)}
	var decoded uuid.UUID
	require.NoError(t, msgpack.NewDecoder(reader).Decode(&decoded))
	require.Equal(t, uuid.MustParse("c8f0fa1f-da29-438c-a040-393f1126ad39"), decoded)
}

func TestUUIDInvalidPayload(t *testing.T) {
	for _, data := range [][]byte{
		{0xc7, 0, 2},
		append([]byte{0xc7, 15, 2}, make([]byte, 15)...),
		append([]byte{0xc7, 17, 2}, make([]byte, 17)...),
		wireUUID[:len(wireUUID)-1],
	} {
		var decoded uuid.UUID
		require.Error(t, msgpack.Unmarshal(data, &decoded))
		var untyped any
		require.Error(t, msgpack.Unmarshal(data, &untyped))
	}
}

func TestOptionalUUIDWire(t *testing.T) {
	id := uuid.MustParse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	encoded, err := msgpack.Marshal(tarantooluuid.SomeOptionalUUID(id))
	require.NoError(t, err)
	require.Equal(t, wireUUID, encoded)

	var decoded tarantooluuid.OptionalUUID
	require.NoError(t, msgpack.Unmarshal(wireUUID, &decoded))
	require.Equal(t, id, decoded.MustGet())

	encoded, err = msgpack.Marshal(tarantooluuid.NoneOptionalUUID())
	require.NoError(t, err)
	require.Equal(t, []byte{0xc0}, encoded)

	for _, data := range [][]byte{
		{0xc7, 0, 2},
		append([]byte{0xc7, 15, 2}, make([]byte, 15)...),
		append([]byte{0xc7, 17, 2}, make([]byte, 17)...),
		append([]byte{0xd8, 3}, make([]byte, 16)...),
		wireUUID[:len(wireUUID)-1],
	} {
		var invalid tarantooluuid.OptionalUUID
		require.Error(t, msgpack.Unmarshal(data, &invalid))
		require.True(t, invalid.IsZero())
	}
}
