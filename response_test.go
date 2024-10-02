package tarantool_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
)

func encodeResponseData(t *testing.T, data interface{}) io.Reader {
	t.Helper()

	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	enc.EncodeMapLen(1)
	enc.EncodeUint8(uint8(iproto.IPROTO_DATA))
	enc.Encode([]interface{}{data})
	return buf

}

func TestDecodeBaseResponse(t *testing.T) {
	tests := []struct {
		name   string
		header tarantool.Header
		body   interface{}
	}{
		{
			"test1",
			tarantool.Header{},
			nil,
		},
		{
			"test2",
			tarantool.Header{RequestId: 123},
			[]byte{'v', '2'},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tarantool.DecodeBaseResponse(tt.header, encodeResponseData(t, tt.body))
			require.NoError(t, err)
			require.Equal(t, tt.header, res.Header())

			got, err := res.Decode()
			require.NoError(t, err)
			require.Equal(t, []interface{}{tt.body}, got)
		})
	}
}
