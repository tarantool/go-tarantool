package tarantool_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

func encodeResponseData(t *testing.T, data interface{}) io.Reader {
	t.Helper()

	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	require.NoError(t, enc.EncodeMapLen(1))
	require.NoError(t, enc.EncodeUint8(uint8(iproto.IPROTO_DATA)))
	require.NoError(t, enc.Encode([]interface{}{data}))

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

func TestBaseResponseRelease(t *testing.T) {
	header := tarantool.Header{RequestId: 123}
	buf := []byte{'v', '3'}

	resp, err := tarantool.DecodeBaseResponse(header, encodeResponseData(t, buf))
	require.NoError(t, err)

	data, err := resp.Decode()
	require.NoError(t, err)
	require.Equal(t, resp.Header(), header)
	require.Equal(t, []interface{}{buf}, data)

	resp.Release()

	data, err = resp.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}(nil), data)
}

func TestSelectResponseRelease(t *testing.T) {
	header := tarantool.Header{RequestId: 123}
	buf := []byte{'v', '3'}
	req := tarantool.NewSelectRequest(nil)

	resp, err := req.Response(header, encodeResponseData(t, buf))
	require.NoError(t, err)

	data, err := resp.Decode()
	require.NoError(t, err)
	require.Equal(t, resp.Header(), header)
	require.Equal(t, []interface{}{buf}, data)

	resp.Release()

	selResp, ok := resp.(*tarantool.SelectResponse)
	require.True(t, ok)
	require.Equal(t, tarantool.SelectResponse{}, *selResp)
}

func TestSelectResponseReleaseReuse(t *testing.T) {
	req := tarantool.NewSelectRequest(nil)

	header1 := tarantool.Header{RequestId: 100}
	buf1 := []byte{'d', 'a', 't', 'a', '1'}
	resp1, err := req.Response(header1, encodeResponseData(t, buf1))
	require.NoError(t, err)

	data1, err := resp1.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf1}, data1)

	resp1.Release()

	header2 := tarantool.Header{RequestId: 200}
	buf2 := []byte{'d', 'a', 't', 'a', '2'}
	resp2, err := req.Response(header2, encodeResponseData(t, buf2))
	require.NoError(t, err)

	data2, err := resp2.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf2}, data2)
	require.Equal(t, header2, resp2.Header())
	require.Equal(t, []interface{}{buf2}, data2)
}

func TestSelectResponseReleaseMultipleObjects(t *testing.T) {
	req := tarantool.NewSelectRequest(nil)

	header1 := tarantool.Header{RequestId: 1}
	buf1 := []byte{'o', 'n', 'e'}
	resp1, err := req.Response(header1, encodeResponseData(t, buf1))
	require.NoError(t, err)

	header2 := tarantool.Header{RequestId: 2}
	buf2 := []byte{'t', 'w', 'o'}
	resp2, err := req.Response(header2, encodeResponseData(t, buf2))
	require.NoError(t, err)

	data1, err := resp1.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf1}, data1)

	data2, err := resp2.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf2}, data2)

	resp1.Release()
	resp2.Release()

	header3 := tarantool.Header{RequestId: 3}
	buf3 := []byte{'t', 'h', 'r', 'e', 'e'}
	resp3, err := req.Response(header3, encodeResponseData(t, buf3))
	require.NoError(t, err)

	data3, err := resp3.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf3}, data3)
	require.Equal(t, header3, resp3.Header())
}

func TestExecuteResponseRelease(t *testing.T) {
	req := tarantool.NewExecuteRequest(insertQuery)

	header := tarantool.Header{RequestId: 123}
	buf := []byte{'v', '3'}
	resp, err := req.Response(header, encodeResponseData(t, buf))

	require.NoError(t, err)
	require.NotNil(t, resp)

	resp.Release()

	execResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok)
	require.Equal(t, tarantool.ExecuteResponse{}, *execResp)
}

func TestExecuteResponseReleaseReuse(t *testing.T) {
	req := tarantool.NewExecuteRequest(insertQuery)

	header1 := tarantool.Header{RequestId: 100}
	buf1 := []byte{'d', 'a', 't', 'a', '1'}
	resp1, err := req.Response(header1, encodeResponseData(t, buf1))
	require.NoError(t, err)

	data1, err := resp1.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf1}, data1)

	resp1.Release()

	header2 := tarantool.Header{RequestId: 200}
	buf2 := []byte{'d', 'a', 't', 'a', '2'}
	resp2, err := req.Response(header2, encodeResponseData(t, buf2))
	require.NoError(t, err)

	data2, err := resp2.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf2}, data2)
	require.Equal(t, header2, resp2.Header())
	require.Equal(t, []interface{}{buf2}, data2)
}

func TestPrepareResponseRelease(t *testing.T) {
	req := tarantool.NewPrepareRequest(insertQuery)

	header := tarantool.Header{RequestId: 123}
	buf := []byte{'v', '3'}
	resp, err := req.Response(header, encodeResponseData(t, buf))

	require.NoError(t, err)
	require.NotNil(t, resp)

	resp.Release()

	prepResp, ok := resp.(*tarantool.PrepareResponse)
	require.True(t, ok)
	require.Equal(t, tarantool.PrepareResponse{}, *prepResp)
}

func TestPrepareResponseReleaseReuse(t *testing.T) {
	req := tarantool.NewPrepareRequest(insertQuery)

	header1 := tarantool.Header{RequestId: 100}
	buf1 := []byte{'d', 'a', 't', 'a', '1'}
	resp1, err := req.Response(header1, encodeResponseData(t, buf1))
	require.NoError(t, err)

	data1, err := resp1.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf1}, data1)

	resp1.Release()

	header2 := tarantool.Header{RequestId: 200}
	buf2 := []byte{'d', 'a', 't', 'a', '2'}
	resp2, err := req.Response(header2, encodeResponseData(t, buf2))
	require.NoError(t, err)

	data2, err := resp2.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{buf2}, data2)
	require.Equal(t, header2, resp2.Header())
	require.Equal(t, []interface{}{buf2}, data2)
}
