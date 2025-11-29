package tarantool_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-iproto"
	. "github.com/tarantool/go-tarantool/v3"
	"github.com/vmihailenco/msgpack/v5"
)

type futureMockRequest struct {
}

func (req *futureMockRequest) Type() iproto.Type {
	return iproto.Type(0)
}

func (req *futureMockRequest) Async() bool {
	return false
}

func (req *futureMockRequest) Body(_ SchemaResolver, _ *msgpack.Encoder) error {
	return nil
}

func (req *futureMockRequest) Conn() *Connection {
	return &Connection{}
}

func (req *futureMockRequest) Ctx() context.Context {
	return nil
}

func (req *futureMockRequest) Response(header Header,
	body io.Reader) (Response, error) {
	resp, err := createFutureMockResponse(header, body)
	return resp, err
}

type futureMockResponse struct {
	header Header
	data   []byte

	decodeCnt      int
	decodeTypedCnt int
}

func (resp *futureMockResponse) Header() Header {
	return resp.header
}

func (resp *futureMockResponse) Decode() ([]interface{}, error) {
	resp.decodeCnt++

	dataInt := make([]interface{}, len(resp.data))
	for i := range resp.data {
		dataInt[i] = resp.data[i]
	}
	return dataInt, nil
}

func (resp *futureMockResponse) DecodeTyped(res interface{}) error {
	resp.decodeTypedCnt++
	return nil
}

func createFutureMockResponse(header Header, body io.Reader) (Response, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &futureMockResponse{header: header, data: data}, nil
}

func TestFuture_Get(t *testing.T) {
	fut := NewFuture(&futureMockRequest{})
	err := fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'}))
	assert.NoError(t, err)

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	mockResp, ok := resp.(*futureMockResponse)
	assert.True(t, ok)

	data, err := fut.Get()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{uint8('v'), uint8('2')}, data)
	assert.Equal(t, 1, mockResp.decodeCnt)
	assert.Equal(t, 0, mockResp.decodeTypedCnt)
}

func TestFuture_GetTyped(t *testing.T) {
	fut := NewFuture(&futureMockRequest{})
	assert.NoError(t,
		fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'})))

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	mockResp, ok := resp.(*futureMockResponse)
	assert.True(t, ok)

	var data []byte

	err = fut.GetTyped(&data)
	assert.NoError(t, err)
	assert.Equal(t, 0, mockResp.decodeCnt)
	assert.Equal(t, 1, mockResp.decodeTypedCnt)
}

func TestFuture_GetResponse(t *testing.T) {
	mockResp, err := createFutureMockResponse(Header{},
		bytes.NewReader([]byte{'v', '2'}))
	assert.NoError(t, err)

	fut := NewFuture(&futureMockRequest{})
	assert.NoError(t,
		fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'})))

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	respConv, ok := resp.(*futureMockResponse)
	assert.True(t, ok)
	assert.Equal(t, mockResp, respConv)

	data, err := resp.Decode()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{uint8('v'), uint8('2')}, data)
}
