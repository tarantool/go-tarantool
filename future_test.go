package tarantool_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
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
	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '2'}))
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
	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '2'}))
	assert.NoError(t, err)

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

	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '2'}))
	assert.NoError(t, err)

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	respConv, ok := resp.(*futureMockResponse)
	assert.True(t, ok)
	assert.Equal(t, mockResp, respConv)

	data, err := resp.Decode()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{uint8('v'), uint8('2')}, data)
}

func BenchmarkFuture_Get(b *testing.B) {
	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '3'}))
	if err != nil {
		b.Errorf("SetResponse error: %s", err)
	}
	b.ResetTimer()

	for b.Loop() {
		data, err := fut.Get()
		if err != nil {
			b.Errorf("Get error: %s", err)
		}
		if !(len(data) == 2 && data[0] == uint8('v') && data[1] == uint8('3')) {
			b.Error("Wrong output: ", data)
		}
	}
}

func BenchmarkFuture_GetTyped(b *testing.B) {
	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '3'}))
	if err != nil {
		b.Errorf("SetResponse error: %s", err)
	}
	var data []byte
	resp, err := fut.GetResponse()
	if err != nil {
		b.Fatalf("GetResponse error: %s", err)
	}
	futResp := resp.(*futureMockResponse)
	b.ResetTimer()

	for i := 1; i <= b.N; i++ {
		err = fut.GetTyped(&data)
		if err != nil {
			b.Errorf("Get error: %s", err)
		}
		if futResp.decodeTypedCnt != i {
			b.Fatalf("Wrong behavior")
		}
	}
}

func BenchmarkFuture_WaitChan(b *testing.B) {
	fut, err := NewFutureWithResponse(&futureMockRequest{},
		Header{}, bytes.NewReader([]byte{'v', '3'}))
	if err != nil {
		b.Errorf("SetResponse error: %s", err)
	}
	b.ResetTimer()

	for b.Loop() {
		ch := fut.WaitChan()
		if _, ok := <-ch; ok {
			b.Fatalf("chan not closed")
		}
	}
}

type futureMock struct {
	value int
}

var _ = Future(&futureMock{})

func (f *futureMock) Get() ([]interface{}, error) {
	return []interface{}{f.value}, nil
}

func (f *futureMock) GetTyped(val interface{}) error {
	if value, ok := val.(*int); ok {
		*value = f.value
		return nil
	}
	return fmt.Errorf("wrong interface type")
}

func (f *futureMock) GetResponse() (Response, error) {
	return createFutureMockResponse(Header{}, bytes.NewReader([]byte(strconv.Itoa(f.value))))
}

func (*futureMock) WaitChan() <-chan struct{} {
	return nil
}

func TestFuture(t *testing.T) {
	fut := &futureMock{value: 5}

	values, err := fut.Get()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{5}, values)

	var typed int
	err = fut.GetTyped(&typed)
	assert.NoError(t, err)
	assert.Equal(t, interface{}(5), typed)

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	futResp := resp.(*futureMockResponse)
	assert.Equal(t, []byte{'5'}, futResp.data)
	assert.Nil(t, fut.WaitChan())
}
