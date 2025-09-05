package test_helpers

import (
	"bytes"
	"io"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// MockResponse is a mock response used for testing purposes.
type MockResponse struct {
	// header contains response header
	header tarantool.Header
	// data contains data inside a response.
	data []byte
}

// NewMockResponse creates a new MockResponse with an empty header and the given data.
// body should be passed as a structure to be encoded.
// The encoded body is served as response data and will be decoded once the
// response is decoded.
func NewMockResponse(t *testing.T, body interface{}) *MockResponse {
	t.Helper()

	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	err := enc.Encode(body)
	if err != nil {
		t.Errorf("unexpected error while encoding: %s", err)
	}

	return &MockResponse{data: buf.Bytes()}
}

// CreateMockResponse creates a MockResponse from the header and a data,
// packed inside an io.Reader.
func CreateMockResponse(header tarantool.Header, body io.Reader) (*MockResponse, error) {
	if body == nil {
		return &MockResponse{header: header, data: nil}, nil
	}
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &MockResponse{header: header, data: data}, nil
}

// Header returns a header for the MockResponse.
func (resp *MockResponse) Header() tarantool.Header {
	return resp.header
}

// Decode returns the result of decoding the response data as slice.
func (resp *MockResponse) Decode() ([]interface{}, error) {
	if resp.data == nil {
		return nil, nil
	}
	dec := msgpack.NewDecoder(bytes.NewBuffer(resp.data))
	return dec.DecodeSlice()
}

// DecodeTyped returns the result of decoding the response data.
func (resp *MockResponse) DecodeTyped(res interface{}) error {
	if resp.data == nil {
		return nil
	}
	dec := msgpack.NewDecoder(bytes.NewBuffer(resp.data))
	return dec.Decode(res)
}
