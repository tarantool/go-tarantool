package test_helpers

import (
	"bytes"
	"io"

	"github.com/tarantool/go-tarantool/v3"
)

// MockDoer is an interface for a mock doer used for custom testing.
// It allows building a sequence of responses and inspecting requests
// that were sent via Do calls.
type MockDoer interface {
	// AddResponse adds a response with a custom header and a body
	// packed inside an io.Reader and returns the MockDoer for chaining.
	AddResponse(header tarantool.Header, body io.Reader) MockDoer
	// AddResponseRaw adds a response with an empty header and the given
	// data, which is encoded with msgpack, and returns the MockDoer for
	// chaining.
	AddResponseRaw(data interface{}) MockDoer
	// AddResponseError adds an error response and returns the MockDoer for
	// chaining.
	AddResponseError(err error) MockDoer
	// Requests returns a slice of requests received via Do calls.
	Requests() []tarantool.Request
	// Do processes the request and returns a Future with the next
	// configured response or error. It calls Fatalf on the test if
	// no responses are configured.
	Do(req tarantool.Request) tarantool.Future
}

type doerResponse struct {
	resp *MockResponse
	err  error
}

// mockDoer is an implementation of the MockDoer interface
// used for testing purposes.
type mockDoer struct {
	// requests is a slice of received requests.
	// It could be used to compare incoming requests with expected.
	requests  []tarantool.Request
	responses []doerResponse
	t         T
}

var _ MockDoer = (*mockDoer)(nil)

func (m *mockDoer) Requests() []tarantool.Request {
	return m.requests
}

func (m *mockDoer) AddResponse(header tarantool.Header, data io.Reader) MockDoer {
	resp, err := CreateMockResponse(header, data)
	if err != nil {
		m.t.Fatalf("failed to create mock response: %v", err)
	}

	m.responses = append(m.responses, doerResponse{resp: resp})
	return m
}

func (m *mockDoer) AddResponseRaw(data interface{}) MockDoer {
	m.responses = append(m.responses, doerResponse{resp: NewMockResponse(m.t, data)})
	return m
}

func (m *mockDoer) AddResponseError(err error) MockDoer {
	m.responses = append(m.responses, doerResponse{err: err})
	return m
}

// NewMockDoer creates a MockDoer for testing. Use AddResponse, AddResponseRaw,
// and AddResponseError methods to configure the sequence of responses.
func NewMockDoer(t T) MockDoer {
	t.Helper()

	return &mockDoer{t: t}
}

// Do returns a future with the current response or an error.
// It saves the current request into MockDoer.Requests.
func (doer *mockDoer) Do(req tarantool.Request) tarantool.Future {
	mockReq := NewMockRequest()
	var fut tarantool.Future

	if len(doer.responses) == 0 {
		doer.t.Fatalf("MockDoer.Do: no responses configured, %d Do() calls were made",
			len(doer.requests))

		return fut
	}

	doer.requests = append(doer.requests, req)
	response := doer.responses[0]

	if response.err != nil {
		fut = tarantool.NewFutureWithErr(mockReq, response.err)
	} else {
		fut, _ = tarantool.NewFutureWithResponse(mockReq,
			response.resp.header, bytes.NewBuffer(response.resp.data))
	}
	doer.responses = doer.responses[1:]

	return fut
}
