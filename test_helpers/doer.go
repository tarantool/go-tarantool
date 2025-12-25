package test_helpers

import (
	"bytes"
	"testing"

	"github.com/tarantool/go-tarantool/v3"
)

type doerResponse struct {
	resp *MockResponse
	err  error
}

// MockDoer is an implementation of the Doer interface
// used for testing purposes.
type MockDoer struct {
	// Requests is a slice of received requests.
	// It could be used to compare incoming requests with expected.
	Requests  []tarantool.Request
	responses []doerResponse
	t         *testing.T
}

// NewMockDoer creates a MockDoer by given responses.
// Each response could be one of two types: MockResponse or error.
func NewMockDoer(t *testing.T, responses ...interface{}) MockDoer {
	t.Helper()

	mockDoer := MockDoer{t: t}
	for _, response := range responses {
		doerResp := doerResponse{}

		switch resp := response.(type) {
		case *MockResponse:
			doerResp.resp = resp
		case error:
			doerResp.err = resp
		default:
			t.Fatalf("unsupported type: %T", response)
		}

		mockDoer.responses = append(mockDoer.responses, doerResp)
	}
	return mockDoer
}

// Do returns a future with the current response or an error.
// It saves the current request into MockDoer.Requests.
func (doer *MockDoer) Do(req tarantool.Request) tarantool.Future {
	doer.Requests = append(doer.Requests, req)

	mockReq := NewMockRequest()
	var fut tarantool.Future

	if len(doer.responses) == 0 {
		doer.t.Fatalf("list of responses is empty")
	}
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
