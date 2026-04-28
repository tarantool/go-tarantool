package test_helpers_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

func TestExampleMockDoer_Responses(t *testing.T) {
	mockDoer := test_helpers.NewMockDoer(t).
		AddResponseRaw([]any{"some data"}).
		AddResponseError(fmt.Errorf("some error")).
		AddResponseRaw("some typed data").
		AddResponseError(fmt.Errorf("some error"))

	data, err := mockDoer.Do(tarantool.NewPingRequest()).Get()
	require.NoError(t, err)
	assert.Equal(t, []any{"some data"}, data)

	data, err = mockDoer.Do(tarantool.NewSelectRequest("foo")).Get()
	require.EqualError(t, err, "some error")
	assert.Nil(t, data)

	var stringData string
	err = mockDoer.Do(tarantool.NewInsertRequest("space")).GetTyped(&stringData)
	require.NoError(t, err)
	assert.Equal(t, "some typed data", stringData)

	err = mockDoer.Do(tarantool.NewPrepareRequest("expr")).GetTyped(&stringData)
	require.EqualError(t, err, "some error")
	assert.Nil(t, data)
}

func TestExampleMockDoer_AddResponseWithRequests(t *testing.T) {
	mockDoer := test_helpers.NewMockDoer(t)

	header := tarantool.Header{RequestId: 123}
	dataStr := "v3"

	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)
	require.NoError(t, enc.Encode(dataStr))

	mockDoer.AddResponse(header, bytes.NewBuffer(buf.Bytes()))

	resp, err := mockDoer.Do(test_helpers.NewMockRequestNamed("bar")).GetResponse()
	require.NoError(t, err)
	assert.Equal(t, header, resp.Header())

	var stringData string
	err = resp.DecodeTyped(&stringData)
	require.NoError(t, err)
	assert.Equal(t, dataStr, stringData)

	require.Len(t, mockDoer.Requests(), 1)
	assert.Equal(t, "bar", mockDoer.Requests()[0].(*test_helpers.MockRequestNamed).Name)
}

func ExampleMockDoer_noResponses() {
	// This example demonstrates that MockDoer calls Fatalf on the test
	// when Do() is called more times than responses were configured.
	mockDoer := test_helpers.NewMockDoer(testingAdapter{}).
		AddResponseRaw([]any{"first"})

	data, err := mockDoer.Do(tarantool.NewPingRequest()).Get()
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("first response:", data)

	mockDoer.Do(tarantool.NewPingRequest())

	// Output:
	// first response: [first]
	// MockDoer.Do: no responses configured, 1 Do() calls were made
}

type testingAdapter struct {
	test_helpers.T
}

func (testingAdapter) Helper() {}
func (testingAdapter) Fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stdout, format, args...)
}
