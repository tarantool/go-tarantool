package test_helpers_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

func TestExampleMockDoer(t *testing.T) {
	mockDoer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, []interface{}{"some data"}),
		fmt.Errorf("some error"),
		test_helpers.NewMockResponse(t, "some typed data"),
		fmt.Errorf("some error"),
	)

	data, err := mockDoer.Do(tarantool.NewPingRequest()).Get()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{"some data"}, data)

	data, err = mockDoer.Do(tarantool.NewSelectRequest("foo")).Get()
	assert.EqualError(t, err, "some error")
	assert.Nil(t, data)

	var stringData string
	err = mockDoer.Do(tarantool.NewInsertRequest("space")).GetTyped(&stringData)
	assert.NoError(t, err)
	assert.Equal(t, "some typed data", stringData)

	err = mockDoer.Do(tarantool.NewPrepareRequest("expr")).GetTyped(&stringData)
	assert.EqualError(t, err, "some error")
	assert.Nil(t, data)
}
