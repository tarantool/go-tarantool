package crud_test

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3/crud"
)

func TestResult_DecodeMsgpack(t *testing.T) {
	sampleCrudResponse := []interface{}{
		map[string]interface{}{
			"rows": []interface{}{"1", "2", "3"},
		},
		nil,
	}
	responses := []interface{}{sampleCrudResponse, sampleCrudResponse}

	b := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(b)
	err := enc.Encode(responses)
	require.NoError(t, err)

	var results []crud.Result
	decoder := msgpack.NewDecoder(b)
	err = decoder.DecodeValue(reflect.ValueOf(&results))
	require.NoError(t, err)
	require.Equal(t, results[0].Rows, []interface{}{"1", "2", "3"})
	require.Equal(t, results[1].Rows, []interface{}{"1", "2", "3"})
}
