package tarantool_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

func TestOperations_EncodeMsgpack(t *testing.T) {
	ops := tarantool.NewOperations().
		Add(1, 2).
		Subtract(1, 2).
		BitwiseAnd(1, 2).
		BitwiseOr(1, 2).
		BitwiseXor(1, 2).
		Splice(1, 2, 3, "a").
		Insert(1, 2).
		Delete(1, 2).
		Assign(1, 2)
	refOps := []interface{}{
		[]interface{}{"+", 1, 2},
		[]interface{}{"-", 1, 2},
		[]interface{}{"&", 1, 2},
		[]interface{}{"|", 1, 2},
		[]interface{}{"^", 1, 2},
		[]interface{}{":", 1, 2, 3, "a"},
		[]interface{}{"!", 1, 2},
		[]interface{}{"#", 1, 2},
		[]interface{}{"=", 1, 2},
	}

	var refBuf bytes.Buffer
	encRef := msgpack.NewEncoder(&refBuf)
	require.NoError(t, encRef.Encode(refOps), "error while encoding")

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	require.NoError(t, enc.Encode(ops), "error while encoding")
	assert.Equal(t, refBuf.Bytes(), buf.Bytes(), "encode response is wrong")
}
