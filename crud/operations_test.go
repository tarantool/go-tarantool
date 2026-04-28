package crud_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3/crud"
)

func TestOperation_EncodeMsgpack(t *testing.T) {
	testCases := []struct {
		name string
		op   crud.Operation
		ref  []any
	}{
		{
			"Add",
			crud.Operation{
				Operator: crud.Add,
				Field:    1,
				Value:    2,
			},
			[]any{"+", 1, 2},
		},
		{
			"Sub",
			crud.Operation{
				Operator: crud.Sub,
				Field:    1,
				Value:    2,
			},
			[]any{"-", 1, 2},
		},
		{
			"And",
			crud.Operation{
				Operator: crud.And,
				Field:    1,
				Value:    2,
			},
			[]any{"&", 1, 2},
		},
		{
			"Or",
			crud.Operation{
				Operator: crud.Or,
				Field:    1,
				Value:    2,
			},
			[]any{"|", 1, 2},
		},
		{
			"Xor",
			crud.Operation{
				Operator: crud.Xor,
				Field:    1,
				Value:    2,
			},
			[]any{"^", 1, 2},
		},
		{
			"Splice",
			crud.Operation{
				Operator: crud.Splice,
				Field:    1,
				Pos:      2,
				Len:      3,
				Replace:  "a",
			},
			[]any{":", 1, 2, 3, "a"},
		},
		{
			"Insert",
			crud.Operation{
				Operator: crud.Insert,
				Field:    1,
				Value:    2,
			},
			[]any{"!", 1, 2},
		},
		{
			"Delete",
			crud.Operation{
				Operator: crud.Delete,
				Field:    1,
				Value:    2,
			},
			[]any{"#", 1, 2},
		},
		{
			"Assign",
			crud.Operation{
				Operator: crud.Assign,
				Field:    1,
				Value:    2,
			},
			[]any{"=", 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var refBuf bytes.Buffer
			encRef := msgpack.NewEncoder(&refBuf)
			require.NoError(t, encRef.Encode(test.ref), "error while encoding reference")

			var buf bytes.Buffer
			enc := msgpack.NewEncoder(&buf)
			require.NoError(t, enc.Encode(test.op), "error while encoding operation")
			assert.Equal(t, refBuf.Bytes(), buf.Bytes(), "encode response is wrong")
		})
	}
}
