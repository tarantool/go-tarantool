package crud_test

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2/crud"
)

func TestOperation_EncodeMsgpack(t *testing.T) {
	testCases := []struct {
		name string
		op   crud.Operation
		ref  []interface{}
	}{
		{
			"Add",
			crud.Operation{
				Operator: crud.Add,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"+", 1, 2},
		},
		{
			"Sub",
			crud.Operation{
				Operator: crud.Sub,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"-", 1, 2},
		},
		{
			"And",
			crud.Operation{
				Operator: crud.And,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"&", 1, 2},
		},
		{
			"Or",
			crud.Operation{
				Operator: crud.Or,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"|", 1, 2},
		},
		{
			"Xor",
			crud.Operation{
				Operator: crud.Xor,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"^", 1, 2},
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
			[]interface{}{":", 1, 2, 3, "a"},
		},
		{
			"Insert",
			crud.Operation{
				Operator: crud.Insert,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"!", 1, 2},
		},
		{
			"Delete",
			crud.Operation{
				Operator: crud.Delete,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"#", 1, 2},
		},
		{
			"Assign",
			crud.Operation{
				Operator: crud.Assign,
				Field:    1,
				Value:    2,
			},
			[]interface{}{"=", 1, 2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			var refBuf bytes.Buffer
			encRef := msgpack.NewEncoder(&refBuf)
			if err := encRef.Encode(test.ref); err != nil {
				t.Errorf("error while encoding: %v", err.Error())
			}

			var buf bytes.Buffer
			enc := msgpack.NewEncoder(&buf)

			if err := enc.Encode(test.op); err != nil {
				t.Errorf("error while encoding: %v", err.Error())
			}
			if !bytes.Equal(refBuf.Bytes(), buf.Bytes()) {
				t.Errorf("encode response is wrong:\n expected %v\n got: %v",
					refBuf, buf.Bytes())
			}
		})
	}
}
