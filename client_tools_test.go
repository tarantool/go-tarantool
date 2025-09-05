package tarantool_test

import (
	"bytes"
	"testing"

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
	if err := encRef.Encode(refOps); err != nil {
		t.Errorf("error while encoding: %v", err.Error())
	}

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)

	if err := enc.Encode(ops); err != nil {
		t.Errorf("error while encoding: %v", err.Error())
	}
	if !bytes.Equal(refBuf.Bytes(), buf.Bytes()) {
		t.Errorf("encode response is wrong:\n expected %v\n got: %v",
			refBuf, buf.Bytes())
	}
}
