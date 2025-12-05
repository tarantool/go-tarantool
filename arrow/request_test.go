package arrow_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/arrow"
)

const validSpace uint32 = 1 // Any valid value != default.

func TestInsertRequestType(t *testing.T) {
	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{})
	require.Equal(t, iproto.IPROTO_INSERT_ARROW, request.Type())
}

func TestInsertRequestAsync(t *testing.T) {
	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{})
	require.Equal(t, false, request.Async())
}

func TestInsertRequestCtx_default(t *testing.T) {
	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{})
	require.Equal(t, nil, request.Ctx())
}

func TestInsertRequestCtx_setter(t *testing.T) {
	ctx := context.Background()
	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{}).Context(ctx)
	require.Equal(t, ctx, request.Ctx())
}

func TestResponseDecode(t *testing.T) {
	header := tarantool.Header{}
	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	require.NoError(t, enc.EncodeMapLen(1))
	require.NoError(t, enc.EncodeUint8(uint8(iproto.IPROTO_DATA)))
	require.NoError(t, enc.Encode([]interface{}{'v', '2'}))

	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{})
	resp, err := request.Response(header, bytes.NewBuffer(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, header, resp.Header())

	decodedInterface, err := resp.Decode()
	require.NoError(t, err)
	require.Equal(t, []interface{}{'v', '2'}, decodedInterface)
}

func TestResponseDecodeTyped(t *testing.T) {
	header := tarantool.Header{}
	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	require.NoError(t, enc.EncodeMapLen(1))
	require.NoError(t, enc.EncodeUint8(uint8(iproto.IPROTO_DATA)))
	require.NoError(t, enc.EncodeBytes([]byte{'v', '2'}))

	request := arrow.NewInsertRequest(validSpace, arrow.Arrow{})
	resp, err := request.Response(header, bytes.NewBuffer(buf.Bytes()))
	require.NoError(t, err)
	require.Equal(t, header, resp.Header())

	var decoded []byte
	err = resp.DecodeTyped(&decoded)
	require.NoError(t, err)
	require.Equal(t, []byte{'v', '2'}, decoded)
}

type stubSchemeResolver struct {
	space interface{}
}

func (r stubSchemeResolver) ResolveSpace(s interface{}) (uint32, error) {
	if id, ok := r.space.(uint32); ok {
		return id, nil
	}
	if _, ok := r.space.(string); ok {
		return 0, nil
	}
	return 0, fmt.Errorf("stub error message: %v", r.space)
}

func (stubSchemeResolver) ResolveIndex(i interface{}, spaceNo uint32) (uint32, error) {
	return 0, nil
}

func (r stubSchemeResolver) NamesUseSupported() bool {
	_, ok := r.space.(string)
	return ok
}

func TestInsertRequestDefaultValues(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	resolver := stubSchemeResolver{validSpace}
	req := arrow.NewInsertRequest(resolver.space, arrow.Arrow{})
	err := req.Body(&resolver, enc)
	require.NoError(t, err)

	require.Equal(t, []byte{0x82, 0x10, 0x1, 0x36, 0xc7, 0x0, 0x8}, buf.Bytes())
}

func TestInsertRequestSpaceByName(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	resolver := stubSchemeResolver{"valid"}
	req := arrow.NewInsertRequest(resolver.space, arrow.Arrow{})
	err := req.Body(&resolver, enc)
	require.NoError(t, err)

	require.Equal(t,
		[]byte{0x82, 0x5e, 0xa5, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x36, 0xc7, 0x0, 0x8},
		buf.Bytes())
}

func TestInsertRequestSetters(t *testing.T) {
	buf := bytes.NewBuffer([]byte{})
	enc := msgpack.NewEncoder(buf)

	arr, err := arrow.MakeArrow([]byte{'a', 'b', 'c'})
	require.NoError(t, err)

	resolver := stubSchemeResolver{validSpace}
	req := arrow.NewInsertRequest(resolver.space, arr)
	err = req.Body(&resolver, enc)
	require.NoError(t, err)

	require.Equal(t, []byte{0x82, 0x10, 0x1, 0x36, 0xc7, 0x3, 0x8, 'a', 'b', 'c'}, buf.Bytes())
}
