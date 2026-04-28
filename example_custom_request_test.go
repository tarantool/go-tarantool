package tarantool_test

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v3"
)

// customPingRequest demonstrates how to implement a custom Request type.
// It sends a ping request and uses DecodeBaseResponse to handle the response.
type customPingRequest struct {
	ctx context.Context
}

func (r *customPingRequest) Type() iproto.Type {
	return iproto.IPROTO_PING
}

func (r *customPingRequest) Body(_ tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	return enc.EncodeMapLen(0)
}

func (r *customPingRequest) Ctx() context.Context {
	return r.ctx
}

func (r *customPingRequest) Async() bool {
	return false
}

// Response creates a response for the custom request.
// It reads all data from body using DecodeBaseResponse helper.
func (r *customPingRequest) Response(
	header tarantool.Header,
	body io.Reader,
) (tarantool.Response, error) {
	return tarantool.DecodeBaseResponse(header, body)
}

func (r *customPingRequest) Context(ctx context.Context) *customPingRequest {
	r.ctx = ctx
	return r
}

// ExampleRequest_Response demonstrates how to implement the Response method
// for a custom Request type.
func ExampleRequest_Response() {
	conn := exampleConnect(dialer, opts)
	defer func() { _ = conn.Close() }()

	future := conn.Do(&customPingRequest{})

	data, err := future.Get()
	fmt.Println("Data", data)
	fmt.Println("Error", err)

	future.Release()
	// Output:
	// Data []
	// Error <nil>
}

// manualRequest demonstrates how to implement a custom Request type
// with manual body decoding (without DecodeBaseResponse helper).
type manualRequest struct {
	ctx     context.Context
	spaceNo uint32
	key     []any
}

func (r *manualRequest) Type() iproto.Type {
	return iproto.IPROTO_SELECT
}

func (r *manualRequest) Body(_ tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	if err := enc.EncodeMapLen(6); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_ITERATOR)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.ITER_EQ)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_OFFSET)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(0)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_LIMIT)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(0xFFFFFFFF)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_SPACE_ID)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(r.spaceNo)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_INDEX_ID)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(0)); err != nil {
		return err
	}
	if err := enc.EncodeUint(uint64(iproto.IPROTO_KEY)); err != nil {
		return err
	}
	return enc.Encode(r.key)
}

func (r *manualRequest) Ctx() context.Context {
	return r.ctx
}

func (r *manualRequest) Async() bool {
	return false
}

// Response creates a response by reading all data from body.
// The body contains msgpack-encoded map with IPROTO_DATA key.
func (r *manualRequest) Response(
	header tarantool.Header,
	body io.Reader,
) (tarantool.Response, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &manualResponse{header: header, body: data}, nil
}

// manualResponse implements tarantool.Response interface.
type manualResponse struct {
	header tarantool.Header
	body   []byte
}

func (r *manualResponse) Header() tarantool.Header { return r.header }
func (r *manualResponse) Release()                 { r.body = nil }

func (r *manualResponse) Decode() ([]any, error) {
	if len(r.body) == 0 {
		return nil, nil
	}

	dec := msgpack.NewDecoder(bytes.NewReader(r.body))

	mapLen, err := dec.DecodeMapLen()
	if err != nil {
		return nil, err
	}

	var result []any

	for range mapLen {
		key, err := dec.DecodeInt()
		if err != nil {
			return nil, err
		}

		if iproto.Key(key) == iproto.IPROTO_DATA {
			if result, err = dec.DecodeSlice(); err != nil {
				return nil, err
			}
		} else {
			if err := dec.Skip(); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

func (r *manualResponse) DecodeTyped(res any) error {
	if len(r.body) == 0 {
		return nil
	}

	dec := msgpack.NewDecoder(bytes.NewReader(r.body))

	mapLen, err := dec.DecodeMapLen()
	if err != nil {
		return err
	}

	for range mapLen {
		key, err := dec.DecodeInt()
		if err != nil {
			return err
		}

		if iproto.Key(key) == iproto.IPROTO_DATA {
			if err := dec.Decode(res); err != nil {
				return err
			}
		} else {
			if err := dec.Skip(); err != nil {
				return err
			}
		}
	}

	return nil
}

// ExampleRequest_Response_manual demonstrates how to implement the Response method
// for a custom Request type with manual body decoding.
func ExampleRequest_Response_manual() {
	conn := exampleConnect(dialer, opts)
	defer func() { _ = conn.Close() }()

	// Insert test data.
	_, err := conn.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	// Delete test data on exit.
	defer func() {
		_, _ = conn.Do(tarantool.NewDeleteRequest(spaceNo).
			Index(indexNo).
			Key([]any{uint(1111)}),
		).Get()
	}()

	// Execute custom select request.
	req := &manualRequest{spaceNo: spaceNo, key: []any{uint(1111)}}
	future := conn.Do(req)

	data, err := future.Get()
	fmt.Println("Data", data)
	fmt.Println("Error", err)

	future.Release()
	// Output:
	// Data [[1111 hello world]]
	// Error <nil>
}

// ExampleRequest_Response_manualDecodeTyped demonstrates how to implement DecodeTyped
// with a custom Request implementation.
func ExampleRequest_Response_manualDecodeTyped() {
	conn := exampleConnect(dialer, opts)
	defer func() { _ = conn.Close() }()

	// Insert test data.
	_, err := conn.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]any{uint(1111), "hello", "world"}),
	).Get()
	if err != nil {
		fmt.Println("Error", err)
		return
	}

	// Delete test data on exit.
	defer func() {
		_, _ = conn.Do(tarantool.NewDeleteRequest(spaceNo).
			Index(indexNo).
			Key([]any{uint(1111)}),
		).Get()
	}()

	// Execute custom select request.
	req := &manualRequest{spaceNo: spaceNo, key: []any{uint(1111)}}
	future := conn.Do(req)

	var tuples []Tuple
	err = future.GetTyped(&tuples)
	if len(tuples) > 0 {
		fmt.Println("Id", tuples[0].Id)
		fmt.Println("Msg", tuples[0].Msg)
		fmt.Println("Name", tuples[0].Name)
	}
	fmt.Println("Error", err)

	future.Release()
	// Output:
	// Id 1111
	// Msg hello
	// Name world
	// Error <nil>
}
