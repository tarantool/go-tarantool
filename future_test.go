package tarantool_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tarantool/go-iproto"
	. "github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
	"github.com/vmihailenco/msgpack/v5"
)

type futureMockRequest struct {
}

func (req *futureMockRequest) Type() iproto.Type {
	return iproto.Type(0)
}

func (req *futureMockRequest) Async() bool {
	return false
}

func (req *futureMockRequest) Body(_ SchemaResolver, _ *msgpack.Encoder) error {
	return nil
}

func (req *futureMockRequest) Conn() *Connection {
	return &Connection{}
}

func (req *futureMockRequest) Ctx() context.Context {
	return nil
}

func (req *futureMockRequest) Response(header Header,
	body io.Reader) (Response, error) {
	resp, err := createFutureMockResponse(header, body)
	return resp, err
}

type futureMockResponse struct {
	header Header
	data   []byte

	decodeCnt      int
	decodeTypedCnt int
}

func (resp *futureMockResponse) Header() Header {
	return resp.header
}

func (resp *futureMockResponse) Decode() ([]interface{}, error) {
	resp.decodeCnt++

	dataInt := make([]interface{}, len(resp.data))
	for i := range resp.data {
		dataInt[i] = resp.data[i]
	}
	return dataInt, nil
}

func (resp *futureMockResponse) DecodeTyped(res interface{}) error {
	resp.decodeTypedCnt++
	return nil
}

func createFutureMockResponse(header Header, body io.Reader) (Response, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, err
	}
	return &futureMockResponse{header: header, data: data}, nil
}

func assertResponseIteratorValue(t testing.TB, it ResponseIterator,
	isPush bool, resp Response) {
	t.Helper()

	if it.Err() != nil {
		t.Errorf("An unexpected iteration error: %q", it.Err().Error())
	}

	if it.Value() == nil {
		t.Errorf("An unexpected nil value")
	} else if it.IsPush() != isPush {
		if isPush {
			t.Errorf("An unexpected response type, expected to be push")
		} else {
			t.Errorf("An unexpected response type, expected not to be push")
		}
	}

	assert.Equalf(t, it.Value(), resp, "An unexpected response %v, expected %v", it.Value(), resp)
}

func assertResponseIteratorFinished(t testing.TB, it ResponseIterator) {
	t.Helper()

	if it.Err() != nil {
		t.Errorf("An unexpected iteration error: %q", it.Err().Error())
	}
	if it.Value() != nil {
		t.Errorf("An unexpected value %v", it.Value())
	}
}

func TestFutureGetIteratorNoItems(t *testing.T) {
	fut := NewFuture(test_helpers.NewMockRequest())

	it := fut.GetIterator()
	if it.Next() {
		t.Errorf("An unexpected next value.")
	} else {
		assertResponseIteratorFinished(t, it)
	}
}

func TestFutureGetIteratorNoResponse(t *testing.T) {
	pushHeader := Header{}
	push := &PushResponse{}
	fut := NewFuture(test_helpers.NewMockRequest())
	fut.AppendPush(pushHeader, nil)

	if it := fut.GetIterator(); it.Next() {
		assertResponseIteratorValue(t, it, true, push)
		if it.Next() == true {
			t.Errorf("An unexpected next value.")
		}
		assertResponseIteratorFinished(t, it)
	} else {
		t.Errorf("A push message expected.")
	}
}

func TestFutureGetIteratorNoResponseTimeout(t *testing.T) {
	pushHeader := Header{}
	push := &PushResponse{}
	fut := NewFuture(test_helpers.NewMockRequest())
	fut.AppendPush(pushHeader, nil)

	if it := fut.GetIterator().WithTimeout(1 * time.Nanosecond); it.Next() {
		assertResponseIteratorValue(t, it, true, push)
		if it.Next() == true {
			t.Errorf("An unexpected next value.")
		}
		assertResponseIteratorFinished(t, it)
	} else {
		t.Errorf("A push message expected.")
	}
}

func TestFutureGetIteratorResponseOnTimeout(t *testing.T) {
	pushHeader := Header{}
	respHeader := Header{}
	push := &PushResponse{}
	resp := &test_helpers.MockResponse{}
	fut := NewFuture(test_helpers.NewMockRequest())
	fut.AppendPush(pushHeader, nil)

	var done sync.WaitGroup
	var wait sync.WaitGroup
	wait.Add(1)
	done.Add(1)

	go func() {
		defer done.Done()

		var it ResponseIterator
		var cnt = 0
		for it = fut.GetIterator().WithTimeout(5 * time.Second); it.Next(); {
			var r Response
			isPush := true
			r = push
			if cnt == 1 {
				isPush = false
				r = resp
			}
			assertResponseIteratorValue(t, it, isPush, r)
			cnt += 1
			if cnt == 1 {
				wait.Done()
			}
		}
		assertResponseIteratorFinished(t, it)

		if cnt != 2 {
			t.Errorf("An unexpected count of responses %d != %d", cnt, 2)
		}
	}()

	wait.Wait()

	fut.SetResponse(respHeader, nil)
	done.Wait()
}

func TestFutureGetIteratorFirstResponse(t *testing.T) {
	resp := &test_helpers.MockResponse{}
	fut := NewFuture(test_helpers.NewMockRequest())
	fut.SetResponse(Header{}, nil)
	fut.SetResponse(Header{}, nil)

	if it := fut.GetIterator(); it.Next() {
		assertResponseIteratorValue(t, it, false, resp)
		if it.Next() == true {
			t.Errorf("An unexpected next value.")
		}
		assertResponseIteratorFinished(t, it)
	} else {
		t.Errorf("A response expected.")
	}
}

func TestFutureGetIteratorFirstError(t *testing.T) {
	const errMsg1 = "error1"
	const errMsg2 = "error2"

	fut := NewFuture(test_helpers.NewMockRequest())
	fut.SetError(errors.New(errMsg1))
	fut.SetError(errors.New(errMsg2))

	it := fut.GetIterator()
	if it.Next() {
		t.Errorf("An unexpected value.")
	} else if it.Err() == nil {
		t.Errorf("An error expected.")
	} else if it.Err().Error() != errMsg1 {
		t.Errorf("An unexpected error %q, expected %q", it.Err().Error(), errMsg1)
	}
}

func TestFutureGetIteratorResponse(t *testing.T) {
	responses := []Response{
		&PushResponse{},
		&PushResponse{},
		&test_helpers.MockResponse{},
	}
	header := Header{}
	fut := NewFuture(test_helpers.NewMockRequest())
	for i := range responses {
		if i == len(responses)-1 {
			fut.SetResponse(header, nil)
		} else {
			fut.AppendPush(header, nil)
		}
	}

	var its = []ResponseIterator{
		fut.GetIterator(),
		fut.GetIterator().WithTimeout(5 * time.Second),
	}
	for _, it := range its {
		var cnt = 0
		for it.Next() {
			isPush := true
			if cnt == len(responses)-1 {
				isPush = false
			}
			assertResponseIteratorValue(t, it, isPush, responses[cnt])
			cnt += 1
		}
		assertResponseIteratorFinished(t, it)

		if cnt != len(responses) {
			t.Errorf("An unexpected count of responses %d != %d", cnt, len(responses))
		}
	}
}

func TestFutureGetIteratorError(t *testing.T) {
	const errMsg = "error message"
	responses := []*PushResponse{
		{},
		{},
	}
	err := errors.New(errMsg)
	fut := NewFuture(test_helpers.NewMockRequest())
	for range responses {
		fut.AppendPush(Header{}, nil)
	}
	fut.SetError(err)

	var its = []ResponseIterator{
		fut.GetIterator(),
		fut.GetIterator().WithTimeout(5 * time.Second),
	}
	for _, it := range its {
		var cnt = 0
		for it.Next() {
			assertResponseIteratorValue(t, it, true, responses[cnt])
			cnt += 1
		}
		if err = it.Err(); err != nil {
			if err.Error() != errMsg {
				t.Errorf("An unexpected error %q, expected %q", err.Error(), errMsg)
			}
		} else {
			t.Errorf("An error expected.")
		}

		if cnt != len(responses) {
			t.Errorf("An unexpected count of responses %d != %d", cnt, len(responses))
		}
	}
}

func TestFutureSetStateRaceCondition(t *testing.T) {
	err := errors.New("any error")

	for i := 0; i < 1000; i++ {
		fut := NewFuture(test_helpers.NewMockRequest())
		for j := 0; j < 9; j++ {
			go func(opt int) {
				if opt%3 == 0 {
					fut.AppendPush(Header{}, nil)
				} else if opt%3 == 1 {
					fut.SetError(err)
				} else {
					fut.SetResponse(Header{}, nil)
				}
			}(j)
		}
	}
	// It may be false-positive, but very rarely - it's ok for such very
	// simple race conditions tests.
}

func TestFutureGetIteratorIsPush(t *testing.T) {
	fut := NewFuture(test_helpers.NewMockRequest())
	fut.AppendPush(Header{}, nil)
	fut.SetResponse(Header{}, nil)
	it := fut.GetIterator()

	it.Next()
	assert.True(t, it.IsPush())
	it.Next()
	assert.False(t, it.IsPush())
}

func TestFuture_Get(t *testing.T) {
	fut := NewFuture(&futureMockRequest{})
	fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'}))

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	mockResp, ok := resp.(*futureMockResponse)
	assert.True(t, ok)

	data, err := fut.Get()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{uint8('v'), uint8('2')}, data)
	assert.Equal(t, 1, mockResp.decodeCnt)
	assert.Equal(t, 0, mockResp.decodeTypedCnt)
}

func TestFuture_GetTyped(t *testing.T) {
	fut := NewFuture(&futureMockRequest{})
	fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'}))

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	mockResp, ok := resp.(*futureMockResponse)
	assert.True(t, ok)

	var data []byte

	err = fut.GetTyped(&data)
	assert.NoError(t, err)
	assert.Equal(t, 0, mockResp.decodeCnt)
	assert.Equal(t, 1, mockResp.decodeTypedCnt)
}

func TestFuture_GetResponse(t *testing.T) {
	mockResp, err := createFutureMockResponse(Header{},
		bytes.NewReader([]byte{'v', '2'}))
	assert.NoError(t, err)

	fut := NewFuture(&futureMockRequest{})
	fut.SetResponse(Header{}, bytes.NewReader([]byte{'v', '2'}))

	resp, err := fut.GetResponse()
	assert.NoError(t, err)
	respConv, ok := resp.(*futureMockResponse)
	assert.True(t, ok)
	assert.Equal(t, mockResp, respConv)

	data, err := resp.Decode()
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{uint8('v'), uint8('2')}, data)
}
