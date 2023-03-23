package tarantool_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/ice-blockchain/go-tarantool"
)

func assertResponseIteratorValue(t testing.TB, it ResponseIterator,
	code uint32, resp *Response) {
	t.Helper()

	if it.Err() != nil {
		t.Errorf("An unexpected iteration error: %q", it.Err().Error())
	}

	if it.Value() == nil {
		t.Errorf("An unexpected nil value")
	} else if it.Value().Code != code {
		t.Errorf("An unexpected response code %d, expected %d", it.Value().Code, code)
	}

	if it.Value() != resp {
		t.Errorf("An unexpected response %v, expected %v", it.Value(), resp)
	}
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
	fut := NewFuture()

	it := fut.GetIterator()
	if it.Next() {
		t.Errorf("An unexpected next value.")
	} else {
		assertResponseIteratorFinished(t, it)
	}
}

func TestFutureGetIteratorNoResponse(t *testing.T) {
	push := &Response{}
	fut := NewFuture()
	fut.AppendPush(push)

	if it := fut.GetIterator(); it.Next() {
		assertResponseIteratorValue(t, it, PushCode, push)
		if it.Next() == true {
			t.Errorf("An unexpected next value.")
		}
		assertResponseIteratorFinished(t, it)
	} else {
		t.Errorf("A push message expected.")
	}
}

func TestFutureGetIteratorNoResponseTimeout(t *testing.T) {
	push := &Response{}
	fut := NewFuture()
	fut.AppendPush(push)

	if it := fut.GetIterator().WithTimeout(1 * time.Nanosecond); it.Next() {
		assertResponseIteratorValue(t, it, PushCode, push)
		if it.Next() == true {
			t.Errorf("An unexpected next value.")
		}
		assertResponseIteratorFinished(t, it)
	} else {
		t.Errorf("A push message expected.")
	}
}

func TestFutureGetIteratorResponseOnTimeout(t *testing.T) {
	push := &Response{}
	resp := &Response{}
	fut := NewFuture()
	fut.AppendPush(push)

	var done sync.WaitGroup
	var wait sync.WaitGroup
	wait.Add(1)
	done.Add(1)

	go func() {
		defer done.Done()

		var it ResponseIterator
		var cnt = 0
		for it = fut.GetIterator().WithTimeout(5 * time.Second); it.Next(); {
			code := PushCode
			r := push
			if cnt == 1 {
				code = OkCode
				r = resp
			}
			assertResponseIteratorValue(t, it, code, r)
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
	fut.SetResponse(resp)
	done.Wait()
}

func TestFutureGetIteratorFirstResponse(t *testing.T) {
	resp1 := &Response{}
	resp2 := &Response{}
	fut := NewFuture()
	fut.SetResponse(resp1)
	fut.SetResponse(resp2)

	if it := fut.GetIterator(); it.Next() {
		assertResponseIteratorValue(t, it, OkCode, resp1)
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

	fut := NewFuture()
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
	responses := []*Response{
		{},
		{},
		{Code: OkCode},
	}
	fut := NewFuture()
	for i, resp := range responses {
		if i == len(responses)-1 {
			fut.SetResponse(resp)
		} else {
			fut.AppendPush(resp)
		}
	}

	var its = []ResponseIterator{
		fut.GetIterator(),
		fut.GetIterator().WithTimeout(5 * time.Second),
	}
	for _, it := range its {
		var cnt = 0
		for it.Next() {
			code := PushCode
			if cnt == len(responses)-1 {
				code = OkCode
			}
			assertResponseIteratorValue(t, it, code, responses[cnt])
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
	responses := []*Response{
		{},
		{},
	}
	err := errors.New(errMsg)
	fut := NewFuture()
	for _, resp := range responses {
		fut.AppendPush(resp)
	}
	fut.SetError(err)

	var its = []ResponseIterator{
		fut.GetIterator(),
		fut.GetIterator().WithTimeout(5 * time.Second),
	}
	for _, it := range its {
		var cnt = 0
		for it.Next() {
			code := PushCode
			assertResponseIteratorValue(t, it, code, responses[cnt])
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
	resp := &Response{}

	for i := 0; i < 1000; i++ {
		fut := NewFuture()
		for j := 0; j < 9; j++ {
			go func(opt int) {
				if opt%3 == 0 {
					respAppend := &Response{}
					fut.AppendPush(respAppend)
				} else if opt%3 == 1 {
					fut.SetError(err)
				} else {
					fut.SetResponse(resp)
				}
			}(j)
		}
	}
	// It may be false-positive, but very rarely - it's ok for such very
	// simple race conditions tests.
}
