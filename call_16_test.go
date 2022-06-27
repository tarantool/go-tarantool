//go:build !go_tarantool_call_17
// +build !go_tarantool_call_17

package tarantool_test

import (
	"testing"

	. "github.com/tarantool/go-tarantool"
)

func TestConnection_Call(t *testing.T) {
	var resp *Response
	var err error

	conn := connect(t, server, opts)
	defer conn.Close()

	// Call16
	resp, err = conn.Call("simple_incr", []interface{}{1})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if resp.Data[0].([]interface{})[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}
}

func TestCallRequest(t *testing.T) {
	var resp *Response
	var err error

	conn := connect(t, server, opts)
	defer conn.Close()

	req := NewCallRequest("simple_incr").Args([]interface{}{1})
	resp, err = conn.Do(req).Get()
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if resp.Data[0].([]interface{})[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}
}

func TestCallRequestCode(t *testing.T) {
	req := NewCallRequest("simple_incrt")
	code := req.Code()
	expected := Call16RequestCode
	if code != int32(expected) {
		t.Errorf("CallRequest actual code %v != %v", code, expected)
	}
}
