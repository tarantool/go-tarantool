//go:build go_tarantool_call_17
// +build go_tarantool_call_17

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

	// Call17
	resp, err = conn.Call17("simple_incr", []interface{}{1})
	if err != nil {
		t.Errorf("Failed to use Call")
	}
	if resp.Data[0].(uint64) != 2 {
		t.Errorf("result is not {{1}} : %v", resp.Data)
	}
}
