//go:build !go_tarantool_call_17
// +build !go_tarantool_call_17

package multi

import (
	"testing"

	"github.com/ice-blockchain/go-tarantool"
)

func TestCall(t *testing.T) {
	var resp *tarantool.Response
	var err error

	multiConn, err := Connect([]string{server1, server2}, connOpts)
	if err != nil {
		t.Fatalf("Failed to connect: %s", err.Error())
	}
	if multiConn == nil {
		t.Fatalf("conn is nil after Connect")
	}
	defer multiConn.Close()

	// Call16
	resp, err = multiConn.Call("simple_concat", []interface{}{"t"})
	if err != nil {
		t.Fatalf("Failed to use Call: %s", err.Error())
	}
	if resp.Data[0].([]interface{})[0].(string) != "tt" {
		t.Fatalf("result is not {{1}} : %v", resp.Data)
	}
}
