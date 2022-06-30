// Run a Tarantool instance before example execution:
// Terminal 1:
// $ cd datetime
// $ TEST_TNT_LISTEN=3013 TEST_TNT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
//
// Terminal 2:
// $ cd datetime
// $ go test -v example_test.go
package datetime_test

import (
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool"
	. "github.com/tarantool/go-tarantool/datetime"
)

// Example demonstrates how to use tuples with datetime. To enable support of
// datetime import tarantool/datetime package.
func Example() {
	opts := tarantool.Opts{
		User: "test",
		Pass: "test",
	}
	conn, err := tarantool.Connect("127.0.0.1:3013", opts)
	if err != nil {
		fmt.Printf("error in connect is %v", err)
		return
	}

	var datetime = "2013-10-28T17:51:56.000000009Z"
	tm, err := time.Parse(time.RFC3339, datetime)
	if err != nil {
		fmt.Printf("error in time.Parse() is %v", err)
		return
	}
	dt, err := NewDatetime(tm)
	if err != nil {
		fmt.Printf("Unable to create Datetime from %s: %s", tm, err)
		return
	}

	space := "testDatetime_1"
	index := "primary"

	// Replace a tuple with datetime.
	resp, err := conn.Replace(space, []interface{}{dt})
	if err != nil {
		fmt.Printf("error in replace is %v", err)
		return
	}
	respDt := resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple replace")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())

	// Select a tuple with datetime.
	var offset uint32 = 0
	var limit uint32 = 1
	resp, err = conn.Select(space, index, offset, limit, tarantool.IterEq, []interface{}{dt})
	if err != nil {
		fmt.Printf("error in select is %v", err)
		return
	}
	respDt = resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple select")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())

	// Delete a tuple with datetime.
	resp, err = conn.Delete(space, index, []interface{}{dt})
	if err != nil {
		fmt.Printf("error in delete is %v", err)
		return
	}
	respDt = resp.Data[0].([]interface{})[0].(Datetime)
	fmt.Println("Datetime tuple delete")
	fmt.Printf("Code: %d\n", resp.Code)
	fmt.Printf("Data: %v\n", respDt.ToTime())
}
