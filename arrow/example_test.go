// Run Tarantool Enterprise Edition instance before example execution:
//
// Terminal 1:
// $ cd arrow
// $ TEST_TNT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool testdata/config-memcs.lua
//
// Terminal 2:
// $ go test -v example_test.go
package arrow_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/arrow"
)

var arrowBinData, _ = hex.DecodeString("ffffffff70000000040000009effffff0400010004000000" +
	"b6ffffff0c00000004000000000000000100000004000000daffffff140000000202" +
	"000004000000f0ffffff4000000001000000610000000600080004000c0010000400" +
	"080009000c000c000c0000000400000008000a000c00040006000800ffffffff8800" +
	"0000040000008affffff0400030010000000080000000000000000000000acffffff" +
	"01000000000000003400000008000000000000000200000000000000000000000000" +
	"00000000000000000000000000000800000000000000000000000100000001000000" +
	"0000000000000000000000000a00140004000c0010000c0014000400060008000c00" +
	"00000000000000000000")

func Example() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	arr, err := arrow.MakeArrow(arrowBinData)
	if err != nil {
		log.Fatalf("Failed prepare Arrow data: %s", err)
	}

	req := arrow.NewInsertRequest("testArrow", arr)

	resp, err := client.Do(req).Get()
	if err != nil {
		log.Fatalf("Failed insert Arrow: %s", err)
	}
	if len(resp) > 0 {
		log.Fatalf("Unexpected response")
	} else {
		fmt.Printf("Batch arrow inserted")
	}
}
