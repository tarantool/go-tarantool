// Run Tarantool Common Edition before example execution:
//
// Terminal 1:
// $ cd box
// $ TEST_TNT_LISTEN=127.0.0.1:3013 tarantool testdata/config.lua
//
// Terminal 2:
// $ go test -v example_test.go
package box_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
)

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

	// You can use Info Request type.

	fut := client.Do(box.NewInfoRequest())

	resp := &box.InfoResponse{}

	err = fut.GetTyped(resp)
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	// Or use simple Box implementation.

	b := box.New(client)

	info, err := b.Info()
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	if info.UUID != resp.Info.UUID {
		log.Fatalf("Box info uuids are not equal")
	}

	fmt.Printf("Box info uuids are equal")
	fmt.Printf("Current box info: %+v\n", resp.Info)
}
