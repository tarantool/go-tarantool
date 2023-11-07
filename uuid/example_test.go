// Run Tarantool instance before example execution:
// Terminal 1:
// $ cd uuid
// $ TEST_TNT_LISTEN=3013 TEST_TNT_WORK_DIR=$(mktemp -d -t 'tarantool.XXX') tarantool config.lua
//
// Terminal 2:
// $ cd uuid
// $ go test -v example_test.go
package uuid_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/tarantool/go-tarantool/v2"
	_ "github.com/tarantool/go-tarantool/v2/uuid"
)

var exampleOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

// Example demonstrates how to use tuples with UUID. To enable UUID support
// in msgpack with google/uuid (https://github.com/google/uuid), import
// tarantool/uuid submodule.
func Example() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, exampleOpts)
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err.Error())
	}

	spaceNo := uint32(524)

	id, uuidErr := uuid.Parse("c8f0fa1f-da29-438c-a040-393f1126ad39")
	if uuidErr != nil {
		log.Fatalf("Failed to prepare uuid: %s", uuidErr)
	}

	resp, err := client.Do(tarantool.NewReplaceRequest(spaceNo).
		Tuple([]interface{}{id}),
	).Get()

	fmt.Println("UUID tuple replace")
	fmt.Println("Error", err)
	fmt.Println("Code", resp.Code)
	fmt.Println("Data", resp.Data)
}
