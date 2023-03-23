package multi

import (
	"fmt"
	"time"

	"github.com/ice-blockchain/go-tarantool"
)

func ExampleConnect() {
	multiConn, err := Connect([]string{"127.0.0.1:3031", "127.0.0.1:3032"}, tarantool.Opts{
		Timeout: 500 * time.Millisecond,
		User:    "test",
		Pass:    "test",
	})
	if err != nil {
		fmt.Printf("error in connect is %v", err)
	}
	fmt.Println(multiConn)
}

func ExampleConnectWithOpts() {
	multiConn, err := ConnectWithOpts([]string{"127.0.0.1:3301", "127.0.0.1:3302"}, tarantool.Opts{
		Timeout: 500 * time.Millisecond,
		User:    "test",
		Pass:    "test",
	}, OptsMulti{
		// Check for connection timeout every 1 second.
		CheckTimeout: 1 * time.Second,
		// Lua function name for getting address list.
		NodesGetFunctionName: "get_cluster_nodes",
		// Ask server for updated address list every 3 seconds.
		ClusterDiscoveryTime: 3 * time.Second,
	})
	if err != nil {
		fmt.Printf("error in connect is %v", err)
	}
	fmt.Println(multiConn)
}
