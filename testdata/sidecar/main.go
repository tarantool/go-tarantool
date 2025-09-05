package main

import (
	"context"
	"os"
	"strconv"

	"github.com/tarantool/go-tarantool/v3"
)

func main() {
	fd, err := strconv.Atoi(os.Getenv("SOCKET_FD"))
	if err != nil {
		panic(err)
	}
	dialer := tarantool.FdDialer{
		Fd: uintptr(fd),
	}
	conn, err := tarantool.Connect(context.Background(), dialer, tarantool.Opts{})
	if err != nil {
		panic(err)
	}
	if _, err := conn.Do(tarantool.NewPingRequest()).Get(); err != nil {
		panic(err)
	}
	// Insert new tuple.
	if _, err := conn.Do(tarantool.NewInsertRequest("test").
		Tuple([]interface{}{239})).Get(); err != nil {
		panic(err)
	}
	// Delete inserted tuple.
	if _, err := conn.Do(tarantool.NewDeleteRequest("test").
		Index("primary").
		Key([]interface{}{239})).Get(); err != nil {
		panic(err)
	}
}
