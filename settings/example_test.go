package settings_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/settings"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

var exampleDialer = tarantool.NetDialer{
	Address:  "127.0.0.1",
	User:     "test",
	Password: "test",
}

var exampleOpts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

func example_connect(dialer tarantool.Dialer, opts tarantool.Opts) *tarantool.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
	return conn
}

func Example_sqlFullColumnNames() {
	var resp tarantool.Response
	var err error
	var isLess bool

	conn := example_connect(exampleDialer, exampleOpts)
	defer conn.Close()

	// Tarantool supports session settings since version 2.3.1
	isLess, err = test_helpers.IsTarantoolVersionLess(2, 3, 1)
	if err != nil || isLess {
		return
	}

	// Create a space.
	req := tarantool.NewExecuteRequest("CREATE TABLE example(id INT PRIMARY KEY, x INT);")
	_, err = conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error in create table: %v\n", err)
		return
	}

	// Insert some tuple into space.
	req = tarantool.NewExecuteRequest("INSERT INTO example VALUES (1, 1);")
	_, err = conn.Do(req).Get()
	if err != nil {
		fmt.Printf("error on insert: %v\n", err)
		return
	}

	// Enable showing full column names in SQL responses.
	_, err = conn.Do(settings.NewSQLFullColumnNamesSetRequest(true)).Get()
	if err != nil {
		fmt.Printf("error on setting setup: %v\n", err)
		return
	}

	// Get some data with SQL query.
	req = tarantool.NewExecuteRequest("SELECT x FROM example WHERE id = 1;")
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		fmt.Printf("error on select: %v\n", err)
		return
	}

	exResp, ok := resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}

	metaData, err := exResp.MetaData()
	if err != nil {
		fmt.Printf("error on getting MetaData: %v\n", err)
		return
	}
	// Show response metadata.
	fmt.Printf("full column name: %v\n", metaData[0].FieldName)

	// Disable showing full column names in SQL responses.
	_, err = conn.Do(settings.NewSQLFullColumnNamesSetRequest(false)).Get()
	if err != nil {
		fmt.Printf("error on setting setup: %v\n", err)
		return
	}

	// Get some data with SQL query.
	resp, err = conn.Do(req).GetResponse()
	if err != nil {
		fmt.Printf("error on select: %v\n", err)
		return
	}
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	if !ok {
		fmt.Printf("wrong response type")
		return
	}
	metaData, err = exResp.MetaData()
	if err != nil {
		fmt.Printf("error on getting MetaData: %v\n", err)
		return
	}
	// Show response metadata.
	fmt.Printf("short column name: %v\n", metaData[0].FieldName)
}
