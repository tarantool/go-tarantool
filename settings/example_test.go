package settings_test

import (
	"fmt"

	"github.com/ice-blockchain/go-tarantool"
	"github.com/ice-blockchain/go-tarantool/settings"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

func example_connect(opts tarantool.Opts) *tarantool.Connection {
	conn, err := tarantool.Connect(server, opts)
	if err != nil {
		panic("Connection is not established: " + err.Error())
	}
	return conn
}

func Example_sqlFullColumnNames() {
	var resp *tarantool.Response
	var err error
	var isLess bool

	conn := example_connect(opts)
	defer conn.Close()

	// Tarantool supports session settings since version 2.3.1
	isLess, err = test_helpers.IsTarantoolVersionLess(2, 3, 1)
	if err != nil || isLess {
		return
	}

	// Create a space.
	_, err = conn.Execute("CREATE TABLE example(id INT PRIMARY KEY, x INT);", []interface{}{})
	if err != nil {
		fmt.Printf("error in create table: %v\n", err)
		return
	}

	// Insert some tuple into space.
	_, err = conn.Execute("INSERT INTO example VALUES (1, 1);", []interface{}{})
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
	resp, err = conn.Execute("SELECT x FROM example WHERE id = 1;", []interface{}{})
	if err != nil {
		fmt.Printf("error on select: %v\n", err)
		return
	}
	// Show response metadata.
	fmt.Printf("full column name: %v\n", resp.MetaData[0].FieldName)

	// Disable showing full column names in SQL responses.
	_, err = conn.Do(settings.NewSQLFullColumnNamesSetRequest(false)).Get()
	if err != nil {
		fmt.Printf("error on setting setup: %v\n", err)
		return
	}

	// Get some data with SQL query.
	resp, err = conn.Execute("SELECT x FROM example WHERE id = 1;", []interface{}{})
	if err != nil {
		fmt.Printf("error on select: %v\n", err)
		return
	}
	// Show response metadata.
	fmt.Printf("short column name: %v\n", resp.MetaData[0].FieldName)
}
