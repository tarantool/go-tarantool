<a href="http://tarantool.org">
	<img src="https://avatars2.githubusercontent.com/u/2344919?v=2&s=250" align="right">
</a>

[![Go Reference][godoc-badge]][godoc-url]
[![Actions Status][actions-badge]][actions-url]
[![Code Coverage][coverage-badge]][coverage-url]
[![Telegram][telegram-badge]][telegram-url]
[![GitHub Discussions][discussions-badge]][discussions-url]
[![Stack Overflow][stackoverflow-badge]][stackoverflow-url]

# Client in Go for Tarantool

The package `go-tarantool` contains everything you need to connect to
[Tarantool 1.10+][tarantool-site].

The advantage of integrating Go with Tarantool, which is an application server
plus a DBMS, is that Go programmers can handle databases and perform on-the-fly
recompilations of embedded Lua routines, just as in C, with responses that are
faster than other packages according to public benchmarks.

## Table of contents

* [Installation](#installation)
  * [Build tags](#build-tags)
* [Documentation](#documentation)
  * [API reference](#api-reference)
  * [Walking\-through example](#walking-through-example)
  * [Example with encrypting traffic](#example-with-encrypting-traffic)
* [Migration guide](#migration-guide)
* [Contributing](#contributing)
* [Alternative connectors](#alternative-connectors)

## Installation

We assume that you have Tarantool version 1.10+ and a modern Linux or BSD
operating system.

You need a current version of `go`, version 1.20 or later (use `go version` to
check the version number). Do not use `gccgo-go`.

**Note:** If your `go` version is older than 1.20 or if `go` is not installed,
download and run the latest tarball from [golang.org][golang-dl].

The package `go-tarantool` is located in [tarantool/go-tarantool][go-tarantool]
repository. To download and install, say:

```
$ go get github.com/tarantool/go-tarantool/v2
```

This should put the source and binary files in subdirectories of
`/usr/local/go`, so that you can access them by adding
`github.com/tarantool/go-tarantool` to the `import {...}` section at the start
of any Go program.

### Build tags

We define multiple [build tags](https://pkg.go.dev/go/build#hdr-Build_Constraints).

This allows us to introduce new features without losing backward compatibility.

1. To run fuzz tests with decimals, you can use the build tag:
   ```
   go_tarantool_decimal_fuzzing
   ```
   **Note:** It crashes old Tarantool versions.

## Documentation

Read the [Tarantool documentation][tarantool-doc-data-model-url]
to find descriptions of terms such as "connect", "space", "index", and the
requests to create and manipulate database objects or Lua functions.

In general, connector methods can be divided into two main parts:

* `Connect()` function and functions related to connecting, and
* Data manipulation functions and Lua invocations such as `Insert()` or `Call()`.

The supported requests have parameters and results equivalent to requests in
the [Tarantool CRUD operations][tarantool-doc-box-space-url].
There are also Typed and Async versions of each data-manipulation function.

### API Reference

Learn API documentation and examples at [pkg.go.dev][godoc-url].

### Walking-through example

We can now have a closer look at the example and make some observations
about what it does.

```go
package tarantool

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	_ "github.com/tarantool/go-tarantool/v2/datetime"
	_ "github.com/tarantool/go-tarantool/v2/decimal"
	_ "github.com/tarantool/go-tarantool/v2/uuid"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dialer := tarantool.NetDialer{
		Address: "127.0.0.1:3301",
		User: 	 "guest",
	}
	opts := tarantool.Opts{
		Timeout: time.Second,
	}

	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		fmt.Println("Connection refused:", err)
		return
	}

	data, err := conn.Do(
		tarantool.NewInsertRequest(999).Tuple([]interface{}{99999, "BB"})).Get()
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Data:", data)
	}
}
```

**Observation 1:** The line "`github.com/tarantool/go-tarantool/v2`" in the
`import(...)` section brings in all Tarantool-related functions and structures.

**Observation 2:** Unused import lines are required to initialize encoders and
decoders for external `msgpack` types.

**Observation 3:** The line starting with "`ctx, cancel :=`" creates a context
object for `Connect()`. The `Connect()` call will return an error when a
timeout expires before the connection is established.

**Observation 4:** The line starting with "`dialer :=`" creates dialer for
`Connect()`. This structure contains fields required to establish a connection.

**Observation 5:** The line starting with "`opts :=`" sets up the options for
`Connect()`. In this example, the structure contains only a single value, the
timeout. The structure may also contain other settings, see more in
[documentation][godoc-opts-url] for the "`Opts`" structure.

**Observation 6:** The line containing "`tarantool.Connect`" is essential for
starting a session. There are three parameters:

* a context,
* the dialer that was set up earlier,
* the option structure that was set up earlier.

There will be only one attempt to connect. If multiple attempts needed,
"`tarantool.Connect`" could be placed inside the loop with some timeout
between each try. Example could be found in the [example_test](./example_test.go),
name - `ExampleConnect_reconnects`.

**Observation 7:** The `err` structure will be `nil` if there is no error,
otherwise it will have a description which can be retrieved with `err.Error()`.

**Observation 8:** The `Insert` request, like almost all requests, is preceded
by the method `Do` of object `conn` which is the object that was returned
by `Connect()`.

### Example with encrypting traffic

For SSL-enabled connections, use `OpenSSLDialer` from the
[go-tlsdialer](https://github.com/tarantool/go-tlsdialer) package.

Here is small example with importing the `go-tlsdialer` library and using the
`OpenSSLDialer`:

```go
package tarantool

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v2"
	_ "github.com/tarantool/go-tarantool/v2/datetime"
	_ "github.com/tarantool/go-tarantool/v2/decimal"
	_ "github.com/tarantool/go-tarantool/v2/uuid"
	"github.com/tarantool/go-tlsdialer"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dialer := tlsdialer.OpenSSLDialer{
		Address:     "127.0.0.1:3013", 
		User:        "test", 
		Password:    "test", 
		SslKeyFile:  "testdata/localhost.key",
		SslCertFile: "testdata/localhost.crt",
		SslCaFile:   "testdata/ca.crt",
	}
	opts := tarantool.Opts{
		Timeout: time.Second,
	}

	conn, err := tarantool.Connect(ctx, dialer, opts)
	if err != nil {
		fmt.Println("Connection refused:", err)
		return
	}

	data, err := conn.Do(
		tarantool.NewInsertRequest(999).Tuple([]interface{}{99999, "BB"})).Get()
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Data:", data)
	}
}
```

Note that [traffic encryption](https://www.tarantool.io/en/doc/latest/enterprise/security/#encrypting-traffic)
is only available in Tarantool Enterprise Edition 2.10 or newer.

## Migration guide

You can review the changes between major versions in the
[migration guide](./MIGRATION.md).

## Contributing

See [the contributing guide](CONTRIBUTING.md) for detailed instructions on how
to get started with our project.

## Alternative connectors

There are two other connectors available from the open source community:

* [viciious/go-tarantool](https://github.com/viciious/go-tarantool),
* [FZambia/tarantool](https://github.com/FZambia/tarantool).

See feature comparison in the [documentation][tarantool-doc-connectors-comparison].

[tarantool-site]: https://tarantool.io/
[godoc-badge]: https://pkg.go.dev/badge/github.com/tarantool/go-tarantool.svg
[godoc-url]: https://pkg.go.dev/github.com/tarantool/go-tarantool
[actions-badge]: https://github.com/tarantool/go-tarantool/actions/workflows/testing.yml/badge.svg
[actions-url]: https://github.com/tarantool/go-tarantool/actions/workflows/testing.yml
[coverage-badge]: https://coveralls.io/repos/github/tarantool/go-tarantool/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/tarantool/go-tarantool?branch=master
[telegram-badge]: https://img.shields.io/badge/Telegram-join%20chat-blue.svg
[telegram-url]: http://telegram.me/tarantool
[discussions-badge]: https://img.shields.io/github/discussions/tarantool/tarantool
[discussions-url]: https://github.com/tarantool/tarantool/discussions
[stackoverflow-badge]: https://img.shields.io/badge/stackoverflow-tarantool-orange.svg
[stackoverflow-url]: https://stackoverflow.com/questions/tagged/tarantool
[golang-dl]: https://go.dev/dl/
[go-tarantool]: https://github.com/tarantool/go-tarantool
[tarantool-doc-data-model-url]: https://www.tarantool.io/en/doc/latest/book/box/data_model/
[tarantool-doc-box-space-url]: https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/
[godoc-opts-url]: https://pkg.go.dev/github.com/tarantool/go-tarantool#Opts
[tarantool-doc-connectors-comparison]: https://www.tarantool.io/en/doc/latest/book/connectors/#go-feature-comparison
