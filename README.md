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
  * [Migration to v2](#migration-to-v2)
    * [datetime package](#datetime-package)
    * [decimal package](#decimal-package)
    * [multi package](#multi-package)
    * [pool package](#pool-package)
    * [msgpack.v5](#msgpackv5)
    * [Call = Call17](#call--call17)
    * [IPROTO constants](#iproto-constants)
    * [Request interface](#request-interface)
* [Contributing](#contributing)
* [Alternative connectors](#alternative-connectors)

## Installation

We assume that you have Tarantool version 1.10+ and a modern Linux or BSD
operating system.

You need a current version of `go`, version 1.13 or later (use `go version` to
check the version number). Do not use `gccgo-go`.

**Note:** If your `go` version is older than 1.13 or if `go` is not installed,
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

1. To disable SSL support and linking with OpenSSL, you can use the tag:
   ```
   go_tarantool_ssl_disable
   ```
2. To run fuzz tests with decimals, you can use the build tag:
   ```
   go_tarantool_decimal_fuzzing
   ```
   **Note:** It crashes old Tarantool versions and requires Go 1.18+.

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
	"fmt"
	"github.com/tarantool/go-tarantool/v2"
)

func main() {
	opts := tarantool.Opts{User: "guest"}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)
	if err != nil {
		fmt.Println("Connection refused:", err)
	}
	resp, err := conn.Do(tarantool.NewInsertRequest(999).
		Tuple([]interface{}{99999, "BB"}),
	).Get()
	if err != nil {
		fmt.Println("Error", err)
		fmt.Println("Code", resp.Code)
	}
}
```

**Observation 1:** The line "`github.com/tarantool/go-tarantool/v2`" in the
`import(...)` section brings in all Tarantool-related functions and structures.

**Observation 2:** The line starting with "`Opts :=`" sets up the options for
`Connect()`. In this example, the structure contains only a single value, the
username. The structure may also contain other settings, see more in
[documentation][godoc-opts-url] for the "`Opts`" structure.

**Observation 3:** The line containing "`tarantool.Connect`" is essential for
starting a session. There are two parameters:

* a string with `host:port` format, and
* the option structure that was set up earlier.

**Observation 4:** The `err` structure will be `nil` if there is no error,
otherwise it will have a description which can be retrieved with `err.Error()`.

**Observation 5:** The `Insert` request, like almost all requests, is preceded
by the method `Do` of object `conn` which is the object that was returned
by `Connect()`.

### Migration to v2

The article describes migration from go-tarantool to go-tarantool/v2.

#### datetime package

Now you need to use objects of the Datetime type instead of pointers to it. A
new constructor `MakeDatetime` returns an object. `NewDatetime` has been
removed.

#### decimal package

Now you need to use objects of the Decimal type instead of pointers to it. A
new constructor `MakeDecimal` returns an object. `NewDecimal` has been removed.

#### multi package

The subpackage has been deleted. You could use `pool` instead.

#### pool package

The logic has not changed, but there are a few renames:

* The `connection_pool` subpackage has been renamed to `pool`.
* The type `PoolOpts` has been renamed to `Opts`.

#### msgpack.v5

Most function names and argument types in `msgpack.v5` and `msgpack.v2`
have not changed (in our code, we noticed changes in `EncodeInt`, `EncodeUint`
and `RegisterExt`). But there are a lot of changes in a logic of encoding and
decoding. On the plus side the migration seems easy, but on the minus side you
need to be very careful.

First of all, `EncodeInt8`, `EncodeInt16`, `EncodeInt32`, `EncodeInt64`
and `EncodeUint*` analogues at `msgpack.v5` encode numbers as is without loss of
type. In `msgpack.v2` the type of a number is reduced to a value.

Secondly, a base decoding function does not convert numbers to `int64` or
`uint64`. It converts numbers to an exact type defined by MessagePack. The
change makes manual type conversions much more difficult and can lead to
runtime errors with an old code. We do not recommend to use type conversions
and give preference to `*Typed` functions (besides, it's faster).

There are also changes in the logic that can lead to errors in the old code,
[as example](https://github.com/vmihailenco/msgpack/issues/327). Although in
`msgpack.v5` some functions for the logic tuning were added (see
[UseLooseInterfaceDecoding](https://pkg.go.dev/github.com/vmihailenco/msgpack/v5#Decoder.UseLooseInterfaceDecoding), [UseCompactInts](https://pkg.go.dev/github.com/vmihailenco/msgpack/v5#Encoder.UseCompactInts) etc), it is still impossible
to achieve full compliance of behavior between `msgpack.v5` and `msgpack.v2`. So
we don't go this way. We use standard settings if it possible.

#### Call = Call17

Call requests uses `IPROTO_CALL` instead of `IPROTO_CALL_16`.

So now `Call` = `Call17` and `NewCallRequest` = `NewCall17Request`. A result
of the requests is an array instead of array of arrays.

#### IPROTO constants

IPROTO constants have been moved to a separate package [go-iproto](https://github.com/tarantool/go-iproto).

#### Request interface

* The method `Code() uint32` replaced by the `Type() iproto.Type`.

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
