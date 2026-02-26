# Migration guide

## Migration from v2.x.x to v3.x.x

* [Major changes](#major-changes-v3)

TODO

### <a id="major-changes-v3">Major changes</a>

* Required Go version is `1.24` now.
* `box.New` returns an error instead of panic
* Added `box.MustNew` wrapper for `box.New` without an error
* Removed deprecated `pool` methods, related interfaces and tests are updated.
* Removed `box.session.push()` support: Future.AppendPush() and Future.GetIterator()
  methods, ResponseIterator and TimeoutResponseIterator types.
* Removed deprecated `Connection` methods, related interfaces and tests are updated.

  *NOTE*: due to Future.GetTyped() doesn't decode SelectRequest into structure, substitute Connection.GetTyped() following the example:
  ```Go
  var singleTpl = Tuple{}
  err = conn.GetTyped(space, index, key, &singleTpl)
  ```
  At now became:
  ```Go
  var tpl []Tuple
	err = conn.Do(NewSelectRequest(space).
		Index(index).
		Limit(1).
		Key(key)
  ).GetTyped(&tpl)
  singleTpl := tpl[0]
  ```
* Future.done replaced with Future.cond (sync.Cond) + Future.finished bool.
* `Future` is an interface now.
* `ConnectionPool.Close()` returns a single error value, combining multiple errors using errors.Join()

## Migration from v1.x.x to v2.x.x

* [Major changes](#major-changes-v2)
* [Main package](#main-package)
  * [Go version](#go-version)
  * [msgpack/v5](#msgpackv5)
  * [Call = Call17](#call--call17)
  * [IPROTO constants](#iproto-constants)
  * [Request interface](#request-interface)
  * [Request changes](#request-changes)
  * [Response interface](#response-interface)
  * [Response changes](#response-changes)
  * [Future type](#future-type)
  * [Protocol types](#protocol-types)
  * [Connector interface](#connector-interface)
  * [Connect function](#connect-function)
  * [Connection schema](#connection-schema)
  * [Schema type](#schema-type)
* [datetime package](#datetime-package)
* [decimal package](#decimal-package)
* [multi package](#multi-package)
* [pool package](#pool-package)
* [crud package](#crud-package)
* [test_helpers package](#test_helpers-package)

### <a id="major-changes-v2">Major changes</a>

* The `go_tarantool_call_17` build tag is no longer needed, since by default
  the `CallRequest` is `Call17Request`.
* The `go_tarantool_msgpack_v5` build tag is no longer needed, since only the
  `msgpack/v5` library is used.
* The `go_tarantool_ssl_disable` build tag is no longer needed, since the
  connector is no longer depends on `OpenSSL` by default. You could use the
  external library [go-tlsdialer](https://github.com/tarantool/go-tlsdialer) to
  create a connection with the `ssl` transport.
* Required Go version is `1.20` now.
* The `Connect` function became more flexible. It now allows to create a
  connection with cancellation and a custom `Dialer` implementation.
* It is required to use `Request` implementation types with the `Connection.Do`
  method instead of `Connection.<Request>` methods.
* The `connection_pool` package renamed to `pool`.

The basic code for the `v1.12.2` release:
```Go
package tarantool

import (
	"fmt"

	"github.com/tarantool/go-tarantool"
	_ "github.com/tarantool/go-tarantool/v3/datetime"
	_ "github.com/tarantool/go-tarantool/v3/decimal"
	_ "github.com/tarantool/go-tarantool/v3/uuid"
)

func main() {
	opts := tarantool.Opts{User: "guest"}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)
	if err != nil {
		fmt.Println("Connection refused:", err)
		return
	}

	resp, err := conn.Insert(999, []interface{}{99999, "BB"})
	if err != nil {
		fmt.Println("Error:", err)
		fmt.Println("Code:", resp.Code)
	} else {
		fmt.Println("Data:", resp.Data)
	}
}
```

At now became:
```Go
package tarantool

import (
	"context"
	"fmt"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	_ "github.com/tarantool/go-tarantool/v3/datetime"
	_ "github.com/tarantool/go-tarantool/v3/decimal"
	_ "github.com/tarantool/go-tarantool/v3/uuid"
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

### Main package

#### Go version

Required Go version is updated from `1.13` to `1.20`.

#### msgpack/v5

At now the `msgpack/v5` library is used for the `msgpack` encoding/decondig.

Most function names and argument types in `msgpack/v5` and `msgpack.v2`
have not changed (in our code, we noticed changes in `EncodeInt`, `EncodeUint`
and `RegisterExt`). But there are a lot of changes in a logic of encoding and
decoding. On the plus side the migration seems easy, but on the minus side you
need to be very careful.

First of all, `EncodeInt8`, `EncodeInt16`, `EncodeInt32`, `EncodeInt64`
and `EncodeUint*` analogues at `msgpack/v5` encode numbers as is without loss of
type. In `msgpack.v2` the type of a number is reduced to a value.

Secondly, a base decoding function does not convert numbers to `int64` or
`uint64`. It converts numbers to an exact type defined by MessagePack. The
change makes manual type conversions much more difficult and can lead to
runtime errors with an old code. We do not recommend to use type conversions
and give preference to `*Typed` functions (besides, it's faster).

There are also changes in the logic that can lead to errors in the old code,
[as example](https://github.com/vmihailenco/msgpack/issues/327). Although in
`msgpack/v5` some functions for the logic tuning were added (see
[UseLooseInterfaceDecoding](https://pkg.go.dev/github.com/vmihailenco/msgpack/v5#Decoder.UseLooseInterfaceDecoding), [UseCompactInts](https://pkg.go.dev/github.com/vmihailenco/msgpack/v5#Encoder.UseCompactInts) etc), it is still impossible
to achieve full compliance of behavior between `msgpack/v5` and `msgpack.v2`.
So we don't go this way. We use standard settings if it possible.

#### Call = Call17

Call requests uses `IPROTO_CALL` instead of `IPROTO_CALL_16`.

So now `Call` = `Call17` and `NewCallRequest` = `NewCall17Request`. A result
of the requests is an array instead of array of arrays.

#### IPROTO constants

* IPROTO constants have been moved to a separate package [go-iproto](https://github.com/tarantool/go-iproto).
* `PushCode` constant is removed. To check whether the current response is
  a push response, use `IsPush()` method of the response iterator instead.
* `ErrorNo` constant is added to indicate that no error has occurred while
  getting the response. It should be used instead of the removed `OkCode`.
  See `ExampleErrorNo`.

#### Request interface

* The method `Code() uint32` replaced by the `Type() iproto.Type`.
* `Response` method added to the `Request` interface.

#### Request changes

* Requests `Update`, `UpdateAsync`, `UpdateTyped`, `Upsert`, `UpsertAsync` no
longer accept `ops` argument (operations) as an `interface{}`. `*Operations`
needs to be passed instead.
* `Op` struct for update operations made private.
* Removed `OpSplice` struct.
* `Operations.Splice` method now accepts 5 arguments instead of 3.
* `UpdateRequest` and `UpsertRequest` structs no longer accept `interface{}`
for an `ops` field. `*Operations` needs to be used instead.

#### Response interface

* `Response` is now an interface.
* Response header stored in a new `Header` struct. It could be accessed via
  `Header()` method.

#### Response changes

* `ResponseIterator` interface now has `IsPush()` method.
  It returns true if the current response is a push response.
* For each request type, a different response type is created. They all
  implement a `Response` interface. `SelectResponse`, `PrepareResponse`,
  `ExecuteResponse`, `PushResponse` are a part of a public API.
  `Pos()`, `MetaData()`, `SQLInfo()` methods created for them to get specific
  info. Special types of responses are used with special requests.

#### Future type

* Method `Get` now returns response data instead of the actual response.
* New method `GetResponse` added to get an actual response.
* `Future` constructors now accept `Request` as their argument.
* Methods `AppendPush` and `SetResponse` accepts response `Header` and data
  as their arguments.
* Method `Err` was removed because it was causing improper error handling.
  You need to check an error from `Get`, `GetTyped` or `GetResponse` with
  an addition check of a value `Response.Header().Error`, see `ExampleErrorNo`.

#### Connector interface

* Operations `Ping`, `Select`, `Insert`, `Replace`, `Delete`, `Update`,
  `Upsert`, `Call`, `Call16`, `Call17`, `Eval`, `Execute` of a `Connector`
  return response data instead of an actual responses.
* New interface `Doer` is added as a child-interface instead of a `Do` method.

#### Connect function

`connection.Connect` no longer return non-working connection objects. This
function now does not attempt to reconnect and tries to establish a connection
only once. Function might be canceled via context. Context accepted as first
argument, and user may cancel it in process.

Now you need to pass `Dialer` as the second argument instead of URI.
If you were using a non-SSL connection, you need to create `NetDialer`.
For SSL-enabled connections, use `OpenSSLDialer` from the
[go-tlsdialer](https://github.com/tarantool/go-tlsdialer) package.

Please note that the options for creating a connection are now stored in
corresponding `Dialer`, not in `Opts`.

#### Connection schema

* Removed `Schema` field from the `Connection` struct. Instead, new
  `GetSchema(Doer)` function was added to get the actual connection
  schema on demand.
* `OverrideSchema(*Schema)` method replaced with the `SetSchema(Schema)`.

#### Protocol types

* `iproto.Feature` type used instead of `ProtocolFeature`.
* `iproto.IPROTO_FEATURE_` constants used instead of local ones.

#### Schema type

* `ResolveSpaceIndex` function for `SchemaResolver` interface split into two:
`ResolveSpace` and `ResolveIndex`. `NamesUseSupported` function added into the
interface to get information if the usage of space and index names in requests
is supported.
* `Schema` structure no longer implements `SchemaResolver` interface.
* `Spaces` and `SpacesById` fields of the `Schema` struct store spaces by value.
* `Fields` and `FieldsById` fields of the `Space` struct store fields by value.
`Index` and `IndexById` fields of the `Space` struct store indexes by value.
* `Fields` field of the `Index` struct store `IndexField` by value.

### datetime package

Now you need to use objects of the Datetime type instead of pointers to it. A
new constructor `MakeDatetime` returns an object. `NewDatetime` has been
removed.

### decimal package

Now you need to use objects of the Decimal type instead of pointers to it. A
new constructor `MakeDecimal` returns an object. `NewDecimal` has been removed.

### multi package

The subpackage has been deleted. You could use `pool` instead.

### pool package

* The `connection_pool` subpackage has been renamed to `pool`.
* The type `PoolOpts` has been renamed to `Opts`.
* `pool.Connect` and `pool.ConnectWithOpts`  now accept context as the first
  argument, which user may cancel in process. If it is canceled in progress,
  an error will be returned and all created connections will be closed.
* `pool.Connect` and `pool.ConnectWithOpts` now accept `[]pool.Instance` as
  the second argument instead of a list of addresses. Each instance is
  associated with a unique string name, `Dialer` and connection options which
  allows instances to be independently configured.
* `pool.Connect`, `pool.ConnectWithOpts` and `pool.Add` add instances into
  the pool even it is unable to connect to it. The pool will try to connect to
  the instance later.
* `pool.Add` now accepts context as the first argument, which user may cancel
  in process.
* `pool.Add` now accepts `pool.Instance` as the second argument instead of
  an address, it allows to configure a new instance more flexible.
* `pool.GetPoolInfo` has been renamed to `pool.GetInfo`. Return type has been
  changed to `map[string]ConnectionInfo`.
* Operations `Ping`, `Select`, `Insert`, `Replace`, `Delete`, `Update`, `Upsert`,
  `Call`, `Call16`, `Call17`, `Eval`, `Execute` of a `Pooler` return
  response data instead of an actual responses.

### crud package

* `crud` operations `Timeout` option has `crud.OptFloat64` type
  instead of `crud.OptUint`.
* A slice of a custom type could be used as tuples for `ReplaceManyRequest` and
  `InsertManyRequest`, `ReplaceObjectManyRequest`.
* A slice of a custom type could be used as objects for `ReplaceObjectManyRequest`
  and `InsertObjectManyRequest`.

### test_helpers package

* Renamed `StrangerResponse` to `MockResponse`.
