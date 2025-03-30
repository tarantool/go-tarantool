# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

- A usage of sync.Pool of msgpack.Decoder saves 2 object allocations per
  a response decoding.

### Changed

- Connect() now retry the connection if a failure occurs and opts.Reconnect > 0.
  The number of attempts is equal to opts.MaxReconnects or unlimited if
  opts.MaxReconnects == 0. Connect() blocks until a connection is established,
  the context is cancelled, or the number of attempts is exhausted (#436).

### Fixed

## [v2.3.0] - 2025-03-11

The release extends box.info responses and ConnectionPool.GetInfo return data.

Be careful, we have changed the test_helpers package a little since we do not
support backward compatibility for it.

### Added

- Extend box with replication information (#427).
- The Instance info has been added to ConnectionInfo for ConnectionPool.GetInfo
  response (#429).
- Added helpers to run Tarantool config storage (#431).

### Changed

- Changed helpers API `StartTarantool` and `StopTarantool`, now it uses
  pointer on `TarantoolInstance`:
  * `StartTarantool()` returns `*TarantoolInstance`;
  * `StopTarantool()` and `StopTarantoolWithCleanup()` accepts
    `*TarantoolInstance` as arguments.
- Field `Cmd` in `TarantoolInstance` struct declared as deprecated.
  Suggested `Wait()`, `Stop()` and `Signal()` methods as safer to use
  instead of direct `Cmd.Process` access (#431).

### Fixed

- Test helpers does not detect a fail to start a Tarantool instance if
  another Tarantool instance already listens a port (#431).

## [v2.2.1] - 2024-12-17

The release fixes a schema lost after a reconnect.

### Fixed

- `unable to use an index name because schema is not loaded` error after
  a reconnect (#424).

## [v2.2.0] - 2024-12-16

The release introduces the IPROTO_INSERT_ARROW request (arrow.InsertRequest)
and a request to archive `box.info` values (box.InfoRequest). Additionally, it
includes some improvements to logging.

### Added

- Error logging to `ConnectionPool.Add()` in case, when unable to establish
  connection and ctx is not canceled (#389).
- Error logging for error case of `ConnectionPool.tryConnect()` calls in
  `ConnectionPool.controller()` and `ConnectionPool.reconnect()` (#389).
- Methods that are implemented but not included in the pooler interface (#395).
- Implemented stringer methods for pool.Role (#405).
- Support the IPROTO_INSERT_ARROW request (#399).
- A simple implementation of using the box interface (#410).

### Changed

- More informative request canceling: log the probable reason for unexpected
  request ID and add request ID info to context done error message (#407).

## [v2.1.0] - 2024-03-06

The small release improves the ConnectionPool. The ConnectionPool now does not
require execute access for `box.info` from a user for Tarantool >= 3.0.0.

### Changed

- `execute` access for `box.info` is no longer required for ConnectionPool
  for a Tarantool version >= 3.0.0 (#380).

### Fixed

- `ConnectionPool.Remove()` does not notify a `ConnectionHandler` after
  an instance is already removed from the pool (#385).

## [v2.0.0] - 2024-02-12

There are a lot of changes in the new major version. The main ones:

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

See the [migration guide](./MIGRATION.md) for more details.

### Added

- Type() method to the Request interface (#158).
- Enumeration types for RLimitAction/iterators (#158).
- IsNullable flag for Field (#302).
- More linters on CI (#310).
- Meaningful description for read/write socket errors (#129).
- Support `operation_data` in `crud.Error` (#330).
- Support `fetch_latest_metadata` option for crud requests with
  metadata (#335).
- Support `noreturn` option for data change crud requests (#335).
- Support `crud.schema` request (#336, #351).
- Support `IPROTO_WATCH_ONCE` request type for Tarantool
  version >= 3.0.0-alpha1 (#337).
- Support `yield_every` option for crud select requests (#350).
- Support `IPROTO_FEATURE_SPACE_AND_INDEX_NAMES` for Tarantool
  version >= 3.0.0-alpha1 (#338). It allows to use space and index names
  in requests instead of their IDs.
- `GetSchema` function to get the actual schema (#7).
- Support connection via an existing socket fd (#321).
- `Header` struct for the response header (#237). It can be accessed via
  `Header()` method of the `Response` interface.
- `Response` method added to the `Request` interface (#237).
- New `LogAppendPushFailed` connection log constant (#237).
  It is logged when connection fails to append a push response.
- `ErrorNo` constant that indicates that no error has occurred while getting
  the response (#237).
- Ability to mock connections for tests (#237). Added new types `MockDoer`,
  `MockRequest` to `test_helpers`.
- `AuthDialer` type for creating a dialer with authentication (#301).
- `ProtocolDialer` type for creating a dialer with `ProtocolInfo` receiving and
  check (#301).
- `GreetingDialer` type for creating a dialer, that fills `Greeting` of a
  connection (#301).
- New method `Pool.DoInstance` to execute a request on a target instance in
  a pool (#376).

### Changed

- connection_pool renamed to pool (#239).
- Use msgpack/v5 instead of msgpack.v2 (#236).
- Call/NewCallRequest = Call17/NewCall17Request (#235).
- Change encoding of the queue.Identify() UUID argument from binary blob to
  plain string. Needed for upgrade to Tarantool 3.0, where a binary blob is
  decoded to a varbinary object (#313).
- Use objects of the Decimal type instead of pointers (#238).
- Use objects of the Datetime type instead of pointers (#238).
- `connection.Connect` no longer return non-working
  connection objects (#136). This function now does not attempt to reconnect
  and tries to establish a connection only once. Function might be canceled
  via context. Context accepted as first argument.
  `pool.Connect` and `pool.Add` now accept context as the first argument, which
  user may cancel in process. If `pool.Connect` is canceled in progress, an
  error will be returned. All created connections will be closed.
- `iproto.Feature` type now used instead of `ProtocolFeature` (#337).
- `iproto.IPROTO_FEATURE_` constants now used instead of local `Feature`
  constants for `protocol` (#337).
- Change `crud` operations `Timeout` option type to `crud.OptFloat64`
  instead of `crud.OptUint` (#342).
- Change all `Upsert` and `Update` requests to accept `*tarantool.Operations`
  as `ops` parameters instead of `interface{}` (#348).
- Change `OverrideSchema(*Schema)` to `SetSchema(Schema)` (#7).
- Change values, stored by pointers in the `Schema`, `Space`, `Index` structs,
  to be stored by their values (#7).
- Make `Dialer` mandatory for creation a single connection (#321).
- Remove `Connection.RemoteAddr()`, `Connection.LocalAddr()`.
  Add `Addr()` function instead (#321).
- Remove `Connection.ClientProtocolInfo`, `Connection.ServerProtocolInfo`.
  Add `ProtocolInfo()` function, which returns the server protocol info (#321).
- `NewWatcher` checks the actual features of the server, rather than relying
  on the features provided by the user during connection creation (#321).
- `pool.NewWatcher` does not create watchers for connections that do not support
  it (#321).
- Rename `pool.GetPoolInfo` to `pool.GetInfo`. Change return type to
  `map[string]ConnectionInfo` (#321).
- `Response` is now an interface (#237).
- All responses are now implementations of the `Response` interface (#237).
  `SelectResponse`, `ExecuteResponse`, `PrepareResponse`, `PushResponse` are part
  of a public API. `Pos()`, `MetaData()`, `SQLInfo()` methods created for them
  to get specific info.
  Special types of responses are used with special requests.
- `IsPush()` method is added to the response iterator (#237). It returns
  the information if the current response is a `PushResponse`.
  `PushCode` constant is removed.
- Method `Get` for `Future` now returns response data (#237). To get the actual
  response new `GetResponse` method has been added. Methods `AppendPush` and
  `SetResponse` accept response `Header` and data as their arguments.
- `Future` constructors now accept `Request` as their argument (#237).
- Operations `Ping`, `Select`, `Insert`, `Replace`, `Delete`, `Update`, `Upsert`,
  `Call`, `Call16`, `Call17`, `Eval`, `Execute` of a `Connector` and `Pooler`
  return response data instead of an actual responses (#237).
- Renamed `StrangerResponse` to `MockResponse` (#237).
- `pool.Connect`, `pool.ConnetcWithOpts` and `pool.Add` use a new type
  `pool.Instance` to determinate connection options (#356).
- `pool.Connect`, `pool.ConnectWithOpts` and `pool.Add` add connections to
  the pool even it is unable to connect to it (#372).
- Required Go version updated from `1.13` to `1.20` (#378).

### Deprecated

- All Connection.<Request>, Connection.<Request>Typed and
  Connection.<Request>Async methods. Instead you should use requests objects +
  Connection.Do() (#241).
- All ConnectionPool.<Request>, ConnectionPool.<Request>Typed and
  ConnectionPool.<Request>Async methods. Instead you should use requests
  objects + ConnectionPool.Do() (#241).
- box.session.push() usage: Future.AppendPush() and Future.GetIterator()
  methods, ResponseIterator and TimeoutResponseIterator types (#324).

### Removed

- multi subpackage (#240).
- msgpack.v2 support (#236).
- pool/RoundRobinStrategy (#158).
- DeadlineIO (#158).
- UUID_extId (#158).
- IPROTO constants (#158).
- Code() method from the Request interface (#158).
- `Schema` field from the `Connection` struct (#7).
- `OkCode` and `PushCode` constants (#237).
- SSL support (#301).
- `Future.Err()` method (#382).

### Fixed

- Flaky decimal/TestSelect (#300).
- Race condition at roundRobinStrategy.GetNextConnection() (#309).
- Incorrect decoding of an MP_DECIMAL when the `scale` value is
  negative (#314).
- Incorrect options (`after`, `batch_size` and `force_map_call`) setup for
  crud.SelectRequest (#320).
- Incorrect options (`vshard_router`, `fields`, `bucket_id`, `mode`,
  `prefer_replica`, `balance`) setup for crud.GetRequest (#335).
- Tests with crud 1.4.0 (#336).
- Tests with case sensitive SQL (#341).
- Splice update operation accepts 3 arguments instead of 5 (#348).
- Unable to use a slice of custom types as a slice of tuples or objects for
  `crud.*ManyRequest/crud.*ObjectManyRequest` (#365).

## [v1.12.0] - 2023-06-07

The release introduces the ability to gracefully close Connection
and ConnectionPool and also provides methods for adding or removing an endpoint
from a ConnectionPool.

### Added

- Connection.CloseGraceful() unlike Connection.Close() waits for all
  requests to complete (#257).
- ConnectionPool.CloseGraceful() unlike ConnectionPool.Close() waits for all
  requests to complete (#257).
- ConnectionPool.Add()/ConnectionPool.Remove() to add/remove endpoints
  from a pool (#290).

### Changed

### Fixed

- crud tests with Tarantool 3.0 (#293).
- SQL tests with Tarantool 3.0 (#295).

## [v1.11.0] - 2023-05-18

The release adds pagination support and wrappers for the
[crud](https://github.com/tarantool/crud) module.

### Added

- Support pagination (#246).
- A Makefile target to test with race detector (#218).
- Support CRUD API (#108).
- An ability to replace a base network connection to a Tarantool
  instance (#265).
- Missed iterator constant (#285).

### Changed

- queue module version bumped to 1.3.0 (#278).

### Fixed

- Several non-critical data race issues (#218).
- Build on Apple M1 with OpenSSL (#260).
- ConnectionPool does not properly handle disconnection with Opts.Reconnect
  set (#272).
- Watcher events loss with a small per-request timeout (#284).
- Connect() panics on concurrent schema update (#278).
- Wrong Ttr setup by Queue.Cfg() (#278).
- Flaky queue/Example_connectionPool (#278).
- Flaky queue/Example_simpleQueueCustomMsgPack (#277).

## [v1.10.0] - 2022-12-31

The release improves compatibility with new Tarantool versions.

### Added

- Support iproto feature discovery (#120).
- Support errors extended information (#209).
- Support error type in MessagePack (#209).
- Support event subscription (#119).
- Support session settings (#215).
- Support pap-sha256 authorization method (Tarantool EE feature) (#243).
- Support graceful shutdown (#214).

### Fixed

- Decimal package uses a test variable DecimalPrecision instead of a
  package-level variable decimalPrecision (#233).
- Flaky test TestClientRequestObjectsWithContext (#244).
- Flaky test multi/TestDisconnectAll (#234).
- Build on macOS with Apple M1 (#260).

## [v1.9.0] - 2022-11-02

The release adds support for the latest version of the
[queue package](https://github.com/tarantool/queue) with master-replica
switching.

### Added

- Support the queue 1.2.1 (#177).
- ConnectionHandler interface for handling changes of connections in
  ConnectionPool (#178).
- Execute, ExecuteTyped and ExecuteAsync methods to ConnectionPool (#176).
- ConnectorAdapter type to use ConnectionPool as Connector interface (#176).
- An example how to use queue and connection_pool subpackages together (#176).

### Fixed

- Mode type description in the connection_pool subpackage (#208).
- Missed Role type constants in the connection_pool subpackage (#208).
- ConnectionPool does not close UnknownRole connections (#208).
- Segmentation faults in ConnectionPool requests after disconnect (#208).
- Addresses in ConnectionPool may be changed from an external code (#208).
- ConnectionPool recreates connections too often (#208).
- A connection is still opened after ConnectionPool.Close() (#208).
- Future.GetTyped() after Future.Get() does not decode response
  correctly (#213).
- Decimal package uses a test function GetNumberLength instead of a
  package-level function getNumberLength (#219).
- Datetime location after encode + decode is unequal (#217).
- Wrong interval arithmetic with timezones (#221).
- Invalid MsgPack if STREAM_ID > 127 (#224).
- queue.Take() returns an invalid task (#222).

## [v1.8.0] - 2022-08-17

The minor release with time zones and interval support for datetime.

### Added

- Optional msgpack.v5 usage (#124).
- TZ support for datetime (#163).
- Interval support for datetime (#165).

### Fixed

- Markdown of documentation for the decimal subpackage (#201).

## [v1.7.0] - 2022-08-02

This release adds a number of features. The extending of the public API has
become possible with a new way of creating requests. New types of requests are
created via chain calls. Streams, context and prepared statements support are
based on this idea.

### Added

- SSL support (#155).
- IPROTO_PUSH messages support (#67).
- Public API with request object types (#126).
- Support decimal type in msgpack (#96).
- Support datetime type in msgpack (#118).
- Prepared SQL statements (#117).
- Context support for request objects (#48).
- Streams and interactive transactions support (#101).
- `Call16` method, support build tag `go_tarantool_call_17` to choose
  default behavior for `Call` method as Call17 (#125).

### Changed

- `IPROTO_*` constants that identify requests renamed from `<Name>Request` to
  `<Name>RequestCode` (#126).

### Removed

- NewErrorFuture function (#190).

### Fixed

- Add `ExecuteAsync` and `ExecuteTyped` to common connector interface (#62).

## [v1.6.0] - 2022-06-01

This release adds a number of features. Also it significantly improves testing,
CI and documentation.

### Added

- Coveralls support (#149).
- Reusable testing workflow (integration testing with latest Tarantool) (#112).
- Simple CI based on GitHub actions (#114).
- Support UUID type in msgpack (#90).
- Go modules support (#91).
- queue-utube handling (#85).
- Master discovery (#113).
- SQL support (#62).

### Changed

- Handle everything with `go test` (#115).
- Use plain package instead of module for UUID submodule (#134).
- Reset buffer if its average use size smaller than quarter of capacity (#95).
- Update API documentation: comments and examples (#123).

### Fixed

- Fix queue tests (#107).
- Make test case consistent with comments (#105).

## [1.5] - 2019-12-29

First release.

### Fixed

- Fix infinite recursive call of `Upsert` method for `ConnectionMulti`.
- Fix index out of range panic on `dial()` to short address.
- Fix cast in `defaultLogger.Report` (#49).
- Fix race condition on extremely small request timeouts (#43).
- Fix notify for `Connected` transition.
- Fix reconnection logic and add `Opts.SkipSchema` method.
- Fix future sending.
- Fix panic on disconnect + timeout.
- Fix block on msgpack error.
- Fix ratelimit.
- Fix `timeouts` method for `Connection`.
- Fix possible race condition on extremely small request timeouts.
- Fix race condition on future channel creation.
- Fix block on forever closed connection.
- Fix race condition in `Connection`.
- Fix extra map fields.
- Fix response header parsing.
- Fix reconnect logic in `Connection`.

### Changed

- Make logger configurable.
- Report user mismatch error immediately.
- Set limit timeout by 0.9 of connection to queue request timeout.
- Update fields could be negative.
- Require `RLimitAction` to be specified if `RateLimit` is specified.
- Use newer typed msgpack interface.
- Do not start timeouts goroutine if no timeout specified.
- Clear buffers on connection close.
- Update `BenchmarkClientParallelMassive`.
- Remove array requirements for keys and opts.
- Do not allocate `Response` inplace.
- Respect timeout on request sending.
- Use `AfterFunc(fut.timeouted)` instead of `time.NewTimer()`.
- Use `_vspace`/`_vindex` for introspection.
- Method `Tuples()` always returns table for response.

### Removed

- Remove `UpsertTyped()` method (#23).

### Added

- Add methods `Future.WaitChan` and `Future.Err` (#86).
- Get node list from nodes (#81).
- Add method `deleteConnectionFromPool`.
- Add multiconnections support.
- Add `Addr` method for the connection (#64).
- Add `Delete` method for the queue.
- Implemented typed taking from queue (#55).
- Add `OverrideSchema` method for the connection.
- Add default case to default logger.
- Add license (BSD-2 clause as for Tarantool).
- Add `GetTyped` method for the connection (#40).
- Add `ConfiguredTimeout` method for the connection, change queue interface.
- Add an example for queue.
- Add `GetQueue` method for the queue.
- Add queue support.
- Add support of Unix socket address.
- Add check for prefix "tcp:".
- Add the ability to work with the Tarantool via Unix socket.
- Add note about magic way to pack tuples.
- Add notification about connection state change.
- Add workaround for tarantool/tarantool#2060 (#32).
- Add `ConnectedNow` method for the connection.
- Add IO deadline and use `net.Conn.Set(Read|Write)Deadline`.
- Add a couple of benchmarks.
- Add timeout on connection attempt.
- Add `RLimitAction` option.
- Add `Call17` method for the connection to make a call compatible with
  Tarantool 1.7.
- Add `ClientParallelMassive` benchmark.
- Add `runtime.Gosched` for decreasing `writer.flush` count.
- Add `Eval`, `EvalTyped`, `SelectTyped`, `InsertTyped`, `ReplaceTyped`,
  `DeleteRequest`, `UpdateTyped`, `UpsertTyped` methods.
- Add `UpdateTyped` method.
- Add `CallTyped` method.
- Add possibility to pass `Space` and `Index` objects into `Select` etc.
- Add custom MsgPack pack/unpack functions.
- Add support of Tarantool 1.6.8 schema format.
- Add support of Tarantool 1.6.5 schema format.
- Add schema loading.
- Add `LocalAddr` and `RemoteAddr` methods for the connection.
- Add `Upsert` method for the connection.
- Add `Eval` and `EvalAsync` methods for the connection.
- Add Tarantool error codes.
- Add auth support.
- Add auth during reconnect.
- Add auth request.
