# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

- Type() method to the Request interface (#158)
- Enumeration types for RLimitAction/iterators (#158)
- IsNullable flag for Field (#302)
- More linters on CI (#310)
- Meaningful description for read/write socket errors (#129)
- Support password and password file to decrypt private SSL key file (#319)
- Support `operation_data` in `crud.Error` (#330) 
- Support `fetch_latest_metadata` option for crud requests with metadata (#335)
- Support `noreturn` option for data change crud requests (#335)
- Support `crud.schema` request (#336)

### Changed

- connection_pool renamed to pool (#239)
- Use msgpack/v5 instead of msgpack.v2 (#236)
- Call/NewCallRequest = Call17/NewCall17Request (#235)
- Change encoding of the queue.Identify() UUID argument from binary blob to
  plain string. Needed for upgrade to Tarantool 3.0, where a binary blob is
  decoded to a varbinary object (#313).
- Use objects of the Decimal type instead of pointers (#238)
- Use objects of the Datetime type instead of pointers (#238)

### Deprecated

- All Connection.<Request>, Connection.<Request>Typed and
  Connection.<Request>Async methods. Instead you should use requests objects +
  Connection.Do() (#241)
- All ConnectionPool.<Request>, ConnectionPool.<Request>Typed and
  ConnectionPool.<Request>Async methods. Instead you should use requests
  objects + ConnectionPool.Do() (#241)

### Removed

- multi subpackage (#240)
- msgpack.v2 support (#236)
- pool/RoundRobinStrategy (#158)
- DeadlineIO (#158)
- UUID_extId (#158)
- IPROTO constants (#158)
- Code() method from the Request interface (#158)

### Fixed

- Flaky decimal/TestSelect (#300)
- Race condition at roundRobinStrategy.GetNextConnection() (#309)
- Incorrect decoding of an MP_DECIMAL when the `scale` value is negative (#314)
- Incorrect options (`after`, `batch_size` and `force_map_call`) setup for
  crud.SelectRequest (#320)
- Incorrect options (`vshard_router`, `fields`, `bucket_id`, `mode`,
  `prefer_replica`, `balance`) setup for crud.GetRequest (#335)
- Tests with crud 1.4.0 (#336)

## [1.12.0] - 2023-06-07

The release introduces the ability to gracefully close Connection
and ConnectionPool and also provides methods for adding or removing an endpoint
from a ConnectionPool.

### Added

- Connection.CloseGraceful() unlike Connection.Close() waits for all
  requests to complete (#257)
- ConnectionPool.CloseGraceful() unlike ConnectionPool.Close() waits for all
  requests to complete (#257)
- ConnectionPool.Add()/ConnectionPool.Remove() to add/remove endpoints
  from a pool (#290)

### Changed

### Fixed

- crud tests with Tarantool 3.0 (#293)
- SQL tests with Tarantool 3.0 (#295)

## [1.11.0] - 2023-05-18

The release adds pagination support and wrappers for the
[crud](https://github.com/tarantool/crud) module.

### Added

- Support pagination (#246)
- A Makefile target to test with race detector (#218)
- Support CRUD API (#108)
- An ability to replace a base network connection to a Tarantool
  instance (#265)
- Missed iterator constant (#285)

### Changed

- queue module version bumped to 1.3.0 (#278)

### Fixed

- Several non-critical data race issues (#218)
- Build on Apple M1 with OpenSSL (#260)
- ConnectionPool does not properly handle disconnection with Opts.Reconnect
  set (#272)
- Watcher events loss with a small per-request timeout (#284)
- Connect() panics on concurrent schema update (#278)
- Wrong Ttr setup by Queue.Cfg() (#278)
- Flaky queue/Example_connectionPool (#278)
- Flaky queue/Example_simpleQueueCustomMsgPack (#277)

## [1.10.0] - 2022-12-31

The release improves compatibility with new Tarantool versions.

### Added

- Support iproto feature discovery (#120)
- Support errors extended information (#209)
- Support error type in MessagePack (#209)
- Support event subscription (#119)
- Support session settings (#215)
- Support pap-sha256 authorization method (Tarantool EE feature) (#243)
- Support graceful shutdown (#214)

### Fixed

- Decimal package uses a test variable DecimalPrecision instead of a
  package-level variable decimalPrecision (#233)
- Flaky test TestClientRequestObjectsWithContext (#244)
- Flaky test multi/TestDisconnectAll (#234)
- Build on macOS with Apple M1 (#260)

## [1.9.0] - 2022-11-02

The release adds support for the latest version of the
[queue package](https://github.com/tarantool/queue) with master-replica
switching.

### Added

- Support the queue 1.2.1 (#177)
- ConnectionHandler interface for handling changes of connections in
  ConnectionPool (#178)
- Execute, ExecuteTyped and ExecuteAsync methods to ConnectionPool (#176)
- ConnectorAdapter type to use ConnectionPool as Connector interface (#176)
- An example how to use queue and connection_pool subpackages together (#176)

### Fixed

- Mode type description in the connection_pool subpackage (#208)
- Missed Role type constants in the connection_pool subpackage (#208)
- ConnectionPool does not close UnknownRole connections (#208)
- Segmentation faults in ConnectionPool requests after disconnect (#208)
- Addresses in ConnectionPool may be changed from an external code (#208)
- ConnectionPool recreates connections too often (#208)
- A connection is still opened after ConnectionPool.Close() (#208)
- Future.GetTyped() after Future.Get() does not decode response
  correctly (#213)
- Decimal package uses a test function GetNumberLength instead of a
  package-level function getNumberLength (#219)
- Datetime location after encode + decode is unequal (#217)
- Wrong interval arithmetic with timezones (#221)
- Invalid MsgPack if STREAM_ID > 127 (#224)
- queue.Take() returns an invalid task (#222)

## [1.8.0] - 2022-08-17

The minor release with time zones and interval support for datetime.

### Added

- Optional msgpack.v5 usage (#124)
- TZ support for datetime (#163)
- Interval support for datetime (#165)

### Changed

### Fixed

- Markdown of documentation for the decimal subpackage (#201)

## [1.7.0] - 2022-08-02

This release adds a number of features. The extending of the public API has
become possible with a new way of creating requests. New types of requests are
created via chain calls. Streams, context and prepared statements support are
based on this idea.

### Added

- SSL support (#155)
- IPROTO_PUSH messages support (#67)
- Public API with request object types (#126)
- Support decimal type in msgpack (#96)
- Support datetime type in msgpack (#118)
- Prepared SQL statements (#117)
- Context support for request objects (#48)
- Streams and interactive transactions support (#101)
- `Call16` method, support build tag `go_tarantool_call_17` to choose
  default behavior for `Call` method as Call17 (#125)

### Changed

- `IPROTO_*` constants that identify requests renamed from `<Name>Request` to
  `<Name>RequestCode` (#126)

### Removed

- NewErrorFuture function (#190)

### Fixed

- Add `ExecuteAsync` and `ExecuteTyped` to common connector interface (#62)

## [1.6.0] - 2022-06-01

This release adds a number of features. Also it significantly improves testing,
CI and documentation.

### Added

- Coveralls support (#149)
- Reusable testing workflow (integration testing with latest Tarantool) (#112)
- Simple CI based on GitHub actions (#114)
- Support UUID type in msgpack (#90)
- Go modules support (#91)
- queue-utube handling (#85)
- Master discovery (#113)
- SQL support (#62)

### Changed

- Handle everything with `go test` (#115)
- Use plain package instead of module for UUID submodule (#134)
- Reset buffer if its average use size smaller than quarter of capacity (#95)
- Update API documentation: comments and examples (#123)

### Fixed

- Fix queue tests (#107)
- Make test case consistent with comments (#105)

## [1.5] - 2019-12-29

First release.

### Fixed

- Fix infinite recursive call of `Upsert` method for `ConnectionMulti`
- Fix index out of range panic on `dial()` to short address
- Fix cast in `defaultLogger.Report` (#49)
- Fix race condition on extremely small request timeouts (#43)
- Fix notify for `Connected` transition
- Fix reconnection logic and add `Opts.SkipSchema` method
- Fix future sending
- Fix panic on disconnect + timeout
- Fix block on msgpack error
- Fix ratelimit
- Fix `timeouts` method for `Connection`
- Fix possible race condition on extremely small request timeouts
- Fix race condition on future channel creation
- Fix block on forever closed connection
- Fix race condition in `Connection`
- Fix extra map fields
- Fix response header parsing
- Fix reconnect logic in `Connection`

### Changed

- Make logger configurable
- Report user mismatch error immediately
- Set limit timeout by 0.9 of connection to queue request timeout
- Update fields could be negative
- Require `RLimitAction` to be specified if `RateLimit` is specified
- Use newer typed msgpack interface
- Do not start timeouts goroutine if no timeout specified
- Clear buffers on connection close
- Update `BenchmarkClientParallelMassive`
- Remove array requirements for keys and opts
- Do not allocate `Response` inplace
- Respect timeout on request sending
- Use `AfterFunc(fut.timeouted)` instead of `time.NewTimer()`
- Use `_vspace`/`_vindex` for introspection
- Method `Tuples()` always returns table for response

### Removed

- Remove `UpsertTyped()` method (#23)

### Added

- Add methods `Future.WaitChan` and `Future.Err` (#86)
- Get node list from nodes (#81)
- Add method `deleteConnectionFromPool`
- Add multiconnections support
- Add `Addr` method for the connection (#64)
- Add `Delete` method for the queue
- Implemented typed taking from queue (#55)
- Add `OverrideSchema` method for the connection
- Add default case to default logger
- Add license (BSD-2 clause as for Tarantool)
- Add `GetTyped` method for the connection (#40)
- Add `ConfiguredTimeout` method for the connection, change queue interface
- Add an example for queue
- Add `GetQueue` method for the queue
- Add queue support
- Add support of Unix socket address
- Add check for prefix "tcp:"
- Add the ability to work with the Tarantool via Unix socket
- Add note about magic way to pack tuples
- Add notification about connection state change
- Add workaround for tarantool/tarantool#2060 (#32)
- Add `ConnectedNow` method for the connection
- Add IO deadline and use `net.Conn.Set(Read|Write)Deadline`
- Add a couple of benchmarks
- Add timeout on connection attempt
- Add `RLimitAction` option
- Add `Call17` method for the connection to make a call compatible with Tarantool 1.7
- Add `ClientParallelMassive` benchmark
- Add `runtime.Gosched` for decreasing `writer.flush` count
- Add `Eval`, `EvalTyped`, `SelectTyped`, `InsertTyped`, `ReplaceTyped`, `DeleteRequest`, `UpdateTyped`, `UpsertTyped` methods
- Add `UpdateTyped` method
- Add `CallTyped` method
- Add possibility to pass `Space` and `Index` objects into `Select` etc.
- Add custom MsgPack pack/unpack functions
- Add support of Tarantool 1.6.8 schema format
- Add support of Tarantool 1.6.5 schema format
- Add schema loading
- Add `LocalAddr` and `RemoteAddr` methods for the connection
- Add `Upsert` method for the connection
- Add `Eval` and `EvalAsync` methods for the connection
- Add Tarantool error codes
- Add auth support
- Add auth during reconnect
- Add auth request
