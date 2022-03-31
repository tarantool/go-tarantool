# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic
Versioning](http://semver.org/spec/v2.0.0.html) except to the first release.

## [Unreleased]

### Added

- Coveralls support (#149)
- Reusable testing workflow (integration testing with latest Tarantool) (#123)
- Simple CI based on GitHub actions (#114)
- Support UUID type in msgpack (#90)
- Go modules support (#91)
- queue-utube handling (#85)

### Fixed

- Fix queue tests (#107)
- Make test case consistent with comments (#105)

### Changed

- Handle everything with `go test` (#115)
- Use plain package instead of module for UUID submodule (#134)
- Reset buffer if its average use size smaller than quater of capacity (#95)

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
