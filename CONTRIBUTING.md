# Contribution Guide

## First steps

Clone the repository and install dependencies.

```sh
$ git clone git@github.com:/tarantool/go-tarantool
$ cd go-tarantool
$ go get .
```

## Running tests

You need to [install Tarantool](https://tarantool.io/en/download/) to run tests.
See the Installation section in the README for requirements.

To install test dependencies (such as the
[tarantool/queue](https://github.com/tarantool/queue) module), run:
```bash
make deps
```

To run tests for the main package and each subpackage:
```bash
make test
```

The tests set up all required `tarantool` processes before run and clean up
afterwards.

If you want to run the tests for a specific package:
```bash
make test-<SUBDIR>
```
For example, for running tests in `multi`, `uuid` and `main` packages, call
```bash
make test-multi test-uuid test-main
```
