SHELL := /bin/bash
COVERAGE_FILE := coverage.out
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_DIR := $(patsubst %/,%,$(dir $(MAKEFILE_PATH)))
DURATION ?= 3s
COUNT ?= 5
BENCH_PATH ?= bench-dir
TEST_PATH ?= ${PROJECT_DIR}/...
BENCH_FILE := ${PROJECT_DIR}/${BENCH_PATH}/bench.txt
REFERENCE_FILE := ${PROJECT_DIR}/${BENCH_PATH}/reference.txt
BENCH_FILES := ${REFERENCE_FILE} ${BENCH_FILE}
BENCH_REFERENCE_REPO := ${BENCH_PATH}/go-tarantool
BENCH_OPTIONS := -bench=. -run=^Benchmark -benchmem -benchtime=${DURATION} -count=${COUNT}
GO_TARANTOOL_URL := https://github.com/tarantool/go-tarantool
GO_TARANTOOL_DIR := ${PROJECT_DIR}/${BENCH_PATH}/go-tarantool

.PHONY: clean
clean:
	( cd ./queue; rm -rf .rocks )
	rm -f $(COVERAGE_FILE)

.PHONY: deps
deps: clean
	( cd ./queue; tarantoolctl rocks install queue 1.1.0 )

.PHONY: test
test:
	go test ./... -v -p 1

.PHONY: test-connection-pool
test-connection-pool:
	@echo "Running tests in connection_pool package"
	go clean -testcache
	go test ./connection_pool/ -v -p 1

.PHONY: test-multi
test-multi:
	@echo "Running tests in multiconnection package"
	go clean -testcache
	go test ./multi/ -v -p 1

.PHONY: test-queue
test-queue:
	@echo "Running tests in queue package"
	cd ./queue/ && tarantool -e "require('queue')"
	go clean -testcache
	go test ./queue/ -v -p 1

.PHONY: test-uuid
test-uuid:
	@echo "Running tests in UUID package"
	go clean -testcache
	go test ./uuid/ -v -p 1

.PHONY: test-main
test-main:
	@echo "Running tests in main package"
	go clean -testcache
	go test . -v -p 1

.PHONY: coverage
coverage:
	go clean -testcache
	go get golang.org/x/tools/cmd/cover
	go test ./... -v -p 1 -covermode=atomic -coverprofile=$(COVERAGE_FILE) -coverpkg=./...

.PHONY: coveralls
coveralls: coverage
	go get github.com/mattn/goveralls
	goveralls -coverprofile=$(COVERAGE_FILE) -service=github

.PHONY: bench-deps
${BENCH_PATH} bench-deps:
	@echo "Installing benchstat tool"
	rm -rf ${BENCH_PATH}
	mkdir ${BENCH_PATH}
	go clean -testcache
	cd ${BENCH_PATH} && git clone https://go.googlesource.com/perf && cd perf && go install ./cmd/benchstat
	rm -rf ${BENCH_PATH}/perf

.PHONY: bench
${BENCH_FILE} bench: ${BENCH_PATH}
	@echo "Running benchmark tests from the current branch"
	go test ${TEST_PATH} ${BENCH_OPTIONS} 2>&1 \
		| tee ${BENCH_FILE}
	benchstat ${BENCH_FILE}

${GO_TARANTOOL_DIR}:
	@echo "Cloning the repository into ${GO_TARANTOOL_DIR}"
	[ ! -e ${GO_TARANTOOL_DIR} ] && git clone --depth=1 ${GO_TARANTOOL_URL} ${GO_TARANTOOL_DIR}

${REFERENCE_FILE}: ${GO_TARANTOOL_DIR}
	@echo "Running benchmark tests from master for using results in bench-diff target"
	cd ${GO_TARANTOOL_DIR} && git pull && go test ./... ${BENCH_OPTIONS} 2>&1 \
		| tee ${REFERENCE_FILE}

bench-diff: ${BENCH_FILES}
	@echo "Comparing performance between master and the current branch"
	@echo "'old' is a version in master branch, 'new' is a version in a current branch"
	benchstat ${BENCH_FILES} | grep -v pkg:
