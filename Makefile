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
TAGS :=
TTCTL := tt
ifeq (,$(shell which tt 2>/dev/null))
	TTCTL := tarantoolctl
endif

.PHONY: clean
clean:
	( rm -rf queue/testdata/.rocks crud/testdata/.rocks )
	rm -f $(COVERAGE_FILE)

.PHONY: deps
deps: clean
	( cd ./queue/testdata; $(TTCTL) rocks install queue 1.3.0 )
	( cd ./crud/testdata; $(TTCTL) rocks install crud 1.4.0 )

.PHONY: datetime-timezones
datetime-timezones:
	(cd ./datetime; ./gen-timezones.sh)

.PHONY: format
format:
	goimports -l -w .

.PHONY: golangci-lint
golangci-lint:
	golangci-lint run --config=.golangci.yaml

.PHONY: test
test:
	@echo "Running all packages tests"
	go clean -testcache
	go test -tags "$(TAGS)" ./... -v -p 1

.PHONY: testdata
testdata:
	(cd ./testdata; ./generate.sh)

.PHONY: testrace
testrace:
	@echo "Running all packages tests with data race detector"
	go clean -testcache
	go test -race -tags "$(TAGS)" ./... -v -p 1

.PHONY: test-pool
test-pool:
	@echo "Running tests in pool package"
	go clean -testcache
	go test -tags "$(TAGS)" ./pool/ -v -p 1

.PHONY: test-datetime
test-datetime:
	@echo "Running tests in datetime package"
	go clean -testcache
	go test -tags "$(TAGS)" ./datetime/ -v -p 1

.PHONY: test-decimal
test-decimal:
	@echo "Running tests in decimal package"
	go clean -testcache
	go test -tags "$(TAGS)" ./decimal/ -v -p 1

.PHONY: test-queue
test-queue:
	@echo "Running tests in queue package"
	cd ./queue/ && tarantool -e "require('queue')"
	go clean -testcache
	go test -tags "$(TAGS)" ./queue/ -v -p 1

.PHONY: test-uuid
test-uuid:
	@echo "Running tests in UUID package"
	go clean -testcache
	go test -tags "$(TAGS)" ./uuid/ -v -p 1

.PHONY: test-settings
test-settings:
	@echo "Running tests in settings package"
	go clean -testcache
	go test -tags "$(TAGS)" ./settings/ -v -p 1

.PHONY: test-crud
test-crud:
	@echo "Running tests in crud package"
	cd ./crud/testdata && tarantool -e "require('crud')"
	go clean -testcache
	go test -tags "$(TAGS)" ./crud/ -v -p 1

.PHONY: test-main
test-main:
	@echo "Running tests in main package"
	go clean -testcache
	go test -tags "$(TAGS)" . -v -p 1

.PHONY: coverage
coverage:
	go clean -testcache
	go get golang.org/x/tools/cmd/cover
	go test -tags "$(TAGS)" ./... -v -p 1 -covermode=atomic -coverprofile=$(COVERAGE_FILE)
	go tool cover -func=$(COVERAGE_FILE)

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
	# It is unable to build a latest version of benchstat with go 1.13. So
	# we need to switch to an old commit.
	cd ${BENCH_PATH} && \
		git clone https://go.googlesource.com/perf && \
		cd perf && \
		git checkout 91a04616dc65ba76dbe9e5cf746b923b1402d303 && \
		go install ./cmd/benchstat
	rm -rf ${BENCH_PATH}/perf

.PHONY: bench
${BENCH_FILE} bench: ${BENCH_PATH}
	@echo "Running benchmark tests from the current branch"
	go test -tags "$(TAGS)" ${TEST_PATH} ${BENCH_OPTIONS} 2>&1 \
		| tee ${BENCH_FILE}
	benchstat ${BENCH_FILE}

${GO_TARANTOOL_DIR}:
	@echo "Cloning the repository into ${GO_TARANTOOL_DIR}"
	[ ! -e ${GO_TARANTOOL_DIR} ] && git clone --depth=1 ${GO_TARANTOOL_URL} ${GO_TARANTOOL_DIR}

${REFERENCE_FILE}: ${GO_TARANTOOL_DIR}
	@echo "Running benchmark tests from master for using results in bench-diff target"
	cd ${GO_TARANTOOL_DIR} && git pull && go test ./... -tags "$(TAGS)" ${BENCH_OPTIONS} 2>&1 \
		| tee ${REFERENCE_FILE}

bench-diff: ${BENCH_FILES}
	@echo "Comparing performance between master and the current branch"
	@echo "'old' is a version in master branch, 'new' is a version in a current branch"
	benchstat ${BENCH_FILES} | grep -v pkg:

.PHONY: fuzzing
fuzzing:
	@echo "Running fuzzing tests"
	go clean -testcache
	go test -tags "$(TAGS)" ./... -run=^Fuzz -v -p 1

.PHONY: codespell
codespell:
	@echo "Running codespell"
	codespell
