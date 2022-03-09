SHELL := /bin/bash
COVERAGE_FILE := coverage.out

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
	go test ./... -v -p 1 -covermode=count -coverprofile=$(COVERAGE_FILE)

.PHONY: coveralls
coveralls: coverage
	go get github.com/mattn/goveralls
	goveralls -coverprofile=$(COVERAGE_FILE) -service=github
