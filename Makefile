SHELL := /bin/bash

.PHONY: clean
clean:
	( cd ./queue; rm -rf .rocks )

.PHONY: deps
deps: clean
	( cd ./queue; tarantoolctl rocks install queue 1.1.0 )

.PHONY: test
test:
	go clean -testcache
	go test ./... -v -p 1	
