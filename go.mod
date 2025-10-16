module github.com/tarantool/go-tarantool/v3

go 1.24

require (
	github.com/google/uuid v1.6.0
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.11.1
	github.com/tarantool/go-iproto v1.1.0
	github.com/tarantool/go-option v1.0.0
	github.com/vmihailenco/msgpack/v5 v5.4.1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

tool (
	github.com/tarantool/go-option/cmd/gentypes
	golang.org/x/tools/cmd/stringer
)
