//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package settings

import (
	"github.com/vmihailenco/msgpack/v5"
)

type encoder = msgpack.Encoder