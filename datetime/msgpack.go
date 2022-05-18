//go:build !go_tarantool_msgpack_v5
// +build !go_tarantool_msgpack_v5

package datetime

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

func init() {
	msgpack.RegisterExt(datetime_extId, &Datetime{})
}
