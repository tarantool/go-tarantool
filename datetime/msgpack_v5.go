//go:build go_tarantool_msgpack_v5
// +build go_tarantool_msgpack_v5

package datetime

import (
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	msgpack.RegisterExt(datetime_extId, (*Datetime)(nil))
}
