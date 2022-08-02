package datetime

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

func init() {
	msgpack.RegisterExt(datetime_extId, &Datetime{})
}
