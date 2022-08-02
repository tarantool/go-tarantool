package decimal

import (
	"gopkg.in/vmihailenco/msgpack.v2"
)

func init() {
	msgpack.RegisterExt(decimalExtID, &Decimal{})
}
