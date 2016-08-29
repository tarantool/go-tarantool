package tarantool

import (
	"fmt"
)

type Error struct {
	Code uint32
	Msg  string
}

func (tnterr Error) Error() string {
	return fmt.Sprintf("%s (0x%x)", tnterr.Msg, tnterr.Code)
}

type ClientError struct {
	Code uint32
	Msg  string
}

func (clierr ClientError) Error() string {
	return fmt.Sprintf("%s (0x%x)", clierr.Msg, clierr.Code)
}

func (clierr ClientError) Temporary() bool {
	switch clierr.Code {
	case ErrConnectionNotReady:
		return true
	default:
		return false
	}
}
