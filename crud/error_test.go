package crud_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2/crud"
)

func TestErrorMany(t *testing.T) {
	errs := crud.ErrorMany{Errors: []crud.Error{
		{
			ClassName: "a",
			Str:       "msg 1",
		},
		{
			ClassName: "b",
			Str:       "msg 2",
		},
		{
			ClassName: "c",
			Str:       "msg 3",
		},
	}}

	require.Equal(t, "msg 1\nmsg 2\nmsg 3", errs.Error())
}
