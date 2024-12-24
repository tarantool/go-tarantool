package box

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

func TestSchemaNotNil(t *testing.T) {
	b := NewSchema(nil)
	require.NotNil(t, b)
	require.NotNil(t, b.User())
}

func TestSchemaConnectionsEquality(t *testing.T) {
	var tCases []tarantool.Doer
	for i := 0; i < 10; i++ {
		doer := test_helpers.NewMockDoer(t)
		tCases = append(tCases, &doer)
	}

	for _, tCase := range tCases {
		sch := NewSchema(tCase)
		require.Equal(t, tCase, sch.conn)
		require.Equal(t, tCase, sch.User().conn)
	}
}

func TestSchemaNilConnectionPanics(t *testing.T) {
	ctx := context.Background()
	b := NewSchema(nil)

	require.Panics(t, func() {
		_, _ = b.User().Info(ctx, "panic-on")
	})
}
