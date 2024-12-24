package box

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

func TestNewSchema(t *testing.T) {
	ctx := context.Background()

	// Create a schema instance with a nil connection. This should lead to a panic later.
	b := NewSchema(nil)

	// Ensure the schema is not nil (which it shouldn't be), but this is not meaningful
	// since we will panic when we call the schema methods with the nil connection.
	t.Run("internal sugar sub-objects not panics", func(t *testing.T) {
		require.NotNil(t, b)
		require.NotNil(t, b.User())
	})

	t.Run("check that connections are equal", func(t *testing.T) {
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
	})

	t.Run("nil conn panics", func(t *testing.T) {
		require.Panics(t, func() {
			_, _ = b.User().Info(ctx, "panic-on")
		})
	})
}
