package box_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3/box"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
)

func TestNew(t *testing.T) {
	t.Parallel()

	_, err := box.New(nil)
	require.Error(t, err)
}

func TestMustNew(t *testing.T) {
	t.Parallel()

	// Create a box instance with a nil connection. This should lead to a panic.
	require.Panics(t, func() { box.MustNew(nil) })
}

func TestMocked_BoxNew(t *testing.T) {
	t.Parallel()

	mock := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, "valid"),
	)

	b, err := box.New(&mock)
	require.NoError(t, err)
	require.NotNil(t, b)

	assert.Len(t, mock.Requests, 0)
	_, _ = b.Schema().User().Exists(box.NewInfoRequest().Ctx(), "")
	require.Len(t, mock.Requests, 1)
}

func TestMocked_BoxInfo(t *testing.T) {
	t.Parallel()

	data := []interface{}{
		map[string]interface{}{
			"version":     "1.0.0",
			"id":          nil,
			"ro":          false,
			"uuid":        "uuid",
			"pid":         456,
			"status":      "status",
			"lsn":         123,
			"replication": nil,
		},
	}
	mock := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, data),
	)
	b := box.MustNew(&mock)

	info, err := b.Info()
	require.NoError(t, err)

	assert.Equal(t, "1.0.0", info.Version)
	assert.Equal(t, 456, info.PID)
}

func TestMocked_BoxSchemaUserInfo(t *testing.T) {
	t.Parallel()

	data := []interface{}{
		[]interface{}{
			[]interface{}{"read,write,execute", "universe", ""},
		},
	}
	mock := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, data),
	)
	b := box.MustNew(&mock)

	privs, err := b.Schema().User().Info(context.Background(), "username")
	require.NoError(t, err)

	assert.Equal(t, []box.Privilege{
		{
			Permissions: []box.Permission{
				box.PermissionRead,
				box.PermissionWrite,
				box.PermissionExecute,
			},
			Type: box.PrivilegeUniverse,
			Name: "",
		},
	}, privs)
}

func TestMocked_BoxSessionSu(t *testing.T) {
	t.Parallel()

	mock := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, []interface{}{}),
		errors.New("user not found or supplied credentials are invalid"),
	)
	b := box.MustNew(&mock)

	err := b.Session().Su(context.Background(), "admin")
	require.NoError(t, err)
}
