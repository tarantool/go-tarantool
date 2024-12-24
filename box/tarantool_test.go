package box_test

import (
	"context"
	"errors"
	"log"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/box"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

var server = "127.0.0.1:3013"
var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}

func validateInfo(t testing.TB, info box.Info) {
	var err error

	// Check all fields run correctly.
	_, err = uuid.Parse(info.UUID)
	require.NoErrorf(t, err, "validate instance uuid is valid")

	require.NotEmpty(t, info.Version)
	// Check that pid parsed correctly.
	require.NotEqual(t, info.PID, 0)

	// Check replication is parsed correctly.
	require.NotEmpty(t, info.Replication)

	// Check one replica uuid is equal system uuid.
	require.Equal(t, info.UUID, info.Replication[1].UUID)
}

func TestBox_Sugar_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	info, err := box.New(conn).Info()
	require.NoError(t, err)

	validateInfo(t, info)
}

func TestBox_Info(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	fut := conn.Do(box.NewInfoRequest())
	require.NotNil(t, fut)

	resp := &box.InfoResponse{}
	err = fut.GetTyped(resp)
	require.NoError(t, err)

	validateInfo(t, resp.Info)
}

func TestBox_Sugar_Schema_UserCreate(t *testing.T) {
	const (
		username = "exists"
		password = "exists"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	t.Run("can connect with new credentials", func(t *testing.T) {
		t.Parallel()
		// Check that password is valid and we can connect to tarantool with such credentials
		var newUserDialer = tarantool.NetDialer{
			Address:  server,
			User:     username,
			Password: password,
		}

		// We can connect with our new credentials
		newUserConn, err := tarantool.Connect(ctx, newUserDialer, tarantool.Opts{})
		require.NoError(t, err)
		require.NotNil(t, newUserConn)
		require.NoError(t, newUserConn.Close())
	})
	t.Run("create user already exists error", func(t *testing.T) {
		t.Parallel()
		// Get error that user already exists
		err := b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
		require.Error(t, err)

		// Require that error code is ER_USER_EXISTS
		var boxErr tarantool.Error
		errors.As(err, &boxErr)
		require.Equal(t, iproto.ER_USER_EXISTS, boxErr.Code)
	})

	t.Run("exists method return true", func(t *testing.T) {
		t.Parallel()
		// Check that already exists by exists call procedure
		exists, err := b.Schema().User().Exists(ctx, username)
		require.True(t, exists)
		require.NoError(t, err)
	})

	t.Run("no error if IfNotExists option is true", func(t *testing.T) {
		t.Parallel()

		err := b.Schema().User().Create(ctx, username, box.UserCreateOptions{
			Password:    password,
			IfNotExists: true,
		})

		require.NoError(t, err)
	})
}

func TestBox_Sugar_Schema_UserPassword(t *testing.T) {
	const (
		password = "passwd"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Require password hash
	hash, err := b.Schema().User().Password(ctx, password)
	require.NoError(t, err)
	require.NotEmpty(t, hash)
}

func TestBox_Sugar_Schema_UserDrop(t *testing.T) {
	const (
		username = "to_drop"
		password = "to_drop"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	t.Run("drop user after create", func(t *testing.T) {
		// Create new user
		err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
		require.NoError(t, err)

		// Try to drop user
		err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
		require.NoError(t, err)

		t.Run("error double drop without IfExists option", func(t *testing.T) {
			// Require error cause user already deleted
			err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name",
				box.UserDropOptions{})
			require.Error(t, err)

			var boxErr tarantool.Error

			// Require that error code is ER_NO_SUCH_USER
			errors.As(err, &boxErr)
			require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
		})
		t.Run("ok double drop with IfExists option", func(t *testing.T) {
			// Require no error with IfExists: true option
			err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name",
				box.UserDropOptions{IfExists: true})
			require.NoError(t, err)
		})
	})

	t.Run("drop not existing user", func(t *testing.T) {
		t.Parallel()
		// Require error cause user already deleted
		err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name", box.UserDropOptions{})
		require.Error(t, err)

		var boxErr tarantool.Error

		// Require that error code is ER_NO_SUCH_USER
		errors.As(err, &boxErr)
		require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
	})
}

func TestSchemaUser_Passwd(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	t.Run("user not found", func(t *testing.T) {
		err = b.Schema().User().Passwd(ctx, "not-exists", "new_password")
		require.Error(t, err)
	})

	t.Run("new user change password and connect", func(t *testing.T) {
		const (
			username      = "new_password_user"
			startPassword = "new_password"
			endPassword   = "end_password"
		)

		err := b.Schema().User().Create(ctx, username,
			box.UserCreateOptions{Password: startPassword, IfNotExists: true})
		require.NoError(t, err)

		err = b.Schema().User().Passwd(ctx, username, endPassword)
		require.NoError(t, err)

		dialer := dialer
		dialer.User = username
		dialer.Password = startPassword

		_, err = tarantool.Connect(ctx, dialer, tarantool.Opts{})
		require.Error(t, err, "can't connect with old password")

		dialer.Password = endPassword
		_, err = tarantool.Connect(ctx, dialer, tarantool.Opts{})
		require.NoError(t, err, "ok connection with new password")
	})

	t.Run("can't change self password without grants", func(t *testing.T) {
		const (
			username      = "new_password_user_fail_conn"
			startPassword = "new_password"
			endPassword   = "end_password"
		)

		err := b.Schema().User().Create(ctx, username,
			box.UserCreateOptions{Password: startPassword, IfNotExists: true})
		require.NoError(t, err)

		dialer := dialer
		dialer.User = username
		dialer.Password = startPassword

		conn2Fail, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
		require.NoError(t, err)
		require.NotNil(t, conn2Fail)

		b := box.New(conn2Fail)
		// can't change self user password without grants
		err = b.Schema().User().Passwd(ctx, endPassword)
		require.Error(t, err)

		// Require that error code is AccessDeniedError,
		var boxErr tarantool.Error
		errors.As(err, &boxErr)
		require.Equal(t, iproto.ER_ACCESS_DENIED, boxErr.Code)
	})
}

func TestSchemaUser_Info(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	t.Run("test user privileges is correct", func(t *testing.T) {
		privileges, err := b.Schema().User().Info(ctx, dialer.User)
		require.NoError(t, err)
		require.NotNil(t, privileges)

		require.Len(t, privileges, 4)
	})

	t.Run("privileges of non existing user", func(t *testing.T) {
		privileges, err := b.Schema().User().Info(ctx, "non-existing")
		require.Error(t, err)
		require.Nil(t, privileges)
	})

}

func TestBox_Sugar_Schema_UserGrant(t *testing.T) {
	const (
		username = "to_grant"
		password = "to_grant"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	t.Run("can`t grant without su permissions", func(t *testing.T) {
		err = b.Schema().User().Grant(ctx, username, box.Privilege{
			Permissions: []box.Permission{
				box.PermissionRead,
			},
			Type: box.PrivilegeSpace,
			Name: "space1",
		}, box.UserGrantOptions{IfNotExists: false})
		require.Error(t, err)
	})

	t.Run("can grant with su admin permissions", func(t *testing.T) {
		startPrivilages, err := b.Schema().User().Info(ctx, username)
		require.NoError(t, err)

		future, err := b.Session().Su(ctx, "admin")
		require.NoError(t, err)

		_, err = future.Get()

		require.NoError(t, err, "default user in super group")

		newPrivilege := box.Privilege{
			Permissions: []box.Permission{
				box.PermissionRead,
			},
			Type: box.PrivilegeSpace,
			Name: "space1",
		}

		require.NotContains(t, startPrivilages, newPrivilege)

		err = b.Schema().User().Grant(ctx,
			username,
			newPrivilege,
			box.UserGrantOptions{
				IfNotExists: false,
			})
		require.NoError(t, err)

		endPrivileges, err := b.Schema().User().Info(ctx, username)
		require.NoError(t, err)
		require.NotEqual(t, startPrivilages, endPrivileges)
		require.Contains(t, endPrivileges, newPrivilege)
	})
}

func TestSchemaUser_Revoke(t *testing.T) {
	const (
		username = "to_revoke"
		password = "to_revoke"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	t.Run("can`t revoke without su permissions", func(t *testing.T) {
		err = b.Schema().User().Grant(ctx, username, box.Privilege{
			Permissions: []box.Permission{
				box.PermissionRead,
			},
			Type: box.PrivilegeSpace,
			Name: "space1",
		}, box.UserGrantOptions{IfNotExists: false})
		require.Error(t, err)
	})

	t.Run("can revoke with su admin permissions", func(t *testing.T) {
		startPrivileges, err := b.Schema().User().Info(ctx, username)
		require.NoError(t, err)

		future, err := b.Session().Su(ctx, "admin")
		require.NoError(t, err)

		_, err = future.Get()

		require.NoError(t, err, "dialer user in super group")

		require.NotEmpty(t, startPrivileges)
		examplePriv := startPrivileges[0]

		err = b.Schema().User().Revoke(ctx,
			username,
			examplePriv,
			box.UserRevokeOptions{
				IfExists: false,
			})

		require.NoError(t, err)

		privileges, err := b.Schema().User().Info(ctx, username)
		require.NoError(t, err)

		require.NotEqual(t, startPrivileges, privileges)
		require.NotContains(t, privileges, examplePriv)
	})

	t.Run("try to revoke non existing permissions", func(t *testing.T) {
		startPrivileges, err := b.Schema().User().Info(ctx, username)
		require.NoError(t, err)

		future, err := b.Session().Su(ctx, "admin")
		require.NoError(t, err)
		_, err = future.Get()

		require.NoError(t, err, "dialer user in super group")

		require.NotEmpty(t, startPrivileges)
		examplePriv := box.Privilege{
			Permissions: []box.Permission{box.PermissionRead},
			Name:        "non_existing_space",
			Type:        box.PrivilegeSpace,
		}

		err = b.Schema().User().Revoke(ctx,
			username,
			examplePriv,
			box.UserRevokeOptions{
				IfExists: false,
			})

		require.Error(t, err)
	})
}

func TestSession_Su_WithCall(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	_, err = b.Session().Su(ctx, "admin", "echo", 1, 2)
	require.Error(t, err, "unsupported now")
}

func TestSession_Su_AdminPermissions(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	_, err = b.Session().Su(ctx, "admin")
	require.NoError(t, err)
}

func runTestMain(m *testing.M) int {
	instance, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})
	defer test_helpers.StopTarantoolWithCleanup(instance)

	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
