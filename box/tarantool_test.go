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

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/box"
	"github.com/tarantool/go-tarantool/v3/test_helpers"
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

func TestBox_Sugar_Schema_UserCreate_NoError(t *testing.T) {
	const (
		username = "user_create_no_error"
		password = "user_create_no_error"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)
}

func TestBox_Sugar_Schema_UserCreate_CanConnectWithNewCred(t *testing.T) {
	const (
		username = "can_connect_with_new_cred"
		password = "can_connect_with_new_cred"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Can connect with new credentials
	// Check that password is valid, and we can connect to tarantool with such credentials
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
}

func TestBox_Sugar_Schema_UserCreate_AlreadyExists(t *testing.T) {
	const (
		username = "create_already_exists"
		password = "create_already_exists"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Create user already exists error.
	// Get error that user already exists.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.Error(t, err)

	// Require that error code is ER_USER_EXISTS.
	var boxErr tarantool.Error
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_USER_EXISTS, boxErr.Code)
}

func TestBox_Sugar_Schema_UserCreate_ExistsTrue(t *testing.T) {
	const (
		username = "exists_check"
		password = "exists_check"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Check that already exists by exists call procedure
	exists, err := b.Schema().User().Exists(ctx, username)
	require.True(t, exists)
	require.NoError(t, err)

}

func TestBox_Sugar_Schema_UserCreate_IfNotExistsNoErr(t *testing.T) {
	const (
		username = "if_not_exists_no_err"
		password = "if_not_exists_no_err"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Again create such user.
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{
		Password:    password,
		IfNotExists: true,
	})
	require.NoError(t, err)
}

func TestBox_Sugar_Schema_UserPassword(t *testing.T) {
	const (
		password = "passwd"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Require password hash.
	hash, err := b.Schema().User().Password(ctx, password)
	require.NoError(t, err)
	require.NotEmpty(t, hash)
}

func TestBox_Sugar_Schema_UserDrop_AfterCreate(t *testing.T) {
	const (
		username = "to_drop_after_create"
		password = "to_drop_after_create"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Try to drop user
	err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
	require.NoError(t, err)
}

func TestBox_Sugar_Schema_UserDrop_DoubleDrop(t *testing.T) {
	const (
		username = "to_drop_double_drop"
		password = "to_drop_double_drop"
	)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Create new user
	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Try to drop user first time
	err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
	require.NoError(t, err)

	// Error double drop with IfExists: false option
	err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
	require.Error(t, err)

	// Require no error with IfExists: true option.
	err = b.Schema().User().Drop(ctx, username,
		box.UserDropOptions{IfExists: true})
	require.NoError(t, err)
}

func TestBox_Sugar_Schema_UserDrop_UnknownUser(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// Require error cause user not exists
	err = b.Schema().User().Drop(ctx, "some_strange_not_existing_name", box.UserDropOptions{})
	require.Error(t, err)

	var boxErr tarantool.Error

	// Require that error code is ER_NO_SUCH_USER
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
}

func TestSchemaUser_Passwd_NotFound(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Passwd(ctx, "not-exists-passwd", "new_password")
	require.Error(t, err)
	// Require that error code is ER_USER_EXISTS.
	var boxErr tarantool.Error
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_NO_SUCH_USER, boxErr.Code)
}

func TestSchemaUser_Passwd_Ok(t *testing.T) {
	const (
		username      = "new_password_user"
		startPassword = "new_password"
		endPassword   = "end_password"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	// New user change password and connect

	err = b.Schema().User().Create(ctx, username,
		box.UserCreateOptions{Password: startPassword, IfNotExists: true})
	require.NoError(t, err)

	err = b.Schema().User().Passwd(ctx, username, endPassword)
	require.NoError(t, err)

	dialer := dialer
	dialer.User = username
	dialer.Password = startPassword

	// Can't connect with old password.
	_, err = tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.Error(t, err, "can't connect with old password")

	// Ok connection with new password.
	dialer.Password = endPassword
	_, err = tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err, "ok connection with new password")
}

func TestSchemaUser_Passwd_WithoutGrants(t *testing.T) {
	const (
		username      = "new_password_user_fail_conn"
		startPassword = "new_password"
		endPassword   = "end_password"
	)
	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username,
		box.UserCreateOptions{Password: startPassword, IfNotExists: true})
	require.NoError(t, err)

	dialer := dialer
	dialer.User = username
	dialer.Password = startPassword

	conn2Fail, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)
	require.NotNil(t, conn2Fail)

	bFail := box.New(conn2Fail)
	// can't change self user password without grants
	err = bFail.Schema().User().Passwd(ctx, endPassword)
	require.Error(t, err)

	// Require that error code is AccessDeniedError,
	var boxErr tarantool.Error
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_ACCESS_DENIED, boxErr.Code)

}

func TestSchemaUser_Info_TestUserCorrect(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	privileges, err := b.Schema().User().Info(ctx, dialer.User)
	require.NoError(t, err)
	require.NotNil(t, privileges)

	require.Len(t, privileges, 4)
}

func TestSchemaUser_Info_NonExistsUser(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	privileges, err := b.Schema().User().Info(ctx, "non-existing")
	require.Error(t, err)
	require.Nil(t, privileges)
}

func TestBox_Sugar_Schema_UserGrant_NoSu(t *testing.T) {
	const (
		username = "to_grant_no_su"
		password = "to_grant_no_su"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	err = b.Schema().User().Grant(ctx, username, box.Privilege{
		Permissions: []box.Permission{
			box.PermissionRead,
		},
		Type: box.PrivilegeSpace,
		Name: "space1",
	}, box.UserGrantOptions{IfNotExists: false})
	require.Error(t, err)

	// Require that error code is ER_ACCESS_DENIED.
	var boxErr tarantool.Error
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_ACCESS_DENIED, boxErr.Code)
}

func TestBox_Sugar_Schema_UserGrant_WithSu(t *testing.T) {
	const (
		username = "to_grant_with_su"
		password = "to_grant_with_su"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	startPrivilages, err := b.Schema().User().Info(ctx, username)
	require.NoError(t, err)

	err = b.Session().Su(ctx, "admin")
	require.NoError(t, err)

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
}

func TestSchemaUser_Revoke_WithoutSu(t *testing.T) {
	const (
		username = "to_revoke_without_su"
		password = "to_revoke_without_su"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Can`t revoke without su permissions.
	err = b.Schema().User().Grant(ctx, username, box.Privilege{
		Permissions: []box.Permission{
			box.PermissionRead,
		},
		Type: box.PrivilegeSpace,
		Name: "space1",
	}, box.UserGrantOptions{IfNotExists: false})
	require.Error(t, err)

	// Require that error code is ER_ACCESS_DENIED.
	var boxErr tarantool.Error
	errors.As(err, &boxErr)
	require.Equal(t, iproto.ER_ACCESS_DENIED, boxErr.Code)
}

func TestSchemaUser_Revoke_WithSu(t *testing.T) {
	const (
		username = "to_revoke_with_su"
		password = "to_revoke_with_su"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	// Can revoke with su admin permissions.
	startPrivileges, err := b.Schema().User().Info(ctx, username)
	require.NoError(t, err)

	err = b.Session().Su(ctx, "admin")
	require.NoError(t, err)

	require.NoError(t, err, "dialer user in super group")

	require.NotEmpty(t, startPrivileges)
	// Let's choose random first privilege.
	examplePriv := startPrivileges[0]

	// Revoke it.
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
}

func TestSchemaUser_Revoke_NonExistsPermission(t *testing.T) {
	const (
		username = "to_revoke_non_exists_permission"
		password = "to_revoke_non_exists_permission"
	)

	defer cleanupUser(username)

	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Schema().User().Create(ctx, username, box.UserCreateOptions{Password: password})
	require.NoError(t, err)

	startPrivileges, err := b.Schema().User().Info(ctx, username)
	require.NoError(t, err)

	err = b.Session().Su(ctx, "admin")
	require.NoError(t, err)

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
}

func TestSession_Su_AdminPermissions(t *testing.T) {
	ctx := context.TODO()

	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	require.NoError(t, err)

	b := box.New(conn)

	err = b.Session().Su(ctx, "admin")
	require.NoError(t, err)
}

func cleanupUser(username string) {
	ctx := context.TODO()
	conn, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	if err != nil {
		log.Fatal(err)
	}

	b := box.New(conn)
	err = b.Schema().User().Drop(ctx, username, box.UserDropOptions{})
	if err != nil {
		log.Fatal(err)
	}
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
	if err != nil {
		log.Printf("Failed to prepare test Tarantool: %s", err)
		return 1
	}

	defer test_helpers.StopTarantoolWithCleanup(instance)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
