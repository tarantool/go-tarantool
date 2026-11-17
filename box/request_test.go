package box_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/box"
)

const validUser = "test" // Any valid value != default.

var validPrivilege = box.Privilege{
	Permissions: []box.Permission{box.PermissionRead},
	Type:        box.PrivilegeUniverse,
}

func TestRequestsThread_default(t *testing.T) {
	sessionSuRequest, err := box.NewSessionSuRequest(validUser)
	require.NoError(t, err)
	userPasswdRequest, err := box.NewUserPasswdRequest(validUser)
	require.NoError(t, err)

	tests := []struct {
		req tarantool.Request
	}{
		{req: box.NewInfoRequest()},
		{req: sessionSuRequest},
		{req: box.NewUserExistsRequest(validUser)},
		{req: box.NewUserCreateRequest(validUser, box.UserCreateOptions{})},
		{req: box.NewUserDropRequest(validUser, box.UserDropOptions{})},
		{req: box.NewUserPasswordRequest(validUser)},
		{req: userPasswdRequest},
		{req: box.NewUserInfoRequest(validUser)},
		{req: box.NewUserGrantRequest(validUser, validPrivilege, box.UserGrantOptions{})},
		{req: box.NewUserRevokeRequest(validUser, validPrivilege, box.UserRevokeOptions{})},
	}

	for _, test := range tests {
		id, ok := test.req.Thread()
		assert.Equal(t, uint32(0), id, "An invalid thread id value")
		assert.False(t, ok, "An unexpected thread id set")
	}
}

func TestRequestsThread_setter(t *testing.T) {
	sessionSuRequest, err := box.NewSessionSuRequest(validUser)
	require.NoError(t, err)
	userPasswdRequest, err := box.NewUserPasswdRequest(validUser)
	require.NoError(t, err)

	tests := []struct {
		req tarantool.Request
	}{
		{req: box.NewInfoRequest().ThreadID(42)},
		{req: sessionSuRequest.ThreadID(42)},
		{req: box.NewUserExistsRequest(validUser).ThreadID(42)},
		{req: box.NewUserCreateRequest(validUser, box.UserCreateOptions{}).ThreadID(42)},
		{req: box.NewUserDropRequest(validUser, box.UserDropOptions{}).ThreadID(42)},
		{req: box.NewUserPasswordRequest(validUser).ThreadID(42)},
		{req: userPasswdRequest.ThreadID(42)},
		{req: box.NewUserInfoRequest(validUser).ThreadID(42)},
		{req: box.NewUserGrantRequest(validUser, validPrivilege,
			box.UserGrantOptions{}).ThreadID(42)},
		{req: box.NewUserRevokeRequest(validUser, validPrivilege,
			box.UserRevokeOptions{}).ThreadID(42)},
	}

	for _, test := range tests {
		id, ok := test.req.Thread()
		assert.Equal(t, uint32(42), id, "An invalid thread id value")
		assert.True(t, ok, "An expected thread id is not set")
	}
}
