package box_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/box"
)

func TestUserExistsResponse_DecodeMsgpack(t *testing.T) {
	tCases := map[bool]func() *bytes.Buffer{
		true: func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.True)

			return buf
		},
		false: func() *bytes.Buffer {
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))
			buf.WriteByte(msgpcode.False)

			return buf
		},
	}

	for tCaseBool, tCaseBuf := range tCases {
		tCaseBool := tCaseBool
		tCaseBuf := tCaseBuf()

		t.Run(fmt.Sprintf("case: %t", tCaseBool), func(t *testing.T) {
			t.Parallel()

			resp := box.UserExistsResponse{}

			require.NoError(t, resp.DecodeMsgpack(msgpack.NewDecoder(tCaseBuf)))
			require.Equal(t, tCaseBool, resp.Exists)
		})
	}

}

func TestUserPasswordResponse_DecodeMsgpack(t *testing.T) {
	tCases := []string{
		"test",
		"$tr0ng_pass",
	}

	for _, tCase := range tCases {
		tCase := tCase

		t.Run(tCase, func(t *testing.T) {
			t.Parallel()
			buf := bytes.NewBuffer(nil)
			buf.WriteByte(msgpcode.FixedArrayLow | byte(1))

			bts, err := msgpack.Marshal(tCase)
			require.NoError(t, err)
			buf.Write(bts)

			resp := box.UserPasswordResponse{}

			err = resp.DecodeMsgpack(msgpack.NewDecoder(buf))
			require.NoError(t, err)
			require.Equal(t, tCase, resp.Hash)
		})
	}

}

func FuzzUserPasswordResponse_DecodeMsgpack(f *testing.F) {
	f.Fuzz(func(t *testing.T, orig string) {
		buf := bytes.NewBuffer(nil)
		buf.WriteByte(msgpcode.FixedArrayLow | byte(1))

		bts, err := msgpack.Marshal(orig)
		require.NoError(t, err)
		buf.Write(bts)

		resp := box.UserPasswordResponse{}

		err = resp.DecodeMsgpack(msgpack.NewDecoder(buf))
		require.NoError(t, err)
		require.Equal(t, orig, resp.Hash)
	})
}

func TestNewUserExistsRequest(t *testing.T) {
	t.Parallel()

	req := box.UserExistsRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserExistsRequest("test")
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserCreateRequest(t *testing.T) {
	t.Parallel()

	req := box.UserCreateRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserCreateRequest("test", box.UserCreateOptions{})
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserDropRequest(t *testing.T) {
	t.Parallel()

	req := box.UserDropRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserDropRequest("test", box.UserDropOptions{})
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserPasswordRequest(t *testing.T) {
	t.Parallel()

	req := box.UserPasswordRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserPasswordRequest("test")
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserPasswdRequest(t *testing.T) {
	t.Parallel()

	var err error
	req := box.UserPasswdRequest{}

	require.NotPanics(t, func() {
		req, err = box.NewUserPasswdRequest("test")
		require.NoError(t, err)
	})

	_, err = box.NewUserPasswdRequest()
	require.Errorf(t, err, "invalid arguments count")

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserInfoRequest(t *testing.T) {
	t.Parallel()

	var err error
	req := box.UserInfoRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserInfoRequest("test")
		require.NoError(t, err)
	})

	require.Implements(t, (*tarantool.Request)(nil), req)
}

func TestNewUserGrantRequest(t *testing.T) {
	t.Parallel()

	var err error
	req := box.UserGrantRequest{}

	require.NotPanics(t, func() {
		req = box.NewUserGrantRequest("test", box.Privilege{
			Permissions: []box.Permission{
				box.PermissionAlter,
				box.PermissionCreate,
				box.PermissionDrop,
			},
			Type: box.PrivilegeUniverse,
			Name: "test",
		}, box.UserGrantOptions{IfNotExists: true})
		require.NoError(t, err)
	})

	assert.Implements(t, (*tarantool.Request)(nil), req)
}
